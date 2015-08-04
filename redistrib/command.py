import re
import hiredis
import logging
from retrying import retry

from exceptions import RedisStatusError
from clusternode import Talker, ClusterNode, base_balance_plan
from clusternode import CMD_INFO, CMD_CLUSTER_NODES, CMD_CLUSTER_INFO

SLOT_COUNT = 16384
PAT_CLUSTER_ENABLED = re.compile('cluster_enabled:([01])')
PAT_CLUSTER_STATE = re.compile('cluster_state:([a-z]+)')
PAT_CLUSTER_SLOT_ASSIGNED = re.compile('cluster_slots_assigned:([0-9]+)')
PAT_MIGRATING_IN = re.compile(r'\[([0-9]+)-<-(\w+)\]')
PAT_MIGRATING_OUT = re.compile(r'\[([0-9]+)->-(\w+)\]')


def _valid_node_info(n):
    return len(n) != 0 and 'fail' not in n and 'handshake' not in n


def _ensure_cluster_status_unset(t):
    m = t.talk_raw(CMD_INFO)
    logging.debug('Ask `info` Rsp %s', m)
    cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
    if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not cluster enabled' % (t.host, t.port))

    m = t.talk_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'fail' or int(cluster_slot_assigned[0]) != 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is already in a cluster' % (t.host, t.port))


def _ensure_cluster_status_set_at(host, port):
    t = Talker(host, port)
    try:
        _ensure_cluster_status_set(t)
    finally:
        t.close()


def _ensure_cluster_status_set(t):
    m = t.talk_raw(CMD_INFO)
    logging.debug('Ask `info` Rsp %s', m)
    cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
    if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not cluster enabled' % (t.host, t.port))

    m = t.talk_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'ok' and int(cluster_slot_assigned[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not in a cluster' % (t.host, t.port))


# Redis instance responses to clients BEFORE changing its 'cluster_state'
#   just retry some times, it should become OK
@retry(stop_max_attempt_number=64, wait_fixed=500)
def _poll_check_status(t):
    m = t.talk_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'ok' or int(
            cluster_slot_assigned[0]) != SLOT_COUNT:
        raise RedisStatusError('Unexpected status: %s' % m)


def start_cluster(host, port):
    t = Talker(host, port)

    try:
        _ensure_cluster_status_unset(t)

        m = t.talk('cluster', 'addslots', *xrange(SLOT_COUNT))
        logging.debug('Ask `cluster addslots` Rsp %s', m)
        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after ADDSLOTS: %s' % m)

        _poll_check_status(t)
        logging.info('Instance at %s:%d started as a standalone cluster',
                     host, port)
    finally:
        t.close()


def start_cluster_on_multi(host_port_list):
    talkers = []
    try:
        for host, port in set(host_port_list):
            t = Talker(host, port)
            talkers.append(t)
            _ensure_cluster_status_unset(t)
            logging.info('Instance at %s:%d checked', t.host, t.port)

        first_talker = talkers[0]
        for i, t in enumerate(talkers[1:]):
            t.talk('cluster', 'meet', first_talker.host, first_talker.port)

        slots_each = SLOT_COUNT / len(talkers)
        slots_residue = SLOT_COUNT - slots_each * len(talkers)
        first_node_slots = slots_residue + slots_each

        first_talker.talk('cluster', 'addslots', *xrange(first_node_slots))
        logging.info('Add %d slots to %s:%d', slots_residue + slots_each,
                     first_talker.host, first_talker.port)
        for i, t in enumerate(talkers[1:]):
            t.talk('cluster', 'addslots', *xrange(
                i * slots_each + first_node_slots,
                (i + 1) * slots_each + first_node_slots))
            logging.info('Add %d slots to %s:%d', slots_each, t.host, t.port)

        for t in talkers:
            _poll_check_status(t)
    finally:
        for t in talkers:
            t.close()


def _migr_keys(src_talker, target_host, target_port, slot):
    key_count = 0
    while True:
        keys = src_talker.talk('cluster', 'getkeysinslot', slot, 10)
        if len(keys) == 0:
            return key_count
        key_count += len(keys)
        src_talker.talk_bulk(
            [['migrate', target_host, target_port, k, 0, 30000] for k in keys])


def _migr_slots(source_node, target_node, slots, nodes):
    logging.info(
        'Migrating %d slots from %s<%s:%d> to %s<%s:%d>', len(slots),
        source_node.node_id, source_node.host, source_node.port,
        target_node.node_id, target_node.host, target_node.port)
    key_count = 0
    for slot in slots:
        key_count += _migr_one_slot(source_node, target_node, slot, nodes)
    logging.info(
        'Migrated: %d slots %d keys from %s<%s:%d> to %s<%s:%d>',
        len(slots), key_count,
        source_node.node_id, source_node.host, source_node.port,
        target_node.node_id, target_node.host, target_node.port)


def _migr_one_slot(source_node, target_node, slot, nodes):
    def expect_talk_ok(m, slot):
        if m.lower() != 'ok':
            raise RedisStatusError('\n'.join([
                'Error while moving slot [ %d ] between' % slot,
                'Source node - %s:%d' % (source_node.host, source_node.port),
                'Target node - %s:%d' % (target_node.host, target_node.port),
                'Got %s' % m]))

    @retry(stop_max_attempt_number=16, wait_fixed=100)
    def setslot_stable(talker, slot, node_id):
        m = talker.talk('cluster', 'setslot', slot, 'node', node_id)
        if m.lower() != 'ok':
            raise hiredis.ReplyError(m)

    source_talker = source_node.talker()
    target_talker = target_node.talker()

    try:
        expect_talk_ok(
            target_talker.talk('cluster', 'setslot', slot, 'importing',
                               source_node.node_id),
            slot)
    except hiredis.ReplyError, e:
        if 'already the owner of' not in e.message:
            raise

    try:
        expect_talk_ok(
            source_talker.talk('cluster', 'setslot', slot, 'migrating',
                               target_node.node_id),
            slot)
    except hiredis.ReplyError, e:
        if 'not the owner of' not in e.message:
            raise

    keys = _migr_keys(source_talker, target_node.host, target_node.port, slot)
    try:
        setslot_stable(source_talker, slot, target_node.node_id)
        for node in nodes:
            setslot_stable(node.talker(), slot, target_node.node_id)
    except hiredis.ReplyError, e:
        expect_talk_ok(e.message, slot)
    return keys


def _join_to_cluster(cluster_host, cluster_port, newin_talker):
    _ensure_cluster_status_set_at(cluster_host, cluster_port)
    _ensure_cluster_status_unset(newin_talker)

    m = newin_talker.talk('cluster', 'meet', cluster_host, cluster_port)
    logging.debug('Ask `cluster meet` Rsp %s', m)
    if m.lower() != 'ok':
        raise RedisStatusError('Unexpected reply after MEET: %s' % m)
    _poll_check_status(newin_talker)


def join_cluster(cluster_host, cluster_port, newin_host, newin_port,
                 balancer=None, balance_plan=base_balance_plan):
    nodes = []
    t = Talker(newin_host, newin_port)
    try:
        _join_to_cluster(cluster_host, cluster_port, t)
        logging.info('Instance at %s:%d has joined %s:%d; now balancing slots',
                     newin_host, newin_port, cluster_host, cluster_port)

        m = t.talk_raw(CMD_CLUSTER_INFO)
        logging.debug('Ask `cluster info` Rsp %s', m)
        cluster_state = PAT_CLUSTER_STATE.findall(m)
        if cluster_state[0] != 'ok':
            raise hiredis.ProtocolError(
                'Node %s:%d is already in a cluster' % (t.host, t.port))
        slots = int(PAT_CLUSTER_SLOT_ASSIGNED.findall(m)[0])
        nodes = _list_nodes(t, default_host=newin_host)[0]

        for source, target, count in balance_plan(nodes, balancer):
            _migr_slots(source, target, source.assigned_slots[:count], nodes)
    finally:
        t.close()
        for n in nodes:
            n.close()


def join_no_load(cluster_host, cluster_port, newin_host, newin_port):
    t = Talker(newin_host, newin_port)
    try:
        _join_to_cluster(cluster_host, cluster_port, t)
    finally:
        t.close()


def _check_master_and_migrate_slots(nodes, myself):
    other_masters = []
    master_ids = set()
    for node in nodes:
        if node.role_in_cluster == 'master':
            other_masters.append(node)
        else:
            master_ids.add(node.master_id)
    if len(other_masters) == 0:
        raise ValueError('This is the last node')
    if myself.node_id in master_ids:
        raise ValueError('The master still has slaves')

    mig_slots_to_each = len(myself.assigned_slots) / len(other_masters)
    for node in other_masters[:-1]:
        _migr_slots(myself, node, myself.assigned_slots[:mig_slots_to_each],
                    nodes)
        del myself.assigned_slots[:mig_slots_to_each]
    node = other_masters[-1]
    _migr_slots(myself, node, myself.assigned_slots, nodes)


def quit_cluster(host, port):
    nodes = []
    myself = None
    t = Talker(host, port)
    try:
        _ensure_cluster_status_set(t)
        nodes, myself = _list_nodes(t)
        nodes.remove(myself)
        if myself.role_in_cluster == 'master':
            _check_master_and_migrate_slots(nodes, myself)
        logging.info('Migrated for %s / Broadcast a `forget`', myself.node_id)
        for node in nodes:
            tk = node.talker()
            try:
                tk.talk('cluster', 'forget', myself.node_id)
            except hiredis.ReplyError, e:
                if 'Unknown node' not in e.message:
                    raise
        t.talk('cluster', 'reset')
    finally:
        t.close()
        if myself is not None:
            myself.close()
        for n in nodes:
            n.close()


def shutdown_cluster(host, port):
    t = Talker(host, port)
    try:
        _ensure_cluster_status_set(t)
        myself = None
        m = t.talk_raw(CMD_CLUSTER_NODES)
        logging.debug('Ask `cluster nodes` Rsp %s', m)
        nodes_info = filter(None, m.split('\n'))
        if len(nodes_info) > 1:
            raise RedisStatusError('More than 1 nodes in cluster.')
        try:
            m = t.talk('cluster', 'reset')
        except hiredis.ReplyError, e:
            if 'containing keys' in e.message:
                raise RedisStatusError('Cluster containing keys')
            raise
        logging.debug('Ask `cluster delslots` Rsp %s', m)
    finally:
        t.close()


def fix_migrating(host, port):
    nodes = dict()
    mig_srcs = []
    mig_dsts = []
    t = Talker(host, port)
    try:
        m = t.talk_raw(CMD_CLUSTER_NODES)
        logging.debug('Ask `cluster nodes` Rsp %s', m)
        for node_info in m.split('\n'):
            if not _valid_node_info(node_info):
                continue
            node = ClusterNode(*node_info.split(' '))
            node.host = node.host or host
            nodes[node.node_id] = node

            mig_dsts.extend([(node, {'slot': g[0], 'id': g[1]})
                             for g in PAT_MIGRATING_IN.findall(node_info)])
            mig_srcs.extend([(node, {'slot': g[0], 'id': g[1]})
                             for g in PAT_MIGRATING_OUT.findall(node_info)])

        for n, args in mig_dsts:
            node_id = args['id']
            if node_id not in nodes:
                logging.error('Fail to fix %s:%d <- (referenced from %s:%d)'
                              ' - node %s is missing', n.host, n.port,
                              host, port, node_id)
                continue
            _migr_one_slot(nodes[node_id], n, int(args['slot']),
                           nodes.itervalues())
        for n, args in mig_srcs:
            node_id = args['id']
            if node_id not in nodes:
                logging.error('Fail to fix %s:%d -> (referenced from %s:%d)'
                              ' - node %s is missing', n.host, n.port,
                              host, port, node_id)
                continue
            _migr_one_slot(n, nodes[node_id], int(args['slot']),
                           nodes.itervalues())
    finally:
        t.close()
        for n in nodes.itervalues():
            n.close()


@retry(stop_max_attempt_number=16, wait_fixed=1000)
def _check_slave(slave_host, slave_port, t):
    slave_addr = '%s:%d' % (slave_host, slave_port)
    for line in t.talk('cluster', 'nodes').split('\n'):
        if slave_addr in line:
            if 'slave' in line:
                return
            raise RedisStatusError('%s not switched to a slave' % slave_addr)


def replicate(master_host, master_port, slave_host, slave_port):
    master_talker = None
    t = Talker(slave_host, slave_port)
    try:
        master_talker = Talker(master_host, master_port)
        _ensure_cluster_status_set(master_talker)
        myself = _list_nodes(master_talker)[1]
        myid = (myself.node_id if myself.role_in_cluster == 'master'
                else myself.master_id)

        _join_to_cluster(master_host, master_port, t)
        logging.info('Instance at %s:%d has joined %s:%d; now set replica',
                     slave_host, slave_port, master_host, master_port)

        m = t.talk('cluster', 'replicate', myid)
        logging.debug('Ask `cluster replicate` Rsp %s', m)
        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after REPCLIATE: %s' % m)
        _check_slave(slave_host, slave_port, master_talker)
        logging.info('Instance at %s:%d set as replica to %s',
                     slave_host, slave_port, myid)
    finally:
        t.close()
        if master_talker is not None:
            master_talker.close()


def _list_nodes(talker, default_host=None, filter_func=lambda node: True):
    m = talker.talk_raw(CMD_CLUSTER_NODES)
    logging.debug('Ask `cluster nodes` Rsp %s', m)
    default_host = default_host or talker.host

    nodes = []
    myself = None
    for node_info in m.split('\n'):
        if not _valid_node_info(node_info):
            continue
        node = ClusterNode(*node_info.split(' '))
        if 'myself' in node_info:
            myself = node
            if myself.host == '':
                myself.host = default_host
        if filter_func(node):
            nodes.append(node)
    return nodes, myself


def _list_masters(talker, default_host=None):
    return _list_nodes(talker, default_host or talker.host,
                       lambda node: node.role_in_cluster == 'master')


def list_nodes(host, port, default_host=None):
    t = Talker(host, port)
    try:
        return _list_nodes(t, default_host or host)
    finally:
        t.close()


def list_masters(host, port, default_host=None):
    t = Talker(host, port)
    try:
        return _list_masters(t, default_host or host)
    finally:
        t.close()


def migrate_slots(src_host, src_port, dst_host, dst_port, slots):
    if src_host == dst_host and src_port == dst_port:
        raise ValueError('Same node')
    t = Talker(src_host, src_port)
    try:
        nodes, myself = _list_masters(t, src_host)
    finally:
        t.close()

    slots = set(slots)
    logging.debug('Migrating %s', slots)
    if not slots.issubset(set(myself.assigned_slots)):
        raise ValueError('Not all slot held by %s:%d' % (src_host, src_port))
    for n in nodes:
        if n.host == dst_host and n.port == dst_port:
            return _migr_slots(myself, n, slots, nodes)
    raise ValueError('Two nodes are not in the same cluster')


def rescue_cluster(host, port, subst_host, subst_port):
    failed_slots = set(xrange(SLOT_COUNT))
    t = None
    s = None
    try:
        t = Talker(host, port)
        _ensure_cluster_status_set(t)
        for node in _list_masters(t)[0]:
            failed_slots -= set(node.assigned_slots)
        if len(failed_slots) == 0:
            logging.info('No need to rescue cluster at %s:%d', host, port)
            return

        s = Talker(subst_host, subst_port)
        _ensure_cluster_status_unset(s)

        m = s.talk('cluster', 'meet', host, port)
        logging.debug('Ask `cluster meet` Rsp %s', m)
        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after MEET: %s' % m)

        m = s.talk('cluster', 'addslots', *failed_slots)
        logging.debug('Ask `cluster addslots` Rsp %s', m)

        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after ADDSLOTS: %s' % m)

        _poll_check_status(s)
        logging.info('Instance at %s:%d serves %d slots to rescue the cluster',
                     subst_host, subst_port, len(failed_slots))
    finally:
        if t is not None:
            t.close()
        if s is not None:
            s.close()
