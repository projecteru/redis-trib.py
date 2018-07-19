import re
import hiredis
import logging
import six
from six.moves import range
from retrying import retry

from .clusternode import ClusterNode, base_balance_plan
from .connection import Connection, CMD_INFO, CMD_CLUSTER_NODES, CMD_CLUSTER_INFO

SLOT_COUNT = 16384
PAT_CLUSTER_ENABLED = re.compile('cluster_enabled:([01])')
PAT_CLUSTER_STATE = re.compile('cluster_state:([a-z]+)')
PAT_CLUSTER_SLOT_ASSIGNED = re.compile('cluster_slots_assigned:([0-9]+)')
PAT_MIGRATING_IN = re.compile(r'\[([0-9]+)-<-(\w+)\]')
PAT_MIGRATING_OUT = re.compile(r'\[([0-9]+)->-(\w+)\]')


def _valid_node_info(n):
    return len(n) != 0 and 'handshake' not in n


def _ensure_cluster_status_unset(t):
    m = t.send_raw(CMD_INFO)
    logging.debug('Ask `info` Rsp %s', m)
    cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
    if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not cluster enabled' % (t.host, t.port))

    m = t.send_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'fail' or int(cluster_slot_assigned[0]) != 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is already in a cluster' % (t.host, t.port))


def _ensure_cluster_status_set(t):
    m = t.send_raw(CMD_INFO)
    logging.debug('Ask `info` Rsp %s', m)
    cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
    if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not cluster enabled' % (t.host, t.port))

    m = t.send_raw(CMD_CLUSTER_INFO)
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
    m = t.send_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'ok' or int(
            cluster_slot_assigned[0]) != SLOT_COUNT:
        t.raise_('Unexpected status: %s' % m)


def _add_slots(t, begin, end, max_slots):
    def addslots(t, begin, end):
        m = t.execute('cluster', 'addslots', *range(begin, end))
        logging.debug('Ask `cluster addslots` Rsp %s', m)
        if m.lower() != 'ok':
            t.raise_('Unexpected reply after ADDSLOTS: %s' % m)

    i = begin + max_slots
    while i < end:
        addslots(t, begin, i)
        begin = i
        i += max_slots
    addslots(t, begin, end)


def create(host_port_list, max_slots=1024):
    conns = []
    try:
        for host, port in set(host_port_list):
            t = Connection(host, port)
            conns.append(t)
            _ensure_cluster_status_unset(t)
            logging.info('Instance at %s:%d checked', t.host, t.port)

        first_conn = conns[0]
        for i, t in enumerate(conns[1:]):
            t.execute('cluster', 'meet', first_conn.host, first_conn.port)

        slots_each = SLOT_COUNT // len(conns)
        slots_residue = SLOT_COUNT - slots_each * len(conns)
        first_node_slots = slots_residue + slots_each

        _add_slots(first_conn, 0, first_node_slots, max_slots)
        logging.info('Add %d slots to %s:%d', slots_residue + slots_each,
                     first_conn.host, first_conn.port)
        for i, t in enumerate(conns[1:]):
            _add_slots(t, i * slots_each + first_node_slots,
                       (i + 1) * slots_each + first_node_slots, max_slots)
            logging.info('Add %d slots to %s:%d', slots_each, t.host, t.port)
        for t in conns:
            _poll_check_status(t)
    finally:
        for t in conns:
            t.close()


def start_cluster(host, port, max_slots=SLOT_COUNT):
    with Connection(host, port) as t:
        _ensure_cluster_status_unset(t)
        _add_slots(t, 0, SLOT_COUNT, max_slots)
        _poll_check_status(t)
        logging.info('Instance at %s:%d started as a standalone cluster',
                     host, port)


def start_cluster_on_multi(host_port_list, max_slots=SLOT_COUNT):
    return create(host_port_list, max_slots)


def _migr_keys(src_conn, target_host, target_port, slot):
    key_count = 0
    while True:
        keys = src_conn.execute('cluster', 'getkeysinslot', slot, 10)
        if len(keys) == 0:
            return key_count
        key_count += len(keys)
        src_conn.execute_bulk(
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
    def expect_exec_ok(m, conn, slot):
        if m.lower() != 'ok':
            conn.raise_('\n'.join([
                'Error while moving slot [ %d ] between' % slot,
                'Source node - %s:%d' % (source_node.host, source_node.port),
                'Target node - %s:%d' % (target_node.host, target_node.port),
                'Got %s' % m]))

    @retry(stop_max_attempt_number=16, wait_fixed=100)
    def setslot_stable(conn, slot, node_id):
        m = conn.execute('cluster', 'setslot', slot, 'node', node_id)
        expect_exec_ok(m, conn, slot)

    source_conn = source_node.get_conn()
    target_conn = target_node.get_conn()

    try:
        expect_exec_ok(
            target_conn.execute('cluster', 'setslot', slot, 'importing',
                                source_node.node_id),
            target_conn, slot)
    except hiredis.ReplyError as e:
        if 'already the owner of' not in str(e):
            target_conn.raise_(str(e))

    try:
        expect_exec_ok(
            source_conn.execute('cluster', 'setslot', slot, 'migrating',
                                target_node.node_id),
            source_conn, slot)
    except hiredis.ReplyError as e:
        if 'not the owner of' not in str(e):
            source_conn.raise_(str(e))

    keys = _migr_keys(source_conn, target_node.host, target_node.port, slot)
    setslot_stable(source_conn, slot, target_node.node_id)
    for node in nodes:
        if node.master:
            setslot_stable(node.get_conn(), slot, target_node.node_id)
    return keys


def _join_to_cluster(clst, new):
    _ensure_cluster_status_set(clst)
    _ensure_cluster_status_unset(new)

    m = clst.execute('cluster', 'meet', new.host, new.port)
    logging.debug('Ask `cluster meet` Rsp %s', m)
    if m.lower() != 'ok':
        clst.raise_('Unexpected reply after MEET: %s' % m)
    _poll_check_status(new)


def join_cluster(cluster_host, cluster_port, newin_host, newin_port,
                 balancer=None, balance_plan=base_balance_plan):
    with Connection(newin_host, newin_port) as t, \
         Connection(cluster_host, cluster_port) as cnode:
        _join_to_cluster(cnode, t)
        nodes = []
        try:
            logging.info(
                'Instance at %s:%d has joined %s:%d; now balancing slots',
                newin_host, newin_port, cluster_host, cluster_port)
            nodes = _list_nodes(t, default_host=newin_host)[0]
            for src, dst, count in balance_plan(nodes, balancer):
                _migr_slots(src, dst, src.assigned_slots[:count], nodes)
        finally:
            for n in nodes:
                n.close()


def add_node(cluster_host, cluster_port, newin_host, newin_port):
    with Connection(newin_host, newin_port) as t, \
         Connection(cluster_host, cluster_port) as c:
        _join_to_cluster(c, t)


def join_no_load(cluster_host, cluster_port, newin_host, newin_port):
    return add_node(cluster_host, cluster_port, newin_host, newin_port)


def _check_master_and_migrate_slots(nodes, myself):
    other_masters = []
    master_ids = set()
    for node in nodes:
        if node.master:
            other_masters.append(node)
        else:
            master_ids.add(node.master_id)
    if len(other_masters) == 0:
        raise ValueError('This is the last node')
    if myself.node_id in master_ids:
        raise ValueError('The master still has slaves')

    mig_slots_to_each = len(myself.assigned_slots) // len(other_masters)
    for node in other_masters[:-1]:
        _migr_slots(myself, node, myself.assigned_slots[:mig_slots_to_each],
                    nodes)
        del myself.assigned_slots[:mig_slots_to_each]
    node = other_masters[-1]
    _migr_slots(myself, node, myself.assigned_slots, nodes)


def del_node(host, port):
    myself = None
    nodes = []
    t = Connection(host, port)
    try:
        _ensure_cluster_status_set(t)
        nodes, myself = _list_nodes(t, filter_func=lambda n: not n.fail)
        nodes.remove(myself)
        if myself.master:
            _check_master_and_migrate_slots(nodes, myself)
        logging.info('Migrated for %s / Broadcast a `forget`', myself.node_id)
        for node in nodes:
            tk = node.get_conn()
            try:
                tk.execute('cluster', 'forget', myself.node_id)
            except hiredis.ReplyError as e:
                if 'Unknown node' not in str(e):
                    raise
        t.execute('cluster', 'reset')
    finally:
        t.close()
        if myself is not None:
            myself.close()
        for n in nodes:
            n.close()


def quit_cluster(host, port):
    return del_node(host, port)


def shutdown_cluster(host, port):
    with Connection(host, port) as t:
        _ensure_cluster_status_set(t)
        myself = None
        m = t.send_raw(CMD_CLUSTER_NODES)
        logging.debug('Ask `cluster nodes` Rsp %s', m)
        nodes_info = [i for i in m.split('\n') if i]
        if len(nodes_info) > 1:
            t.raise_('More than 1 nodes in cluster.')
        try:
            m = t.execute('cluster', 'reset')
        except hiredis.ReplyError as e:
            if 'containing keys' in str(e):
                t.raise_('Redis still contains keys')
            raise
        logging.debug('Ask `cluster delslots` Rsp %s', m)


def fix_migrating(host, port):
    nodes = dict()
    mig_srcs = []
    mig_dsts = []
    t = Connection(host, port)
    try:
        m = t.send_raw(CMD_CLUSTER_NODES)
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
                           six.itervalues(nodes))
        for n, args in mig_srcs:
            node_id = args['id']
            if node_id not in nodes:
                logging.error('Fail to fix %s:%d -> (referenced from %s:%d)'
                              ' - node %s is missing', n.host, n.port,
                              host, port, node_id)
                continue
            _migr_one_slot(n, nodes[node_id], int(args['slot']),
                           six.itervalues(nodes))
    finally:
        t.close()
        for n in six.itervalues(nodes):
            n.close()


@retry(stop_max_attempt_number=16, wait_fixed=1000)
def _check_slave(slave_host, slave_port, t):
    slave_addr = '%s:%d' % (slave_host, slave_port)
    for line in t.execute('cluster', 'nodes').split('\n'):
        if slave_addr in line:
            if 'slave' in line:
                return
            t.raise_('%s not switched to a slave' % slave_addr)


def replicate(master_host, master_port, slave_host, slave_port):
    with Connection(slave_host, slave_port) as t, \
         Connection(master_host, master_port) as master_conn:
        _ensure_cluster_status_set(master_conn)
        myself = _list_nodes(master_conn)[1]
        myid = myself.node_id if myself.master else myself.master_id

        _join_to_cluster(master_conn, t)
        logging.info('Instance at %s:%d has joined %s:%d; now set replica',
                     slave_host, slave_port, master_host, master_port)

        m = t.execute('cluster', 'replicate', myid)
        logging.debug('Ask `cluster replicate` Rsp %s', m)
        if m.lower() != 'ok':
            t.raise_('Unexpected reply after REPCLIATE: %s' % m)
        _check_slave(slave_host, slave_port, master_conn)
        logging.info('Instance at %s:%d set as replica to %s',
                     slave_host, slave_port, myid)


def _alive_master(node):
    return node.master and not node.fail


def _filter_master(node):
    return node.master


def _list_nodes(conn, default_host=None, filter_func=lambda node: True):
    m = conn.send_raw(CMD_CLUSTER_NODES)
    logging.debug('Ask `cluster nodes` Rsp %s', m)
    default_host = default_host or conn.host

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


def _list_masters(conn, default_host=None):
    return _list_nodes(conn, default_host or conn.host,
                       filter_func=_filter_master)


def list_nodes(host, port, default_host=None, filter_func=lambda node: True):
    with Connection(host, port) as t:
        return _list_nodes(t, default_host or host, filter_func)


def list_masters(host, port, default_host=None):
    with Connection(host, port) as t:
        return _list_masters(t, default_host or host)


def migrate_slots(src_host, src_port, dst_host, dst_port, slots):
    if src_host == dst_host and src_port == dst_port:
        raise ValueError('Same node')
    with Connection(src_host, src_port) as t:
        nodes, myself = _list_masters(t, src_host)

    slots = set(slots)
    logging.debug('Migrating %s', slots)
    if not slots.issubset(set(myself.assigned_slots)):
        raise ValueError('Not all slot held by %s:%d' % (src_host, src_port))

    try:
        for n in nodes:
            if n.host == dst_host and n.port == dst_port:
                return _migr_slots(myself, n, slots, nodes)
        raise ValueError('Two nodes are not in the same cluster')
    finally:
        for n in nodes:
            n.close()


def rescue_cluster(host, port, subst_host, subst_port):
    failed_slots = set(range(SLOT_COUNT))
    with Connection(host, port) as t:
        _ensure_cluster_status_set(t)
        for node in _list_masters(t)[0]:
            if not node.fail:
                failed_slots -= set(node.assigned_slots)
        if len(failed_slots) == 0:
            logging.info('No need to rescue cluster at %s:%d', host, port)
            return

    with Connection(subst_host, subst_port) as s:
        _ensure_cluster_status_unset(s)

        m = s.execute('cluster', 'meet', host, port)
        logging.debug('Ask `cluster meet` Rsp %s', m)
        if m.lower() != 'ok':
            s.raise_('Unexpected reply after MEET: %s' % m)

        m = s.execute('cluster', 'addslots', *failed_slots)
        logging.debug('Ask `cluster addslots` Rsp %s', m)

        if m.lower() != 'ok':
            s.raise_('Unexpected reply after ADDSLOTS: %s' % m)

        _poll_check_status(s)
        logging.info(
            'Instance at %s:%d serves %d slots to rescue the cluster',
            subst_host, subst_port, len(failed_slots))


def execute(host, port, master_only, slave_only, commands):
    with Connection(host, port) as c:
        filter_func = lambda n: True
        if master_only:
            filter_func = _filter_master
        elif slave_only:
            filter_func = lambda n: n.slave
        nodes = _list_nodes(c, filter_func=filter_func)[0]

        result = []
        for n in nodes:
            r = None
            exc = None
            try:
                r = n.get_conn().execute(*commands)
            except Exception as e:
                exc = e
            result.append({
                'node': n,
                'result': r,
                'exception': exc,
            })
        return result
