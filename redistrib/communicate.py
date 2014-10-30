import socket
import re
import hiredis
import logging
from retrying import retry

SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''


class RedisStatusError(Exception):
    pass


def encode(value, encoding='utf-8'):
    if isinstance(value, bytes):
        return value
    if isinstance(value, (int, long)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, unicode):
        return value.encode(encoding)
    if not isinstance(value, basestring):
        return str(value)
    return value


def pack_command(command, *args):
    output = []
    if ' ' in command:
        args = tuple([s for s in command.split(' ')]) + args
    else:
        args = (command,) + args

    buff = SYM_EMPTY.join((SYM_STAR, str(len(args)), SYM_CRLF))

    for arg in map(encode, args):
        if len(buff) > 6000 or len(arg) > 6000:
            buff = SYM_EMPTY.join((buff, SYM_DOLLAR, str(len(arg)), SYM_CRLF))
            output.append(buff)
            output.append(arg)
            buff = SYM_CRLF
        else:
            buff = SYM_EMPTY.join((buff, SYM_DOLLAR, str(len(arg)),
                                   SYM_CRLF, arg, SYM_CRLF))
    output.append(buff)
    return output

CMD_PING = pack_command('ping')
CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_CLUSTER_INFO = pack_command('cluster', 'info')

PAT_CLUSTER_ENABLED = re.compile('cluster_enabled:([01])')
PAT_CLUSTER_STATE = re.compile('cluster_state:([a-z]+)')
PAT_CLUSTER_SLOT_ASSIGNED = re.compile('cluster_slots_assigned:([0-9]+)')

# One Redis cluster requires at least 16384 slots
SLOT_COUNT = 16384


class Talker(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reader = hiredis.Reader()
        self.last_raw_message = None

        self.sock.settimeout(8)
        logging.debug('Connect to %s:%d', host, port)
        self.sock.connect((host, port))

    def talk_raw(self, command):
        for c in command:
            self.sock.send(c)
        self.last_raw_message = self.sock.recv(16384)
        self.reader.feed(self.last_raw_message)
        return self.reader.gets()

    def talk(self, *args):
        return self.talk_raw(pack_command(*args))

    def close(self):
        return self.sock.close()


class ClusterNode(object):
    # What does each field mean in "cluster nodes" output
    # > http://oldblog.antirez.com/post/2-4-and-other-news.html
    # but assigned slots / node_index not listed
    def __init__(self, node_id, latest_know_ip_address_and_port,
                 role_in_cluster, node_id_of_master_if_it_is_a_slave,
                 last_ping_sent_time, last_pong_received_time, node_index,
                 link_status, *assigned_slots):
        self.node_id = node_id
        host, port = latest_know_ip_address_and_port.split(':')
        self.host = host
        self.port = int(port)
        self.role_in_cluster = (role_in_cluster.split(',')[1]
                                if 'myself' in role_in_cluster
                                else role_in_cluster)
        self.master_id = node_id_of_master_if_it_is_a_slave
        self.assigned_slots = []
        for slots_range in assigned_slots:
            if '[' == slots_range[0] and ']' == slots_range[-1]:
                # exclude migrating slot
                continue
            if '-' in slots_range:
                begin, end = slots_range.split('-')
                self.assigned_slots.extend(range(int(begin), int(end) + 1))
            else:
                self.assigned_slots.append(int(slots_range))
        self._talker = None

    def talker(self):
        if self._talker is None:
            self._talker = Talker(self.host, self.port)
        return self._talker

    def close(self):
        if self._talker is not None:
            self._talker.close()
            self._talker = None


def _ensure_cluster_status_unset(t):
    m = t.talk_raw(CMD_PING)
    if m.lower() != 'pong':
        raise hiredis.ProtocolError('Expect pong but recv: %s' % m)

    m = t.talk_raw(CMD_INFO)
    logging.debug('Ask `info` Rsp %s', m)
    cluster_enabled = PAT_CLUSTER_ENABLED.findall(m)
    if len(cluster_enabled) == 0 or int(cluster_enabled[0]) == 0:
        raise hiredis.ProtocolError(
            'Node %s:%d is not cluster enabled' % (t.host, t.port))

    m = t.talk_raw(CMD_CLUSTER_NODES)
    logging.debug('Ask `cluster nodes` Rsp %s', m)
    if len(filter(None, m.split('\n'))) != 1:
        raise hiredis.ProtocolError(
            'Node %s:%d is already in a cluster' % (t.host, t.port))

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
    m = t.talk_raw(CMD_PING)
    if m.lower() != 'pong':
        raise hiredis.ProtocolError('Expect pong but recv: %s' % m)

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
    if cluster_state[0] != 'ok':
        raise hiredis.ProtocolError(
            'Node %s:%d is not in a cluster' % (t.host, t.port))


# Redis instance responses to clients BEFORE changing its 'cluster_state'
#   just retry some times, it should become OK
@retry(stop_max_attempt_number=16, wait_fixed=1000)
def _poll_check_status(t):
    m = t.talk_raw(CMD_CLUSTER_INFO)
    logging.debug('Ask `cluster info` Rsp %s', m)
    cluster_state = PAT_CLUSTER_STATE.findall(m)
    cluster_slot_assigned = PAT_CLUSTER_SLOT_ASSIGNED.findall(m)
    if cluster_state[0] != 'ok' or int(
            cluster_slot_assigned[0]) != SLOT_COUNT:
        raise RedisStatusError('Unexpected status after ADDSLOTS: %s' % m)


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


def _migr_keys(src_talker, target_host, target_port, slot):
    while True:
        keys = src_talker.talk('cluster', 'getkeysinslot', slot, 10)
        if len(keys) == 0:
            return
        for k in keys:
            # Why 0, 15000 ? Just following existent codes
            # > https://github.com/antirez/redis/blob/3.0/src/redis-trib.rb
            # > #L784
            m = src_talker.talk('migrate', target_host, target_port, k,
                                0, 15000)
            # don't panic when one of the keys failed to migrate, log & retry
            if m.lower() != 'ok':
                logging.warning(
                    'Not OK while moving key [ %s ] in slot [ %d ]\n'
                    '  Source node - %s:%d => Target node - %s:%d\n'
                    'Got %s\nRetry later', k, slot, src_talker.host,
                    src_talker.port, target_host, target_port, m)


def _migr_slot(source_node, target_node, migrate_count, nodes):
    def expect_talk_ok(m, slot):
        if m.lower() != 'ok':
            raise RedisStatusError('\n'.join([
                'Error while moving slot [ %d ] between' % slot,
                'Source node - %s:%d' % (source_node.host, source_node.port),
                'Target node - %s:%d' % (target_node.host, target_node.port),
                'Got %s' % m]))

    source_talker = source_node.talker()
    target_talker = target_node.talker()
    for slot in source_node.assigned_slots[:migrate_count]:
        expect_talk_ok(
            target_talker.talk('cluster', 'setslot', slot, 'importing',
                               source_node.node_id),
            slot)
        expect_talk_ok(
            source_talker.talk('cluster', 'setslot', slot, 'migrating',
                               target_node.node_id),
            slot)
        _migr_keys(source_talker, target_node.host, target_node.port, slot)

        for node in nodes:
            if node.node_id == source_node.node_id:
                continue
            t = node.talker()
            expect_talk_ok(t.talk(
                'cluster', 'setslot', slot, 'node', target_node.node_id), slot)
        expect_talk_ok(source_talker.talk(
            'cluster', 'setslot', slot, 'node', target_node.node_id), slot)
        expect_talk_ok(target_talker.talk(
            'cluster', 'setslot', slot, 'node', target_node.node_id), slot)


def join_cluster(cluster_host, cluster_port, newin_host, newin_port):
    _ensure_cluster_status_set_at(cluster_host, cluster_port)

    nodes = []
    myself = None
    t = Talker(newin_host, newin_port)

    try:
        _ensure_cluster_status_unset(t)

        m = t.talk('cluster', 'meet', cluster_host, cluster_port)
        logging.debug('Ask `cluster meet` Rsp %s', m)
        if m.lower() != 'ok':
            raise RedisStatusError('Unexpected reply after MEET: %s' % m)

        _poll_check_status(t)
        logging.info('Instance at %s:%d has joined %s:%d; now balancing slots',
                     newin_host, newin_port, cluster_host, cluster_port)

        m = t.talk_raw(CMD_CLUSTER_INFO)
        logging.debug('Ask `cluster info` Rsp %s', m)
        cluster_state = PAT_CLUSTER_STATE.findall(m)
        if cluster_state[0] != 'ok':
            raise hiredis.ProtocolError(
                'Node %s:%d is already in a cluster' % (t.host, t.port))
        slots = int(PAT_CLUSTER_SLOT_ASSIGNED.findall(m)[0])
        m = t.talk_raw(CMD_CLUSTER_NODES)
        logging.debug('Ask `cluster nodes` Rsp %s', m)
        for node_info in m.split('\n'):
            if len(node_info) == 0:
                continue
            node = ClusterNode(*node_info.split(' '))
            if 'myself' in node_info:
                myself = node
                # A new node might have a empty host string because it does not
                # know what interface it binds
                if myself.host == '':
                    myself.host = newin_host
            else:
                nodes.append(node)
        if myself is None:
            raise RedisStatusError('Myself is missing:\n%s' % m)

        mig_slots_in_each = SLOT_COUNT / (1 + len(nodes)) / len(nodes)
        for node in nodes:
            logging.info('Migrating %d slots from %s[%s:%d]',
                         mig_slots_in_each, node.node_id, node.host, node.port)
            _migr_slot(node, myself, mig_slots_in_each, nodes)
    finally:
        t.close()
        if myself is not None:
            myself.close()
        for n in nodes:
            n.close()


def quit_cluster(host, port):
    nodes = []
    myself = None
    t = Talker(host, port)
    try:
        _ensure_cluster_status_set(t)
        m = t.talk_raw(CMD_CLUSTER_NODES)
        logging.debug('Ask `cluster nodes` Rsp %s', m)
        for node_info in m.split('\n'):
            if len(node_info) == 0:
                continue
            node = ClusterNode(*node_info.split(' '))
            if 'myself' in node_info:
                myself = node
            else:
                nodes.append(node)
        if myself is None:
            raise RedisStatusError('Myself is missing:\n%s' % m)

        mig_slots_to_each = len(myself.assigned_slots) / len(nodes)
        for node in nodes[:-1]:
            logging.info('Migrating %d slots to %s[%s:%d]',
                         mig_slots_to_each, node.node_id, node.host, node.port)
            _migr_slot(myself, node, mig_slots_to_each, nodes)
            del myself.assigned_slots[:mig_slots_to_each]
        node = nodes[-1]
        logging.info('Migrating %d slots to %s[%s:%d]',
                     len(myself.assigned_slots), node.node_id, node.host,
                     node.port)
        _migr_slot(myself, node, len(myself.assigned_slots), nodes)

        logging.info('Migrated for %s / Broadcast a `forget`', myself.node_id)
        for node in nodes:
            tk = node.talker()
            tk.talk('cluster', 'forget', myself.node_id)
            t.talk('cluster', 'forget', node.node_id)
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
        myself = ClusterNode(*nodes_info[0].split(' '))

        for s in myself.assigned_slots:
            m = t.talk('cluster', 'countkeysinslot', s)
            logging.debug('Ask `cluster countkeysinslot` Rsp %s', m)
            if m != 0:
                raise RedisStatusError('Slot #%d not empty.', s)

        m = t.talk('cluster', 'delslots', *range(SLOT_COUNT))
        logging.debug('Ask `cluster delslots` Rsp %s', m)
    finally:
        t.close()
