import socket
import hiredis
import logging

SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''


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


def squash_commands(commands):
    output = []
    buf = ''

    for c in commands:
        buf = SYM_EMPTY.join((buf, SYM_STAR, str(len(c)), SYM_CRLF))

        for arg in map(encode, c):
            if len(buf) > 6000 or len(arg) > 6000:
                output.append(SYM_EMPTY.join((buf, SYM_DOLLAR, str(len(arg)),
                                              SYM_CRLF)))
                output.append(arg)
                buf = SYM_CRLF
            else:
                buf = SYM_EMPTY.join((buf, SYM_DOLLAR, str(len(arg)),
                                      SYM_CRLF, arg, SYM_CRLF))
    output.append(buf)
    return output


def pack_command(command, *args):
    return squash_commands([(command,) + args])

CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_CLUSTER_INFO = pack_command('cluster', 'info')


class Talker(object):
    def __init__(self, host, port, timeout=5):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reader = hiredis.Reader()
        self.last_raw_message = ''

        self.sock.settimeout(timeout)
        logging.debug('Connect to %s:%d', host, port)
        self.sock.connect((host, port))

    def _recv(self):
        while True:
            m = self.sock.recv(16384)
            self.last_raw_message += m
            self.reader.feed(m)
            r = self.reader.gets()
            if r != False:
                return r

    def _recv_multi(self, n):
        resp = []
        while len(resp) < n:
            m = self.sock.recv(16384)
            self.last_raw_message += m
            self.reader.feed(m)

            r = self.reader.gets()
            while r != False:
                resp.append(r)
                r = self.reader.gets()
        return resp

    def talk_raw(self, command, recv=None):
        recv = recv or self._recv
        for c in command:
            self.sock.send(c)
        r = recv()
        if r is None:
            raise ValueError('No reply')
        if isinstance(r, hiredis.ReplyError):
            raise r
        return r

    def talk(self, *args):
        return self.talk_raw(pack_command(*args))

    def talk_bulk(self, cmd_list):
        return self.talk_raw(squash_commands(cmd_list),
                             recv=lambda: self._recv_multi(len(cmd_list)))

    def close(self):
        return self.sock.close()


class ClusterNode(object):
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
        self.slots_migrating = False
        for slots_range in assigned_slots:
            if '[' == slots_range[0] and ']' == slots_range[-1]:
                # exclude migrating slot
                self.slots_migrating = True
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


class BaseBalancer(object):
    def weight(self, clusternode):
        return 1


def base_balance_plan(nodes, balancer=None):
    if balancer is None:
        balancer = BaseBalancer()
    nodes = [n for n in nodes if 'master' == n.role_in_cluster]
    origin_slots = [len(n.assigned_slots) for n in nodes]
    total_slots = sum(origin_slots)
    weights = [balancer.weight(n) for n in nodes]
    total_weight = sum(weights)

    result_slots = [total_slots * w / total_weight for w in weights]
    frag_slots = total_slots - sum(result_slots)

    migratings = [[n, r - o] for n, r, o in
                  zip(nodes, result_slots, origin_slots)]

    for m in migratings:
        if frag_slots > -m[1] > 0:
            frag_slots += m[1]
            m[1] = 0
        elif frag_slots <= -m[1]:
            m[1] += frag_slots
            break

    migrating = sorted([m for m in migratings if m[1] != 0],
                       key=lambda x: x[1])
    mig_out = 0
    mig_in = len(migrating) - 1

    plan = []
    while mig_out < mig_in:
        if migrating[mig_in][1] < -migrating[mig_out][1]:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         migrating[mig_in][1]))
            migrating[mig_out][1] += migrating[mig_in][1]
            mig_in -= 1
        elif migrating[mig_in][1] > -migrating[mig_out][1]:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         -migrating[mig_out][1]))
            migrating[mig_in][1] += migrating[mig_out][1]
            mig_out += 1
        else:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         migrating[mig_in][1]))
            mig_out += 1
            mig_in -= 1

    return plan
