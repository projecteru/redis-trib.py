from werkzeug.utils import cached_property

from .connection import Connection


class ClusterNode(object):
    def __init__(self, node_id, latest_know_ip_address_and_port, flags,
                 master_id, last_ping_sent_time, last_pong_received_time,
                 node_index, link_status, *assigned_slots):
        self.node_id = node_id
        host, port = latest_know_ip_address_and_port.split('@')[0].split(':')
        self.host = host
        self.port = int(port)
        self.flags = flags.split(',')
        self.master_id = None if master_id == '-' else master_id
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

        self._conn = None

    def addr(self):
        return '%s:%d' % (self.host, self.port)

    @cached_property
    def role_in_cluster(self):
        return 'master' if self.master else 'slave'

    @cached_property
    def myself(self):
        return 'myself' in self.flags

    @cached_property
    def master(self):
        return 'master' in self.flags

    @cached_property
    def slave(self):
        return not self.master

    @cached_property
    def fail(self):
        return 'fail' in self.flags or 'fail?' in self.flags

    def get_conn(self):
        if self._conn is None:
            self._conn = Connection(self.host, self.port)
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def talker(self):
        import warnings
        warnings.warn(
            'redistrib.clusternode.ClusterNode.talker is deprecated and will be removed at the next release.'
        )
        return self.get_conn()


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

    result_slots = [total_slots * w // total_weight for w in weights]
    frag_slots = total_slots - sum(result_slots)

    migratings = [[n, r - o]
                  for n, r, o in zip(nodes, result_slots, origin_slots)]

    for m in migratings:
        if frag_slots > -m[1] > 0:
            frag_slots += m[1]
            m[1] = 0
        elif frag_slots <= -m[1]:
            m[1] += frag_slots
            break

    migrating = sorted(
        [m for m in migratings if m[1] != 0], key=lambda x: x[1])
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
