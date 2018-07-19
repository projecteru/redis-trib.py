import logging
import socket
import hiredis
import six
from six import b
from functools import wraps

from .exceptions import RedisStatusError, RedisIOError

SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
EMPTY = b('')

ENCODING='utf-8'


def encode(value):
    if isinstance(value, six.binary_type):
        return value
    if isinstance(value, six.integer_types):
        return b(str(value))
    if isinstance(value, float):
        return b(repr(value))
    if isinstance(value, six.text_type):
        return value.encode(ENCODING)
    if not isinstance(value, six.string_types):
        return b(value)
    return value


def squash_commands(commands):
    output = []
    buf = EMPTY

    for c in commands:
        buf = EMPTY.join((buf, SYM_STAR, b(str(len(c))), SYM_CRLF))

        for arg in map(encode, c):
            if len(buf) > 6000 or len(arg) > 6000:
                output.append(EMPTY.join((buf, SYM_DOLLAR, b(str(len(arg))),
                                          SYM_CRLF)))
                output.append(arg)
                buf = SYM_CRLF
            else:
                buf = EMPTY.join((buf, SYM_DOLLAR, b(str(len(arg))), SYM_CRLF,
                                  arg, SYM_CRLF))
    output.append(buf)
    return output


def pack_command(command, *args):
    return squash_commands([(command,) + args])


CMD_INFO = pack_command('info')
CMD_CLUSTER_NODES = pack_command('cluster', 'nodes')
CMD_CLUSTER_INFO = pack_command('cluster', 'info')


def _wrap_sock_op(f):
    @wraps(f)
    def g(conn, *args, **kwargs):
        try:
            return f(conn, *args, **kwargs)
        except IOError as e:
            raise RedisIOError(e, conn.host, conn.port)
    return g


class Connection(object):
    def __init__(self, host, port, timeout=5):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reader = hiredis.Reader()
        self.last_raw_message = EMPTY

        self.sock.settimeout(timeout)
        logging.debug('Connect to %s:%d', host, port)
        self._conn()

    @_wrap_sock_op
    def _conn(self):
        self.sock.connect((self.host, self.port))

    @_wrap_sock_op
    def _recv(self):
        while True:
            m = self.sock.recv(16384)
            self.last_raw_message += m
            self.reader.feed(m)
            r = self.reader.gets()
            if r != False:
                return r

    @_wrap_sock_op
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

    @_wrap_sock_op
    def send_raw(self, command, recv=None):
        recv = recv or self._recv
        for c in command:
            self.sock.send(c)
        r = recv()
        if r is None:
            raise ValueError('No reply')
        if isinstance(r, hiredis.ReplyError):
            raise r

        if isinstance(r, list):
            return [i.decode(ENCODING) for i in r]
        return r.decode(ENCODING)

    def execute(self, *args):
        return self.send_raw(pack_command(*args))

    def execute_bulk(self, cmd_list):
        return self.send_raw(squash_commands(cmd_list),
                             recv=lambda: self._recv_multi(len(cmd_list)))

    def close(self):
        return self.sock.close()

    def raise_(self, message):
        raise RedisStatusError(message, self.host, self.port)

    def __enter__(self):
        return self

    def __exit__(self, except_type, except_obj, tb):
        self.close()
        return False

    def talk_raw(self, command, recv=None):
        return self.send_raw(command, recv)

    def talk(self, *args):
        return self.execute(*args)

    def talk_bulk(self, cmd_list):
        return self.execute_bulk(cmd_list)
