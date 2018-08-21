class RedisErrorBase(Exception):
    def __init__(self, message, host, port):
        Exception.__init__(self, '%s:%d - %s' % (host, port, message))
        self.host = host
        self.port = port


class RedisStatusError(RedisErrorBase):
    pass


class RedisIOError(IOError, RedisErrorBase):
    def __init__(self, error, host, port):
        IOError.__init__(self, error)
        RedisErrorBase.__init__(self, error, host, port)
