from __future__ import with_statement

import errno
from itertools import chain
import os
from select import select
import socket
import sys
import threading
import time
import warnings

from redis_bus_python.redis_lib import fastqueue
from redis_bus_python.redis_lib._compat import b, xrange, imap, byte_to_chr, \
    unicode, bytes, long, nativestr, basestring, iteritems, LifoQueue, Empty, Full, \
    urlparse, parse_qs, unquote
from redis_bus_python.redis_lib.exceptions import RedisError, ConnectionError, \
    TimeoutError, BusyLoadingError, ResponseError, InvalidResponse, \
    AuthenticationError, NoScriptError, ExecAbortError, ReadOnlyError


try:
    import ssl
    ssl_available = True
except ImportError:
    ssl_available = False


SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."


class Token(object):
    """
    Literal strings in Redis commands, such as the command names and any
    hard-coded arguments are wrapped in this class so we know not to apply
    and encoding rules on them.
    """
    def __init__(self, value):
        if isinstance(value, Token):
            value = value.value
        self.value = value

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


class BaseParser(object):
    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
        'READONLY': ReadOnlyError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            return self.EXCEPTION_CLASSES[error_code](response)
        return ResponseError(response)


class SocketLineReader(threading.Thread):
    
    # Seconds for socket.recv to time out when
    # nothing is arriving, so that we can check
    # whether we are to stop the thread, or purge
    # the delivery buffer:
    SOCKET_READ_TIMEOUT = 0.5
    
    DO_BLOCK = True
    
    def __init__(self, socket, socket_read_size=4096, name=None):
        '''
        Thread responsible for receiving data from the Redis server
        on one socket. The dest_buffer is an object that satisfies
        the same interface as a Queue.Queue.
        
        :param socket: the socket object for which this thread is responsible.
        :type socket: socket
        :param socket_read_size: socket buffer size
        :type socket_read_size: int
        :param name: optional name for this thread; use to make debugging easier
        :type name: string
        '''

        threading.Thread.__init__(self, name='SocketReader' if name is None else 'SocketReader' + name)
        self.setDaemon(True)

        self._sock = socket
        
        self._sock.setblocking(True)
        
        # Make the socket non-blocking, so that
        # we can periodically check whether this
        # thread is to stop:
        self._sock.settimeout(SocketLineReader.SOCKET_READ_TIMEOUT)
        
        self._socket_read_size = socket_read_size
        #******self._delivery_queue = Queue.Queue()
        self._delivery_queue = fastqueue.FastQueue()
        
        self._done = False
        self.start()
        
    def empty(self):
        return self._delivery_queue.empty()
        
    def readline(self):
        
        while not self._done:
            try:
                # Read one whole line, without the closing SYM_CRLF,
                # pausing occasionally to check whether the thread
                # has been stopped:
                return self._delivery_queue.get(SocketLineReader.DO_BLOCK, SocketLineReader.SOCKET_READ_TIMEOUT)
            #****except Queue.Empty:
            except fastqueue.Empty:
                continue
        
    def read(self, length):
        '''
        Read the specified number of bytes from the
        socket. We know from the Redis protocol that
        even binary payloads will be terminated by
        SYM_CRLF. So we read in chunks of lines, adding
        the SYM_CRLF strings that may be part of the
        binary payload back in as we go. Blocks until
        enough bytes have been read from the socket.
        Pauses occasionally to check whether thread
        has been stopped.
        
        Note: the sequence of requested bytes must be
        followed by a SYM_CRLF coming in from the socket. 
        
        :param length: number of bytes to read
        :type length: int
        :return: string of requested length
        :rtype: string
        :raise IndexError: when requested chunk of bytes does not end in SYM_CRLF.
        '''

        if length == 0:
            return ''        
        msg = []
        bytes_read = 0
        
        while not self._done:
            # Get one line from socket, blocking if necessary:
            msgFragment = self.readline()
            if self._done:
                return
    
            # If there was at least one prior
            # fragment, add the SYM_CRLF to
            # its end, b/c that was stripped by
            # readline():
            if len(msg) > 0:
                msg.append(SYM_CRLF)
                bytes_read += 2
            msg.append(msgFragment)
            bytes_read += len(msgFragment)
            if bytes_read < length:
                continue
            if bytes_read == length:
                return ''.join(msg)
        
            # Requested chunk was not followed by SYM_CRLF:    
            raise IndexError('Attempt to read byte sequence of length %d from socket that does not end with \r\n' % length)
        
        
    def stop(self):
        self._done = True

    def close(self):
        self.stop()

    def run(self):
        socket_read_size = self._socket_read_size
        remnant = None

        try:
            while True:
                if self._done:
                    return
                try:
                    data = self._sock.recv(socket_read_size)
                    #**********
                    self._delivery_queue.put_nowait(data)
                    continue
                    #**********
                    
                    if self._done:
                        return
                    if remnant is not None:
                        data = remnant + data
                        remnant = None
                except socket.timeout:
                    # Just check for whether to stop, and
                    # go right into recv again:
                    continue
                except socket.error as e:
                    if e.args[0] == errno.EAGAIN:
                        time.sleep(0.3)
                        continue
                    else:
                        raise
                    
                # An empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise socket.error(SERVER_CLOSED_CONNECTION_ERROR)
                if SYM_CRLF in data:
                    
                    lines = data.split(SYM_CRLF)
                                        
                    # str.split() adds an empty str if the
                    # str ends in the split symbol. If str does
                    # not end in the split symbol, the resulting
                    # array has the unfinished fragment at the end.
                    # Don't push the final element to the out queue
                    # either way:
                    
                    for line in lines[:-1]:
                        self._delivery_queue.put_nowait(line)
                        
                    # Final bytes may or may not have their
                    # closing SYM_CRLF yet:
                    if not data.endswith(SYM_CRLF):
                        # Have a partial line at the end:
                        remnant = lines[-1]
                    
        except socket.error:
            # Will occur legitimately when socket gets closed
            # via a call to the associated Connection instance's
            # disconnect() method: that method does call this
            # thread's stop() method, but the socket may still
            # be hanging in the recv() above:
            if self._done:
                return
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

class PythonParser(BaseParser):
    "Plain Python parsing class"
    encoding = None

    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size
        self._sock = None
        self._buffer = None
                
    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._sock = connection._sock
        self._buffer = SocketLineReader(self._sock, self.socket_read_size, name=connection.name)
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"

        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None

        if self._sock is not None:
            self._sock.close()
            self._sock = None
        self.encoding = None


    def can_read(self):
        return self._buffer and not self._buffer.empty()
    
    def read_int(self):
        '''
        Use if an integer or long is expected in response to a
        command. Provides efficiency for when higher layers know
        what to expect back on this connection.
        
        :return: the expected integer or long
        :rtype: long
        :raise InvalidResponse if a non-integer is received from the Redis server. 
        '''
        response = self._buffer.readline().strip()
        if not response:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        if not response.startswith(':'):
            raise InvalidResponse("Expecting integer response but received '%s'" % response)
        try:
            return long(response[1:])
        except ValueError:
            raise InvalidResponse("Expecting integer response but received '%s'" % response)

    def read_response(self):
        '''
        Reads one line from the wire, and interprets it.
        Example: the acknowledgment to an unsubscribe
        from topic myTopic on the wire looks like this:
        
             *3\r\n$11\r\nUNSUBSCRIBE\r\n$7\r\nmyTopic\r\n:1\r\n'
             
        *3    # three items to follow
        $11   # string of 11 chars
        UNSUBSCRIBE
        $7    # string of 7 chars
        myTopic
        :1    # one topic subscribed to now
        
        Each line will cause a recursive call to this method
        (see elif byte == '*' below).
        
        Simpler calls will be individual elements, such
        as ':12', which returns the integer 12.
        
        These are the possible prefixes; each item
        is followed by a \r\n, which is stripped
        by SocketLineReader:
        
            +<str>    simple string
            :<int>    integer
            $<n>    string of length <n>
            *<num>    start of array with <num> elements

        When the message to parse is the acknowledgment of
        a SUBSCRIBE or UNSUBSCRIBE command, this method
        will set() event self.unsubscribeAckEvent/self.unsubscribeAckEvent.

        :return: response string
        :rtype: string
        '''
        response = self._buffer.readline()
        if not response:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = byte_to_chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error: %s, %s" %
                                  (str(byte), str(response)))

        # server returned an error
        if byte == '-':
            response = nativestr(response)
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # simple-string: response holds result:
        elif byte == '+':
            pass
        # int value
        elif byte == ':':
            response = long(response)
            
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                # Null string:
                return None
            response = self._buffer.read(length)
                        
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for _ in xrange(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        #***********
        #print('Response: %s' % byte + '|' + str(response))
        #***********
                
        return response

DefaultParser = PythonParser


class Connection(object):
    "Manages TCP communication to and from a Redis server"
    
    description_format = "Connection<host=%(host)s,port=%(port)s,db=%(db)s,name=%(_name)s>"

    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, socket_connect_timeout=None,
                 socket_keepalive=False, socket_keepalive_options=None,
                 retry_on_timeout=False, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser, socket_read_size=4096, name=None):
        self.pid = os.getpid()
        self.host = host
        self.port = int(port)
        self.db = db
        self._name = name
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.expectingOrphanedReturn = False
        self.orphanExpirationTime = time.time()
        self.retry_on_timeout = retry_on_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = parser_class(socket_read_size=socket_read_size)
        self._description_args = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
        }
        self._connect_callbacks = []

    def __repr__(self):
        return self.description_format % self._description_args

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name

    @property
    def subscribeAckEvent(self):
        '''
        Returns an Event() object that will be set()
        when the acknowledgment from the Redis server
        to a prior subscribe command arrives.
        
        '''
        return self._parser.subscribeAckEvent

    @property
    def unsubscribeAckEvent(self):
        '''
        Returns an Event() object that will be set()
        when the acknowledgment from the Redis server
        to a prior unsubscribe command arrives.
        
        '''
        return self._parser.unsubscribeAckEvent


    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    def connect(self):
        "Connects to the Redis server if not already connected"
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.error:
            e = sys.exc_info()[1]
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        try:
            self.on_connect()
        except RedisError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            callback(self)

    def _connect(self):
        "Create a TCP socket connection"
        # we want to mimic what socket.create_connection does to support
        # ipv4/ipv6, but we want to set options prior to calling
        # socket.connect()
        err = None
        for res in socket.getaddrinfo(self.host, self.port, 0,
                                      socket.SOCK_STREAM):
            family, socktype, proto, canonname, socket_address = res  #@UnusedVariable
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in iteritems(self.socket_keepalive_options):
                        sock.setsockopt(socket.SOL_TCP, k, v)

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)

                # connect
                sock.connect(socket_address)

                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)
                return sock

            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        raise socket.error("socket.getaddrinfo returned an empty list")

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting to %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            if nativestr(self.read_response()) != 'OK':
                raise AuthenticationError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if nativestr(self.read_response()) != 'OK':
                raise ConnectionError('Invalid Database')

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._sock.sendall(item)
        except socket.timeout:
            self.disconnect()
            raise TimeoutError("Timeout writing to socket")
        except socket.error:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (_errno, errmsg))
        except:
            self.disconnect()
            raise

    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

    def can_read(self, timeout=0):
        "Poll the socket to see if there's data that can be read."
        sock = self._sock
        if not sock:
            self.connect()
            sock = self._sock
        return self._parser.can_read() or \
            bool(select([sock], [], [], timeout)[0])

    def read_int(self):
        '''
        Read an (expected) integer/long result from a previously sent command.
        
        :return: the expected integer/long response
        :rtype: long
        :raise InvalidResponse: if a non-integer is received from the Redis server.
        '''
        try:
            return self._parser.read_int()
        except:
            self.disconnect()
            raise

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response

    def encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, Token):
            return b(value.value)
        elif isinstance(value, bytes):
            return value
        elif isinstance(value, (int, long)):
            value = b(str(value))
        elif isinstance(value, float):
            value = b(repr(value))
        elif not isinstance(value, basestring):
            value = unicode(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def pack_command(self, *args):
        '''
        Pack a series of arguments into the Redis protocol.
        For an optimized special case for PUBLISH messages,
        see pack_publish_command.
        
        :return: array of all arguments packed into the Redis
            wire protocol.
        :rtype: [string]
        '''
        
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        command = args[0]
        if ' ' in command:
            args = tuple([Token(s) for s in command.split(' ')]) + args[1:]
        else:
            args = (Token(command),) + args[1:]

        buff = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF))

        for arg in imap(self.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values
            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, b(str(len(arg))), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b(str(len(arg))),
                                       SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def pack_publish_command(self, channel, msg):
        '''
        Given a message and a channel, return a string that
        is the corresponding wire message. This is an optimized
        special case for PUBLISH messages. 
        
        :param channel: the channel to which to publish
        :type channel: string
        :param msg: the message to publish
        :type msg: string
        '''
        wire_msg = '\r\n'.join(['*3',                # 3 parts to follow
                                '$7',                # 7 letters
                                'PUBLISH',           # <command>
                                '$%d' % len(channel),# num topic-letters to follow 
                                channel,             # <topic>
                                '$%d' % len(msg),    # num msg-letters to follow
                                msg,                 # <msg>
                                ''                   # forces a closing \r\n
                                ])
        return wire_msg


    def pack_commands(self, commands):
        "Pack multiple commands into the Redis protocol"
        output = []
        pieces = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                pieces.append(chunk)
                buffer_length += len(chunk)

            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output

class OneShotConnection(object):
    
    def __init__(self):
        
        self._sock = None
#***** set timeout on sock        
        self.returnedResult = None
        self.resultLock = threading.Lock()
        
    def get_int(self, block=True, timeout=None):
        '''
        read a Redis int from the socket. Ensure
        that it is an int, i.e. of the form ":[0-9]*\r\n.
        After locking self.returnedResult, set self.returnedResult
        to the integer, unlock, and return.
        
        :param block: whether to block, or return immediately if
            no data is available on the socket.
        :type block: bool
        :param timeout: if block==True, how long to block. None or 0: wait forever.
        :type timeout: float
        '''
        
        if block:
            rawRes = self._sock.recv(1024)
        else:
            (rlist, wlist, xlist) = select.select([self._sock], [], [], timeout) #@UnusedVariable
            if len(rlist) == 0: 
                return None
        intRes = int(rawRes[1:].strip())
        return
        
    




class SSLConnection(Connection):
    description_format = "SSLConnection<host=%(host)s,port=%(port)s,db=%(db)s>"

    def __init__(self, ssl_keyfile=None, ssl_certfile=None, ssl_cert_reqs=None,
                 ssl_ca_certs=None, **kwargs):
        if not ssl_available:
            raise RedisError("Python wasn't built with SSL support")

        super(SSLConnection, self).__init__(**kwargs)

        self.keyfile = ssl_keyfile
        self.certfile = ssl_certfile
        if ssl_cert_reqs is None:
            ssl_cert_reqs = ssl.CERT_NONE
        elif isinstance(ssl_cert_reqs, basestring):
            CERT_REQS = {
                'none': ssl.CERT_NONE,
                'optional': ssl.CERT_OPTIONAL,
                'required': ssl.CERT_REQUIRED
            }
            if ssl_cert_reqs not in CERT_REQS:
                raise RedisError(
                    "Invalid SSL Certificate Requirements Flag: %s" %
                    ssl_cert_reqs)
            ssl_cert_reqs = CERT_REQS[ssl_cert_reqs]
        self.cert_reqs = ssl_cert_reqs
        self.ca_certs = ssl_ca_certs

    def _connect(self):
        "Wrap the socket with SSL support"
        sock = super(SSLConnection, self)._connect()
        sock = ssl.wrap_socket(sock,
                               cert_reqs=self.cert_reqs,
                               keyfile=self.keyfile,
                               certfile=self.certfile,
                               ca_certs=self.ca_certs)
        return sock


class UnixDomainSocketConnection(Connection):
    description_format = "UnixDomainSocketConnection<path=%(path)s,db=%(db)s>"

    def __init__(self, path='', db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 retry_on_timeout=False,
                 parser_class=DefaultParser, socket_read_size=4096):
        self.pid = os.getpid()
        self.path = path
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.retry_on_timeout = retry_on_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = parser_class(socket_read_size=socket_read_size)
        self._description_args = {
            'path': self.path,
            'db': self.db,
        }
        self._connect_callbacks = []

    def _connect(self):
        "Create a Unix domain socket connection"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect(self.path)
        return sock

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to unix socket: %s. %s." % \
                (self.path, exception.args[0])
        else:
            return "Error %s connecting to unix socket: %s. %s." % \
                (exception.args[0], self.path, exception.args[1])


class ConnectionPool(object):
    "Generic connection pool"
    @classmethod
    def from_url(cls, url, db=None, decode_components=False, **kwargs):
        """
        Return a connection pool configured from the given URL.

        For example::

            redis://[:password]@localhost:6379/0
            rediss://[:password]@localhost:6379/0
            unix://[:password]@/path/to/socket.sock?db=0

        Three URL schemes are supported:
            redis:// creates a normal TCP socket connection
            rediss:// creates a SSL wrapped TCP socket connection
            unix:// creates a Unix Domain Socket connection

        There are several ways to specify a database number. The parse function
        will return the first specified option:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// scheme, the path argument of the url, e.g.
               redis://localhost/0
            3. The ``db`` argument to this function.

        If none of these options are specified, db=0 is used.

        The ``decode_components`` argument allows this function to work with
        percent-encoded URLs. If this argument is set to ``True`` all ``%xx``
        escapes will be replaced by their single-character equivalents after
        the URL has been parsed. This only applies to the ``hostname``,
        ``path``, and ``password`` components.

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        url_string = url
        url = urlparse(url)
        qs = ''

        # in python2.6, custom URL schemes don't recognize querystring values
        # they're left as part of the url.path.
        if '?' in url.path and not url.query:
            # chop the querystring including the ? off the end of the url
            # and reparse it.
            qs = url.path.split('?', 1)[1]
            url = urlparse(url_string[:-(len(qs) + 1)])
        else:
            qs = url.query

        url_options = {}

        for name, value in iteritems(parse_qs(qs)):
            if value and len(value) > 0:
                url_options[name] = value[0]

        if decode_components:
            password = unquote(url.password) if url.password else None
            path = unquote(url.path) if url.path else None
            hostname = unquote(url.hostname) if url.hostname else None
        else:
            password = url.password
            path = url.path
            hostname = url.hostname

        # We only support redis:// and unix:// schemes.
        if url.scheme == 'unix':
            url_options.update({
                'password': password,
                'path': path,
                'connection_class': UnixDomainSocketConnection,
            })

        else:
            url_options.update({
                'host': hostname,
                'port': int(url.port or 6379),
                'password': password,
            })

            # If there's a path argument, use it as the db argument if a
            # querystring value wasn't specified
            if 'db' not in url_options and path:
                try:
                    url_options['db'] = int(path.replace('/', ''))
                except (AttributeError, ValueError):
                    pass

            if url.scheme == 'rediss':
                url_options['connection_class'] = SSLConnection

        # last shot at the db value
        url_options['db'] = int(url_options.get('db', db or 0))

        # update the arguments from the URL values
        kwargs.update(url_options)

        # backwards compatability
        if 'charset' in kwargs:
            warnings.warn(DeprecationWarning(
                '"charset" is deprecated. Use "encoding" instead'))
            kwargs['encoding'] = kwargs.pop('charset')
        if 'errors' in kwargs:
            warnings.warn(DeprecationWarning(
                '"errors" is deprecated. Use "encoding_errors" instead'))
            kwargs['encoding_errors'] = kwargs.pop('errors')

        return cls(**kwargs)

    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        """
        Create a connection pool. If max_connections is set, then this
        object raises redis.ConnectionError when the pool's limit is reached.

        By default, TCP connections are created connection_class is specified.
        Use redis.UnixDomainSocketConnection for unix sockets.

        Any additional keyword arguments are passed to the constructor of
        connection_class.
        """
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, (int, long)) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        self.reset()

    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.reset()

    def get_connection(self, command_name, *keys, **options):
        '''
        Get a connection from the pool. If a free connection
        is expecting an orphaned return value from, e.g. a prior
        non-blocking call to publish(), then we try to retrieve
        and discard that return value from the candidate connection's
        socket. If that value isn't available yet, we pick a different
        connection. 
        
        :param command_name: name for this connection; useful during debugging
        :type command_name: string
        '''
        
        self._checkpid()
        
        try:
            found_conn = False
            connectionsOrphanWaiting = []
            while not found_conn:
                # Get connection from the pool
                connection = self._available_connections.pop()
                
                # Is this connection expecting a return value
                # from the server that is to be discarded? E.g.
                # from a prior non-blocking publish() call:
                if connection.expectingOrphanedReturn:
                    # Check whether that return val arrived:
                    sock = connection._sock
                    (inputSrcs, outputSrcs, eventSrcs) = select([sock],[],[],0) #@UnusedVariable
                    if len(inputSrcs) > 0:
                        # Scrape the old server return value out of the socket:
                        sock.setblocking(0)
                        try:
                            sock.recv(1024)
                        except socket.error:
                            # If server hung up, or other issue, just fly on:
                            pass
                        sock.setblocking(1)
                        connection.expectingOrphanedReturn = False
                        found_conn = True

                    elif time.time() > connection.orphanExpirationTime:
                        # Give up expecting the return value, and
                        # use the connection:
                        connection.expectingOrphanedReturn = False
                        found_conn = True                        
                    else:
                        # Return value not received yet: get a 
                        # different connection instance from the pool,
                        connectionsOrphanWaiting.append(connection)
                else:
                    found_conn = True
        except IndexError:
            # No available connection in the pool
            # (Or all are waiting for Redis server returns,
            # and their expiration time hasn't come):
            connection = self.make_connection()
            # Throw the still-orphaned connections back into the pool
            # to check for expiration next time:
            self._available_connections.extend(connectionsOrphanWaiting)

        self._in_use_connections.add(connection)
        connection.name = command_name
        return connection


    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._checkpid()
        if connection.pid != self.pid:
            return
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()


class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from redis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    ``:py:class: ~redis.connection.ConnectionPool`` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a ``:py:class: ~redis.exceptions.ConnectionError`` (as the default
    ``:py:class: ~redis.connection.ConnectionPool`` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        # Raise a ``ConnectionError`` after five seconds if a connection is
        # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """
    def __init__(self, max_connections=50, timeout=20,
                 connection_class=Connection, queue_class=LifoQueue,
                 **connection_kwargs):

        self.queue_class = queue_class
        self.timeout = timeout
        super(BlockingConnectionPool, self).__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs)

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def make_connection(self):
        "Make a fresh connection."
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()
        if connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        for connection in self._connections:
            connection.disconnect()
