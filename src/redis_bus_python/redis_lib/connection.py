from __future__ import with_statement

import copy
import errno
from itertools import chain
import os
from parser import Token
import re
from select import select
import socket
import sys
import threading
import time
import warnings



from redis_bus_python.redis_lib._compat import b, imap, unicode, bytes, long, \
    nativestr, basestring, iteritems, LifoQueue, Empty, Full, urlparse, parse_qs, \
    unquote
    
from redis_bus_python.redis_lib.exceptions import RedisError, ConnectionError, \
    TimeoutError, ResponseError, InvalidResponse, AuthenticationError

# Redis library exception types not currently used in this module: 
# from redis_bus_python.redis_lib.exceptions import BusyLoadingError, NoScriptError, ExecAbortError, ReadOnlyError 
    
from redis_bus_python.redis_lib.parser import PythonParser, SYM_EMPTY, SYM_STAR, \
    SYM_CRLF, SYM_DOLLAR, BaseParser


#@UnusedImport
try:
    import ssl
    ssl_available = True
except ImportError:
    ssl_available = False



SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

DefaultParser = PythonParser

class Connection(object):
    '''
    Abstract class to handle connections to the Redis server.
    Subclasses, like ParsedConnection or OneShotConnection
    should be used by applications.
    '''
    
    description_format = "Connection<host=%(host)s,port=%(port)s,db=%(db)s,name=%(name)s>"

    # Socket timeout when awaiting a response
    # from the Redis server:
    REDIS_RESPONSE_TIMEOUT = 0.3 # sec
    
    # Redis protocol start of a string:
    # 2 elements to follow (*2\r\n);
    # length indicator '$':
    WIRE_PROTOCOL_STR_START_PATTERN = re.compile(r'[$]')
    
    # Integer string search pattern:
    INT_PATTERN = re.compile("[0-9]*")
    
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, socket_connect_timeout=1.0,
                 socket_keepalive=False, socket_keepalive_options=None,
                 retry_on_timeout=False, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 socket_read_size=4096, name=None, **kwargs):
        '''
        Called by all subclasses. The parameters control behavior
        of this connection instance's socket. To set socket_timeout
        or socket_connect_timeout to infinite, specify 0. If set
        to None (the default), then defaults are used. See class
        variables for those values. 
        
        :param host:
        :type host:
        :param port:
        :type port:
        :param db:
        :type db:
        :param password:
        :type password:
        :param socket_timeout: number of (fractional) seconds to wait for
            a response from the Redis server
        :type socket_timeout: float
        :param socket_connect_timeout: number of (fractional) seconds to wait when
            connecting to the Redis server
        :type socket_connect_timeout: float
        :param socket_keepalive:
        :type socket_keepalive:
        :param socket_keepalive_options:
        :type socket_keepalive_options:
        :param retry_on_timeout:
        :type retry_on_timeout:
        :param encoding:
        :type encoding:
        :param encoding_errors:
        :type encoding_errors:
        :param decode_responses:
        :type decode_responses:
        :param socket_read_size:
        :type socket_read_size:
        :param name:
        :type name:
        '''
        self.pid = os.getpid()
        self.host = host
        self.port = int(port)
        self.db = db
        self._name = name
        self.password = password
        if socket_timeout is None:
            self.socket_timeout = Connection.REDIS_RESPONSE_TIMEOUT
        else:
            self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout if socket_connect_timeout is not None else self.socket_timeout
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.socket_read_size = socket_read_size
        self.expectingOrphanedReturn = False
        self.orphanExpirationTime = time.time()
        self.retry_on_timeout = retry_on_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = None
        self._description_args = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'name' : self._name
        }
        self._connect_callbacks = []

    def shutdown(self):
        '''
        Free resources with the assumption that this connection
        will never be used again. Example: stop parser thread.
        '''
        self.disconnect()
        if self._parser is not None:
            self._parser.shutdown()

    def __repr__(self):
        return 'Connection<host=%s,port=%s,db=%s,name=%s>' % (self.host,self.port,self.db,self.name)
        #return self.description_format % self._description_args

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

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    def select(self, timeout=None):
        if self._sock is None:
            raise ValueError('Socket has not been initialized for this connection (%s)' % self.name)
        if timeout is None and timeout > 0:
            select([self._sock, [], []])
        else:
            select([self._sock], [], [], timeout)
        

    def connect(self, name=None):
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
            self.on_connect(name=name)
        except RedisError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            callback(self)

    def disconnect(self):
        self._disconnect()
        self.name = None

    def on_connect(self, name=None):
        self.name = name
        self._on_connect()

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect(name=command)
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

    # --------------------- Connection Private Methods -----------------------
    
    def _on_connect(self):
        
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

    def _disconnect(self):
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

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

#----------------------------------

class ParsedConnection(Connection):

    def __init__(self, parser_class=DefaultParser, **kwargs):
        super(ParsedConnection, self).__init__(**kwargs)
        self._parser = parser_class(socket_read_size=self.socket_read_size)

    def on_connect(self, name=None):
        "Initialize the connection, authenticate and select a database"
        self.name = name
        self._parser.on_connect(self)
        self._on_connect()

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        self._disconnect()
        

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

    def read_subscription_cmd_status_return(self, subscription_command, channel):
        return self._parser.read_subscription_cmd_status_return(subscription_command, channel)

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response(encoding=self.encoding)
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response



class OneShotConnection(Connection):
    '''
    Connection that is not backed by a buffer, and
    is not a thread. Used exclusively for messages
    to the Redis server that expect a single return
    data item.
    
    Methods get_int() and get_string() know about the
    Redis wire protocol, and extract the payload. No
    external parser is used. No callbacks are made to
    anywhere. 
    
    '''

    def __init__(self, host='localhost', port=6379, **kwargs):
        super(OneShotConnection, self).__init__(host=host, port=port, **kwargs)
        
        self.returnedResult = None
        
        # Get a basic parser that knows nothing about connections
        # and sockets:
        self._parser = BaseParser()
        
        # When more bytes have been pulled from the server
        # than were requested in one of the read_xxx() methods
        # below, the bytes are saved in the following:
        self.read_bytes = None
        
        # Similarly, readline() remembers lines
        self.lines_read = []
        self.remnant = None
        
        self.connect()
        
    def read_int(self, block=True, timeout=None):
        '''
        Reads a Redis int from the socket. Ensures
        that it is an int, i.e. of the form ":[0-9]*\r\n.
        
        :param block: whether to block, or return immediately if
            no data is available on the socket.
        :type block: bool
        :param timeout: if block==True, how long to block. None or 0: wait forever.
        :type timeout: float
        :return: integer that was returned by the Redis server
        :rtype: int
        :raise ResponseError if returned value is not an integer
        :raises TimeoutError: if no data arrives from server in time. 
        '''
        
        rawRes = self.readline(block=block, timeout=timeout)
        try:
            intRes = int(rawRes[1:].strip())
        except (ValueError, IndexError):
            raise ResponseError("Server did not return an int; returned '%s'" % rawRes)
        return intRes
    
    def read(self, num_bytes, block=True, timeout=None):
        bytes_left_to_read = num_bytes
        
        while bytes_left_to_read > 0:
            if len(self.lines_read) > 0:
                # There already are lines that were read earlier.
                # read those first:
                
                result = ''
                for line in self.lines_read:
                    if len(line) > bytes_left_to_read:
                        result = line[:bytes_left_to_read]
                        # Remove the read bytes from the line:
                        self.lines_read = self.lines_read[bytes_left_to_read:]
                        return result
                    elif len(line) == bytes_left_to_read:
                        return self.lines_read.pop(0)
                    elif len(line) < bytes_left_to_read:
                        # Need to use this line, plus more:
                        line = self.lines_read.pop(0)
                        result += line
                        bytes_left_to_read -= len(line)
                        continue 
                if self.remnant is not None:
                    if len(self.remnant) <= bytes_left_to_read:
                        result += self.remnant
                        if len(self.remnant) == bytes_left_to_read:
                            self.remnant = None
                            return result
                        else: # Remnant > than what we still need
                            len_remnant_part_used = bytes_left_to_read
                            result += self.remnant[:bytes_left_to_read]
                            self.remnant = self.remnant[bytes_left_to_read:]
                            bytes_left_to_read -= len_remnant_part_used 
                            return result
        
            # Either no lines had previously been read, or
            # not enough data was in previously read lines
            # and self.remnant:
            next_line = self.readline(block=block, timeout=timeout)
            self.lines_read = [next_line] + self.lines_read
         
        
    
    def readline(self, block=True, timeout=None):
        '''
        Return one line from the socket, without the closing
        CR/LF, or raise a TimeoutError exception. If socket
        yields more than one line, the remaining lines are
        stored in the self.lines_read array. If a partial 
        line is at the end of the socket, that is stored
        in self.remnant, and prepended to subsequent
        readline() results.
        
        :param block: whether or not to wait for data
        :type block: boolean
        :param timeout: (fractional) seconds before timing out
        :type timeout: float
        :return: a string corresponding to one \r\n-delimited element
            of the wire protocol.
        :rtype: string
        :raise TimeoutError if no data from server in time.
        :raise socket.error if socket problem
        '''

        if len(self.lines_read) > 0:
            return self.lines_read.pop(0)
        while True:
            try:
                # Wait for incoming messages:
                if block:
                    if timeout is None:
                        # Block forever:
                        (readReady, writeReady, errReady) = select([self._sock],[],[]) #@UnusedVariable
                    else:
                        # Block, but with timeout:
                        (readReady, writeReady, errReady) = select([self._sock],[],[], timeout) #@UnusedVariable
                else:
                    # Just poll:
                    (readReady, writeReady, errReady) = select([self._sock],[],[], 0) #@UnusedVariable
                    return None
                
                # Something arrived on the socket.
                data = self._sock.recv(self.socket_read_size)
                # An empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise socket.error(SERVER_CLOSED_CONNECTION_ERROR)
                
                # If over-read before, prepend that remnant:
                if self.remnant is not None:
                    data = self.remnant + data
                    self.remnant = None
            except socket.timeout:
                raise TimeoutError("Server did not respond in time when we expected a line of data.")
            except socket.error as e:
                if e.args[0] == errno.EAGAIN:
                    time.sleep(0.3)
                    continue
                else:
                    raise
                
            if SYM_CRLF in data:
                
                lines = data.split(SYM_CRLF)
                                    
                # str.split() adds an empty str if the
                # str ends in the split symbol. If str does
                # not end in the split symbol, the resulting
                # array has the unfinished fragment at the end.
                
                for line in lines[:-1]:
                    self.lines_read.append(line)
                    
                # Final bytes may or may not have their
                # closing SYM_CRLF yet:
                if not data.endswith(SYM_CRLF):
                    # Have a partial line at the end:
                    self.remnant = lines[-1]
                
                # Return the first line:
                return self.lines_read.pop(0)

    def read_string(self, block=True, timeout=None):
        '''
        Returns a Redis wire protocol encoded string from the 
        socket. Expect incoming data to be a Redis 'simple string',
        or a lenth-specified string: 
        
        Simple string form: +<str>
        Length specified string form: "*2\r\n$<strLen>\r\n<str>
        
        :param block: block for data to arrive from server, or not.
        :type block: bool
        :param timeout: if block == True, when to time out
        :type timeout: float
        :return: the string returned by the server
        :rtype: string
        :raise ResponseError: if response arrives, but is not in proper string format
        :raises TimeoutError: if no data arrives from server in time. 
        '''

        # Strings start with a +, followed by a string, followed
        # by a CR/LF, or with a length, like this:
        #      '$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'\r\n
        
        # Get the length or simple-string
        rawRes = self.readline(block=block, timeout=timeout)

        # Is it a 'simple string', i.e. "+<str>"?
        if rawRes[0] == '+':
            return(rawRes[1:-len(SYM_CRLF)])
        
        # Looking at '$<strLen>':
        try:
            str_len = int(rawRes[1:])
        except ValueError:
            raise ResponseError("Expected integer string length, but received '%s'" % rawRes)
        
        res_str =  self.readline(block=block, timeout=timeout)
        if len(res_str) != str_len:
            raise InvalidResponse("String length %d is not the length of '%s'" % (str_len, res_str))
        
        return res_str

    def write_socket(self, msg):
        '''
        Write an arbitrary string to this OneShotConnection's socket.
        
        :param msg: message to write
        :type msg: string
        :raises: socket.error if something bad happens in the socket system call
        '''
        self._sock.sendall(msg)
        
    def parse_response(self, response=None, block=True, timeout=None):
        '''
        Obtain a server stream of bytes directly
        from the socket, and parse it until one
        completely parsed Redis structure is obtained.
        Any additional bytes read from the buffer are
        safed in self.lines_read and self.remnant.
        
        :param response: a previously obtained line of Redis wire protocol.
            If none, readline() is called.
        :type response: string
        :param block: if True, wait for data from server
        :type block: boolean
        :param timeout: (fractional) seconds to wait for data from server
        :type timeout: float
        :return: an array of parsed Redis
        :rtype: [string]
        '''
        # Parse a response
        return self._parser.parse_response(response, self, encoding=self.encoding, timeout=timeout, block=block)
        

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
    
    
    def pack_subscription_command(self, command, channel):
        '''
        Given a channel and a subscribe or unsubscribe name, return a string that
        is the corresponding wire message. This is an optimized
        special case for (un)subscribe messages.
    
        :param command: which command to construct; must be one of 
            {SUBSCRIBE | UNSUBSCRIBE | PSUBSCRIBE | PUNSUBSCRIBE}
        :type command: string
        :param channel: the channel to which to subscribe
        :type channel: string
        '''

        wire_msg = '\r\n'.join(['*2',                  # 2 parts to follow
                                '$%d' % len(command),  # number of letters in command
                                command,               # <command>
                                '$%d' % len(channel),  # num channel-letters to follow 
                                channel,               # <topic>
                                ''                     # forces a closing \r\n
                                ])
        return wire_msg

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
        # Make print name something like: ConnectionPool<ParsedConnection>s
        class_name = type(self).__name__
        connections_type = self.connection_class.__name__
        return "%s<%s>s" % (
            class_name,
            connections_type,
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

    def get_connection(self, *keys, **options):
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

        # Very fast starting and stopping of server
        # can cause connection already to have been
        # removed:
        try:
            self._in_use_connections.remove(connection)
        except KeyError:
            pass
        if connection not in self._available_connections:
            self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()
    
    def disconnect_all(self):
        conns_copy = copy.copy(self._in_use_connections)
        for conn in conns_copy:
            self.release(conn)
            
        for conn in self._available_connections:
            conn.disconnect()

    def shutdown_all(self):
        self.disconnect_all()
        for connection in self._in_use_connections:
            connection.shutdown()
        for connection in self._available_connections:
            connection.shutdown()

class BlockingConnectionPool(ConnectionPool):
    """
    NOTE: This class is not maintained and likely obsolete.
    
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

    def get_connection(self, *keys, **options):
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
