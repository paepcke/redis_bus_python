'''
Created on Jul 24, 2015

@author: paepcke
'''

from redis_bus_python.redis_lib._compat import byte_to_chr, nativestr

from redis_bus_python.redis_lib.exceptions import NoScriptError, ExecAbortError, ReadOnlyError, \
    ResponseError, BusyLoadingError
    
# Redis library exception types not currently used in this module: 
# from redis_bus_python.redis_lib.exceptions import BusyLoadingError, NoScriptError, ExecAbortError, ReadOnlyError 
    
# from redis_bus_python.redis_lib.exceptions import RedisError, ConnectionError, \
#     TimeoutError, InvalidResponse, AuthenticationError \
    

    
from redis_bus_python.redis_lib.exceptions import ConnectionError #@UnusedImport
from redis_bus_python.redis_lib.exceptions import InvalidResponse #@UnusedImport
from redis_bus_python.redis_lib.socket_reader import SocketLineReader


SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''

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

    def parse_response(self, response=None, socket_buffer=None, encoding=None, block=True, timeout=None):
        '''
        Given a full line of Redis wire protocol,
        parse that line, requesting additional lines if
        needed by requesting them from the passed-in
        socket_buffer. That object must provide a 
        readline(block, timeout) method, else error.
        
        Examples for a response are:
            - *2
            - subscribe
            - $9
            
        This method is called recursively.
        
        :param response: one line of the wire protocol 
        :type response: string
        :param socket_buffer: object that provides a readline(block, timeout) method
        :type socket_buffer: {SocketLineReader | OneShotConnection | ...}
        :returns: parsed response
        :rtype: [string]
        :raise TimeoutError
        '''

        if encoding is None:
            encoding = self.connection.encoding

        if response is None:
            response = socket_buffer.readline(block=block, timeout=timeout)
            
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
            response = socket_buffer.read(length)
                        
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.parse_response(response=None, 
                                            socket_buffer=socket_buffer, 
                                            block=block, 
                                            timeout=timeout,
                                            encoding=encoding) for _ in xrange(length)]
        if isinstance(response, bytes) and encoding:
            response = response.decode(encoding)
        #***********
        #print('Response: %s' % byte + '|' + str(response))
        #***********
                
        return response

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

    def read_subscription_cmd_status_return(self, subscription_command, channel):
        '''
        Given a subscription related command, send the command, parse the status response,
        and return the number of channels caller is subscribed to after
        the command was processed on the server.N
        
        :param subscription_command: the command to which a status response is expected:
            {subsribe | unsubscribe | psubscribe | punsubscribe}
        :type subscription_command: string
        :return: the number of channels caller is subscribed to after 
            the subscription command.
        :rtype: int
        :raises ResponseError: if data read from the server is not 
            of proper format.
        :raises TimeoutError: if server does not respond in time.
        '''
        
        return self._buffer.read_subscription_cmd_status_return(subscription_command, channel)
        

    def read_response(self, socket_buffer=None, encoding=None):
        '''
        Reads one line from the wire, and interprets it.
        Example: the acknowledgment to an unsubscribe
        from channel myChannel on the wire looks like this:
        
             *3\r\n$11\r\nUNSUBSCRIBE\r\n$7\r\nmyChannel\r\n:1\r\n'
             
        *3    # three items to follow
        $11   # string of 11 chars
        UNSUBSCRIBE
        $7    # string of 7 chars
        myChannel
        :1    # one channel subscribed to now
        
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


        :return: response string
        :rtype: string
        '''
        
        if socket_buffer is None:
            socket_buffer = self._buffer
        response = socket_buffer.readline()
        
        if not response:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        return self.parse_response(response, socket_buffer=self._buffer, encoding=encoding)

