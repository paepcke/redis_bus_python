'''
Created on Jul 24, 2015

@author: paepcke
'''
import Queue
import errno
import re
from select import select
import socket
import sys
import threading
import time

from redis_bus_python.redis_lib.exceptions import ConnectionError, TimeoutError, \
    ResponseError


SYM_CRLF = '\r\n'
SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

class SocketLineReader(threading.Thread):
    
    # Seconds for socket.recv to time out when
    # nothing is arriving, so that we can check
    # whether we are to stop the thread, or purge
    # the delivery buffer:
    SOCKET_READ_TIMEOUT = 0.5
    
    DO_BLOCK = True
    DONT_BLOCK = False

    # Pattern for extracting the 'subscriptions-left' integer
    # from (p)(un)subscribe command status returns. Example return:
    #    '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'
    # Goal is to capture the '1' at the end in group(1):    
    SUBSRIPTION_RETURN_STATUS_PATTERN = re.compile(r'[^:]*:([0-9]*)')
    
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
        self._delivery_queue = Queue.Queue()
        
        self.socket_lock = threading.Lock()
        
        self._done = False
        self.start()
        
    def empty(self):
        return self._delivery_queue.empty()
        
    def readline(self):
        '''
        Hangs on the queue where the run() method
        places incoming strings. Returns the oldest
        single line from that queue without the 
        trailing CRLF.
        
        :return: one cr/lf delimited line that was received from they server.
        :rtype: string 
        '''
        
        while not self._done:
            try:
                # Read one whole line, without the closing SYM_CRLF,
                # pausing occasionally to check whether the thread
                # has been stopped:
                return self._delivery_queue.get(SocketLineReader.DO_BLOCK, SocketLineReader.SOCKET_READ_TIMEOUT)
            except Queue.Empty:
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
        
    def read_subscription_cmd_status_return(self, subscription_command, channel, timeout=None):
        '''
        Locks access to socket, then creates and sends a
        subscription-related command. Reads the resulting 
        data from the server. It is expected that this data is the returned
        status of the prior (p)(un)subscribe command. These
        returns are of the form:
        
        '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'
        
        with 'subscribe' and its preceding integer changing
        depending on whether the preceding command was
        subscribe, unsubscribe, psubscribe, or punsubscribe:
        
        NOTE: this method should be in this module, because
        it is aware of data formats. The method should be in
        the parser. But for speed we need to access the socket
        directly when retrieving the return:
        
        :param subscription_command: the command to which a status response is expected:
            {subsribe | unsubscribe | psubscribe | punsubscribe}
        :type subscription_command: string
        :param channel:
        :param timeout: optional time in (fractional) seconds to wait for the
            status to be sent from the server. If None, uses default socket_timeout.
            If 0, hangs forever.
        :type timeout: {float | int | None}
        '''
        
        if type(timeout) == float or type(timeout) == int:
            prior_socket_timeout = self._sock.gettimeout()
        else:
            prior_socket_timeout = None

        # Construct the necessary Redis command, like this
        # subscribe command to topic tmp.0:
        #    *2\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n
        
        cmd = '*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(subscription_command),
                                                    subscription_command,
                                                    len(channel),
                                                    channel)
        
        # Wait till the run() method has pulled earlier data
        # from the server into the delivery queue by acquiring
        # socket_lock:
        try:
            with self.socket_lock:
                
                self._sock.sendall(cmd)
                
                # Read the (un)subscription command's return:
                try: 
                    subscription_ret_status = self._sock.recv(self._socket_read_size)
                except socket.timeout:
                    raise TimeoutError('Server did not respond to %s command within %s seconds' % (subscription_command, self._sock.gettimeout())) 
                
                # We might want to ensure that the received material 
                # is in fact the receipt for one of the subscription cmds.
                # We don't to save time. Instead we assume that the
                # return is of the form:
                    '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'
                # and read the trailing integer (in this case '1'):
                try:
                    return int(SocketLineReader.SUBSRIPTION_RETURN_STATUS_PATTERN.match(subscription_ret_status).group(1))
                except Exception:
                    raise ResponseError("Expected returned status for a '%s' command; got %s instead" % (subscription_command, subscription_ret_status))
        finally:
            if prior_socket_timeout is not None:
                self._sock.settimeout(prior_socket_timeout)
        
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
                    
                    # If an (un)subscribe method has written
                    # to the in-message connection, it will use 
                    # the read_subscription_cmd_status_return() method
                    # for the confirmation to that status reply on this
                    # socket. In that case that connection will have
                    # set the socket_lock, and is reading from the
                    # socket directly for speed. Hang till that lock is
                    # released:
                    
                    self.socket_lock.acquire()
                    
                    # Before hanging in the select on the socket,
                    # release the lock to allow future subscribe/unsubscribes;
                    # we'll lock again after select:
                     
                    self.socket_lock.release()
                    
                    # Wait for incoming messages:
                    (readReady, writeReady, errReady) = select([self._sock],[],[], self.SOCKET_READ_TIMEOUT) #@UnusedVariable
                    if len(readReady) == 0:
                        # Timeout out:
                        continue
                    
                    # Something arrived on the socket. If
                    # an (un)subscribe method has acquired the lock
                    # in the meantime, loop, waiting for the lock release
                    # above, because the select was released by the
                    # (un)subscribe status return, which is what the
                    # (un)subscribe methods are waiting for and will read:
                    
                    if not self.socket_lock.acquire(SocketLineReader.DONT_BLOCK):
                        # Subscribe/Unsubscribe activity happening, the
                        # read_subscription_cmd_status_return() method
                        # is using the socket: 
                        continue
                    
                    data = self._sock.recv(socket_read_size)
                    self.socket_lock.release()
                    
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

