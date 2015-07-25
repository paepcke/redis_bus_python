'''
Created on Jul 24, 2015

@author: paepcke
'''
import Queue
import errno
import socket
import sys
import threading
import time

from redis_bus_python.redis_lib.exceptions import ConnectionError

SYM_CRLF = '\r\n'
SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

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
        self._delivery_queue = Queue.Queue()
        
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
                #***********
                #*******return self._delivery_queue.get(SocketLineReader.DO_BLOCK, SocketLineReader.SOCKET_READ_TIMEOUT)
                return self._delivery_queue.get(False)
                #***********
            except Queue.Empty:
                #***********
                time.sleep(0.01)
                #***********
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

