'''
Created on Jul 25, 2015

@author: paepcke
'''
import collections
import threading
import time

from redis_bus_python.redis_lib._compat import iteritems, imap, iterkeys, \
    nativestr
from redis_bus_python.redis_lib.connection import Connection
from redis_bus_python.redis_lib.exceptions import ConnectionError, TimeoutError, \
    ResponseError
from redis_bus_python.redis_lib.utils import list_or_args


class PubSubListener(threading.Thread):
    """
    PubSub provides subscribe, unsubscribe and listen support to Redis channels.

    Runs in its own thread, which keeps listening to a socket
    that is connected to the Redis server. The special dicts
    self.channels, and self.patterns keep the channels we are
    currently subscribed to (self.channels), as well as the pattern
    subscriptions (self.pattern).

    """
    PUBLISH_MESSAGE_TYPES = ('message', 'pmessage')
    UNSUBSCRIBE_MESSAGE_TYPES = ('unsubscribe', 'punsubscribe')
    SUBSCRIBE_MESSAGE_TYPES = ('subscribe', 'psubscribe')

    # Time to wait for Redis server to acknowledge receipt
    # of a subscribe or unsubscribe command:
    CMD_ACK_TIMEOUT = 1.0  # seconds
    
    DO_BLOCK   = True
    DONT_BLOCK = False
    
    def __init__(self, connection_pool, oneshot_connection_pool, host='localhost', port=6379, 
                 shard_hint=None, ignore_subscribe_messages=False):
        '''
        Constantly attempt to read the socket coming in from
        the Redis Server. No value is retrieved up to this
        thread. The handle_message() instead forwards the 
        incoming messages. Note that this connection is separate
        from the ones that carry outgoing (publish()'ed) msgs.
        
        :param connection_pool: pool from which to obtain connection instances
        :type connection_pool: ConnectionPool
        :param shard_hint: hint for connection choice (needs clarification)
        :type shard_hint: string
        :param ignore_subscribe_messages: set True if the acknowledgements from subscribe/unsubscribe
            message should just be ignored when they arrive.
        :type ignore_subscribe_messages: bool
        '''

        threading.Thread.__init__(self, name='PubSubInMsgs')
        self.setDaemon(True)
        
        self.connection_pool = connection_pool
        self.oneshot_connection_pool = oneshot_connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.done = False
        self.paused = False
        self.connection = None
        
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        conn = connection_pool.get_connection('shard_hint')
        try:
            self.encoding = conn.encoding
            self.encoding_errors = conn.encoding_errors
            self.decode_responses = conn.decode_responses
        finally:
            connection_pool.release(conn)
        self.reset()
        
        # Create a connection for incoming messages;
        # it will be monitored by the run() method:
        self.connection = self.connection_pool.get_connection(self.shard_hint)
        self.connection.connect(name='PubSubInMsgs')

        # Start listening on that connection:        
        self.start()

    def pause_in_traffic(self):
        self.paused = True
        # Get the run() loop out of its
        # hanging call to handle_message():
        self.execute_command('PING')

    def continue_in_traffic(self):
        self.paused = True

    def stop(self):
        if self.done:
            return
        self.done = True
        # Cause the run() loop to break out
        # of its recv() on the socket to the
        # Redis server:
        self.reset()

    def run(self):
        # Keep hanging on the socket to the
        # redis server, calling handler messages
        # on incoming messages, or pulling in
        # acknowledgments of (p)(un)subscribe 
        # messages we send to the server:
        
        try:
            while not self.done:
                if self.paused:
                    # For testing we sometimes pause reading from
                    # the socket so that other threads can read
                    # it instead:
                    time.sleep(1)
                    continue

                # Finally, act on the incoming message:
                # We don't do anything with the response,
                # because this implementation only supports
                # handler-based operation: the call to
                # handle_message() will call registered
                # handler methods/functions. The assigment
                # stays for setting breakpoints during debugging:
                
                response = self.handle_message(self.parse_response(block=True)) #@UnusedVariable
                
        except Exception as e:
            # If close() was called by the owner of this
            # client, the closing of subsystems eventually
            # causes an exception, such as ConnectionError;
            # we know when a prior close() call was the cause,
            # and just exit this PubSub thread:
            if self.done:
                print("Closing PubSub thread.")
                return
            else:
                # An actual error occurred:
                print('Exit from client: %s' % `e`)
                raise
                return

    def __del__(self):
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self):
        if self.connection:
            self.connection.disconnect()
            self.connection.clear_connect_callbacks()
            self.connection_pool.release(self.connection)
            self.connection = None
        self.channels = ConditionedDict()
        self.patterns = ConditionedDict()

    def close(self):
        self.stop()

    def on_connect(self, connection):
        "Re-subscribe to any channels and patterns previously subscribed to"
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        if self.channels:
            channels = {}
            for k, v in iteritems(self.channels):
                if not self.decode_responses:
                    k = k.decode(self.encoding, self.encoding_errors)
                channels[k] = v
            self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in iteritems(self.patterns):
                if not self.decode_responses:
                    k = k.decode(self.encoding, self.encoding_errors)
                patterns[k] = v
            self.psubscribe(**patterns)

    def encode(self, value):
        """
        Encode the value so that it's identical to what we'll
        read off the connection
        """
        if self.decode_responses and isinstance(value, bytes):
            value = value.decode(self.encoding, self.encoding_errors)
        elif not self.decode_responses and isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    @property
    def subscribed(self):
        "Indicates if there are subscriptions to any channels or patterns"
        return bool(self.channels or self.patterns)

    def execute_command(self, *args):
        '''
        Execute a publish/subscribe command. PUBLISH()
        and other types of commands are processed via
        the client.py's execute_command, not this one.
        The server status response is picked up via
        the run() method and handle_message().
        
        NOTE: For single-channel calls it is faster to use
        the Connection instance's read_subscription_cmd_status_return()
        method, which also waits for the response.
        
        :param args: first arg must be {SUBSCRIBE | UNSUBSCRIBE | PSUBSCRIBE | PUNSUBSCRIBE}
            The following args are channels or patterns to subscribe to.
        :type args: string
        :return: None
        :rtype: None type 
        '''
        conn = self.connection
        if conn is None:
            self.connection = self.connection_pool.get_connection()
            conn = self.connection
        # The callable to _execute is the connection's send_command:
        self._execute(conn, conn.send_command, *args)

    def _execute(self, connection, command, *args):
        '''
        Workhorse for execute_command(). given a connection
        instance, a callable, and arguments, this method
        invokes the callable with args. If the call fails,
        disconnects, reconnects, and tries one more time.
        Example for a command value is connection.send_command.
        
        :param connection: connection on which command is to be delivered to the server
        :type connection: Connection
        :param command: callable to be invoked with the arguments
        :type command: callable
        :return: result of callable
        :rtype: <any>
        '''
        try:
            return command(*args)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # Connect manually here. If the Redis server is down, this will
            # fail and raise a ConnectionError as desired.
            connection.connect()
            # the ``on_connect`` callback should haven been called by the
            # connection to resubscribe us to any channels and patterns we were
            # previously listening to
            return command(*args)

    def parse_response(self, block=True, timeout=0):
        '''
        Parse an incoming message, so that handle_message()
        can digest it.

        :param block: if True, wait for answer
        :type block: bool
        :param timeout: raise exception if no response with timeout (fractional) seconds.
        :type timeout: float
        :return: string parsed from Redis wire protocol.
        :raise TimeoutError: if server does not return a result within
            timeout (fractional) seconds.
        '''
        
        connection = self.connection
        if not block and not connection.can_read(timeout=timeout):
            return None
        res = self._execute(connection, connection.read_response)
          
        return res

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern.
        """
        
        if self.connection is None:
            self.connection = self.connection_pool.get_connection()
        
        if args:
            args = list_or_args(args[0], args[1:])
        new_patterns = {}
        new_patterns.update(dict.fromkeys(imap(self.encode, args)))
        for pattern, handler in iteritems(kwargs):
            new_patterns[self.encode(pattern)] = handler

        # Get the access lock to the dict that
        # holds the pattern-subscriptions:
        self.patterns.acquire()

        try:
            ret_val = self.execute_command('PSUBSCRIBE', *iterkeys(new_patterns))
    
            # Now update the self.patterns data structure;
            # acquire the access lock.
            # (update the patternss dict AFTER we send the command. we don't want to
            # subscribe twice to these patterns, once for the command and again
            # for the reconnection.):
            
            self.patterns.update(new_patterns)
        finally:
            self.patterns.release()

        return ret_val

    def punsubscribe(self, *args, **kwargs):
        """
        Unsubscribe from the supplied patterns. If empy, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
            
        try:
            block = kwargs['block']
        except KeyError:
            block = False
            
        # Get the access lock to the dict that
        # holds the pattern-subscriptions:
        self.patterns.acquire()
        try:
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('PUNSUBSCRIBE', *args)
    
            # Now update the self.patterns data structure;
            # acquire the access lock.
            # (update the patterns dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for pattern in args:
                try:
                    del self.patterns[pattern]
                except KeyError:
                    # Don't get tripped up by unsubscribes that aren't subscribed:
                    pass
        finally:
            self.patterns.release()
            
        return cmdRes

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel.
        """
        if args:
            args = list_or_args(args[0], args[1:])
            
        new_channels = {}
        new_channels.update(dict.fromkeys(imap(self.encode, args)))
        for channel, handler in iteritems(kwargs):
            new_channels[self.encode(channel)] = handler

        # Get the access lock to the dict that
        # holds the channel subscriptions:
        self.channels.acquire()

        try:
            if len(new_channels) == 1:
                # Go the fast way through the 
                # parser to the socket for speed:
                self.connection.read_subscription_cmd_status_return('SUBSCRIBE',new_channels.keys()[0])
                
            else:            
                # Send subscribe cmd to server
                self.execute_command('SUBSCRIBE', *iterkeys(new_channels))
            
            # Now update the self.channels data structure;
            # acquire the access lock.
            # (update the channels dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            self.channels.update(new_channels)
        finally:
            self.channels.release()

    def unsubscribe(self, *args, **kwargs):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            args = list_or_args(args[0], args[1:])

        try:
            block = kwargs['block']
        except KeyError:
            block = False

        # Get the access lock to the dict that
        # holds the channel subscriptions:
        self.channels.acquire()

        try:
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('UNSUBSCRIBE', *args)
    
            # Now update the self.channels data structure;
            # acquire the access lock.
            # (update the channels dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for channel in args:
                try:
                    del self.channels[channel]
                except KeyError:
                    # Don't get tripped up by unsuscribes that aren't subscribed
                    pass
        finally:
            self.channels.release()
            
        return cmdRes
    
    def pub_round_trip(self, to_channel, msg, from_channel, timeout):
        '''
        Special bypass of main subscription path for the 
        synchronous roundtrip case. It is used like a remote
        procedure call to a service, which returns a value.
        
        This method uses its own OneShotConnection instance. 
        The main connection on which subscriptions, publishing, 
        and message listening are done(i.e. self.connection) is 
        not involved. 
         
        The method first subscribes to the from_channel, and then 
        publishes the given message to the to_channel. It then 
        watches the OneShotConnection's socket for a return value
        from a service at the other end. 
        
        NOTE again: this subscription will be for this OneShotConnection
        instance! The main run() method uses a ParsedConnection instance
        in self.connection. That is where the regular subscriptions
        go, so that incoming messages on the new topic are actually
        received. Messages for subscriptions made through this method 
        will arrive at the socket of this connection, and must be
        pulled from the socket. The run() loop will not pick them
        up.
        
        
        :param to_channel:
        :type to_channel:
        :param msg:
        :type msg:
        :param from_channel:
        :type from_channel:
        :param timeout: time to wait for the service to compute its
            response, and send it. If None or zero, hangs forever.
        :type timeout: float
        :raise TimeoutError: if server does not send expected data.
        '''
        conn = self.oneshot_connection_pool.get_connection()
        
        subscribe_cmd   = conn.pack_subscription_command('SUBSCRIBE', from_channel)
        publish_cmd     = conn.pack_publish_command(to_channel, msg)
        unsubscribe_cmd = conn.pack_subscription_command('UNSUBSCRIBE', from_channel)
            
        conn.write_socket(subscribe_cmd)
        
        # Read and discard the returned status.
        # The method will throw a timeout error if the
        # server does not send the status. The
        # status to a subscribe consists of six lines,
        # such as: 
        #       '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'

        for _ in range(6):
            conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT)
        
        # Publish the request:
        self.conn.write_socket(publish_cmd)
        
        response_arr = self.conn.parse_response(block=True, timeout=timeout)
        
        conn.write_socket(unsubscribe_cmd)

        # Read and discard the returned status.
        # Same expected return format as the subscribe above:

        for _ in range(6):
            conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT)
        
        return response_arr

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = self.parse_response(block=False, timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        
        message_type = nativestr(response[0])
        if message_type == 'pmessage':
            message = {
                'type': message_type,
                'pattern': response[1],
                'channel': response[2],
                'data': response[3]
            }
        else:
            message = {
                'type': message_type,
                'pattern': None,
                'channel': response[1],
                'data': response[2]
            }

            
        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # Incoming message;
            # if there's a message handler, invoke it
            handler = None
            if message_type == 'pmessage':
                handler = self.patterns.get(message['pattern'], None)
            else:
                handler = self.channels.get(message['channel'], None)
            if handler:
                handler(message)
                return None
            else:
                return message
            
        elif message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            # if this is an unsubscribe message, indicate that
            # (p)unsubscribe() method(s) may now remove the
            # subscription from memory:
            
            if message_type == 'punsubscribe':
                self.patterns.setUnsubscribeAckArrived()
            else:
                self.channels.setUnsubscribeAckArrived()
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None
            
        elif message_type in self.SUBSCRIBE_MESSAGE_TYPES:
            # this is a (p)subscribe message. ignore if we don't
            # want them, but let (p)subscribe() method(s) know that
            # the ack arrived:
            if message_type == 'psubscribe':
                self.patterns.setSubscribeAckArrived()
            else:
                self.channels.setSubscribeAckArrived()
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message


class ConditionedDict(collections.MutableMapping):
    
    DONT_BLOCK = False

    def __init__(self, *args, **kwargs):
        
        self.store = dict()
        
        self.subscribeAckArrivedEv   = threading.Event()
        self.unsubscribeAckArrivedEv = threading.Event()
        self.accessLock              = threading.Lock()
        
        # Init with passed-in key/value pairs:
        self.store.update(*args, **kwargs)
        
    def acquire(self):
        self.accessLock.acquire()

    def release(self):
        self.accessLock.release()
    
    def awaitSubscribeAck(self, timeout=PubSubListener.CMD_ACK_TIMEOUT):
        return self.subscribeAckArrivedEv.wait(timeout)

    def awaitUnsubscribeAck(self, timeout=PubSubListener.CMD_ACK_TIMEOUT):
        return self.unsubscribeAckArrivedEv.wait(timeout)

    def setSubscribeAckArrived (self):
        self.subscribeAckArrivedEv.set()
        
    def setUnsubscribeAckArrived (self):
        self.unsubscribeAckArrivedEv.set()
        
    def clearSubscribeAckArrived(self):
        self.subscribeAckArrivedEv.clear()

    def clearUnsubscribeAckArrived(self):
        self.unsubscribeAckArrivedEv.clear()

    
    def __setitem__(self, key, value):
        self.store[key] = value

    def __getitem__(self, key):
        return self.store[key]

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)
        
