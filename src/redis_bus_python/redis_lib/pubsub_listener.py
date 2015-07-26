'''
Created on Jul 25, 2015

@author: paepcke
'''
import collections
import threading
import time

from redis_bus_python.redis_lib._compat import iteritems, imap, iterkeys, \
    nativestr
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
        conn = connection_pool.get_connection('InMsgs', shard_hint)
        try:
            self.encoding = conn.encoding
            self.encoding_errors = conn.encoding_errors
            self.decode_responses = conn.decode_responses
        finally:
            connection_pool.release(conn)
        self.reset()
        
        # Start a connection to the server, ignoring
        # the (hopefully) resulting PONG:
        self.execute_command('PING') 

        # Start listening on that connection:        
        self.start()

    def pauseInTraffic(self):
        self.paused = True
        # Get the run() loop out of its
        # hanging call to handle_message():
        self.execute_command('PING')

    def continueInTraffic(self):
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

    def execute_command(self, *args, **kwargs):
        "Execute a publish/subscribe command"

        # NOTE: don't parse the response in this function. it could pull a
        # legitmate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'PubSubInMsgs',
                self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        self._execute(connection, connection.send_command, *args)

    def _execute(self, connection, command, *args):
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
        "Parse the response from a publish/subscribe command"
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
            # Make sure the subscribe event flag is initially clear:
            self.patterns.clearSubscribeAckArrived()
    
            ret_val = self.execute_command('PSUBSCRIBE', *iterkeys(new_patterns))
    
            # Wait for handle_message() to announce that the 
            # subscribe ack from the Redis server has arrived:
            
            ackHappened = self.patterns.awaitSubscribeAck()
            if not ackHappened:
                # If someone called close, just return:
                if self.done:
                    return
                raise ResponseError('Redis server did not acknowledge psubscribe request within %d seconds' % PubSubListener.CMD_ACK_TIMEOUT)
    
            # Now update the self.patterns data structure;
            # acquire the access lock.
            # (update the patternss dict AFTER we send the command. we don't want to
            # subscribe twice to these patterns, once for the command and again
            # for the reconnection.):
            
            self.patterns.update(new_patterns)
            self.patterns.clearSubscribeAckArrived()
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
            
            # Make sure the unsubscribe event flag is initially clear:
            self.patterns.clearUnsubscribeAckArrived()
    
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('PUNSUBSCRIBE', *args)
    
            # If block, wait for handle_message() to announce that the 
            # punsubscribe ack from the Redis server has arrived:
            if block:
                ackHappened = self.patterns.awaitUnsubscribeAck()
                if not ackHappened:
                    # If someone called close, just return:
                    if self.done:
                        return
                    raise ResponseError('Redis server did not acknowledge punsubscribe request within %d seconds' % PubSubListener.CMD_ACK_TIMEOUT)
    
            # Now update the self.patterns data structure;
            # acquire the access lock.
            # (update the patterns dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for pattern in args:
                del self.patterns[pattern]
            self.patterns.clearUnsubscribeAckArrived()
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
            
            # Make sure the subscribe event flag is initially clear:
            self.channels.clearSubscribeAckArrived()
    
            # Send subscribe cmd to server
            ret_val = self.execute_command('SUBSCRIBE', *iterkeys(new_channels))
    
            # Wait for handle_message() to announce that the 
            # subscribe ack from the Redis server has arrived:
            
            ackHappened = self.channels.awaitSubscribeAck()
            if not ackHappened:
                # If someone called close, just return:
                if self.done:
                    return
                raise ResponseError('Redis server did not acknowledge subscribe request within %d seconds' % PubSubListener.CMD_ACK_TIMEOUT)
    
            # Now update the self.channels data structure;
            # acquire the access lock.
            # (update the channels dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            self.channels.update(new_channels)
            self.channels.clearSubscribeAckArrived()
        finally:
            self.channels.release()
    
        return ret_val

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
            # Make sure the unsubscribe event flag is initially clear:
            self.channels.clearUnsubscribeAckArrived()
    
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('UNSUBSCRIBE', *args)
    
            # If block, wait for handle_message() to announce that the 
            # unsubscribe ack from the Redis server has arrived:
            
            if block:
                ackHappened = self.channels.awaitUnsubscribeAck()
                if not ackHappened:
                    # If someone called close, just return:
                    if self.done:
                        return
                    raise ResponseError('Redis server did not acknowledge unsubscribe request within %d seconds' % PubSubListener.CMD_ACK_TIMEOUT)
    
            # Now update the self.channels data structure;
            # acquire the access lock.
            # (update the channels dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for channel in args:
                del self.channels[channel]
            self.channels.clearUnsubscribeAckArrived()
        finally:
            self.channels.release()
            
        return cmdRes

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
        
        if not isinstance(response, basestring):
            return response
        
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
        
