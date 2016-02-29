'''
Created on Jul 25, 2015

@author: paepcke
'''
import Queue
import collections
import threading
import time

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_lib._compat import iteritems, iterkeys, \
    nativestr
from redis_bus_python.redis_lib.connection import Connection

from redis_bus_python.redis_lib.exceptions import ConnectionError, TimeoutError

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
        self.daemon = True
        
        self.connection_pool = connection_pool
        self.oneshot_connection_pool = oneshot_connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.done = False
        self._paused = False
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
        self._paused = True
        # Get the run() loop out of its
        # hanging call to handle_message():
        self.execute_command('PING')

    def continue_in_traffic(self):
        self._paused = True

    def stop(self):
        if self.done:
            return
        self.done = True
        # Cause the run() loop to break out
        # of its recv() on the socket to the
        # Redis server:
        self.reset()
        
        self.connection_pool.shutdown_all()
        self.oneshot_connection_pool.shutdown_all()        

    def run(self):
        # Keep hanging on the socket to the
        # redis server, calling handler messages
        # on incoming messages, or pulling in
        # acknowledgments of (p)(un)subscribe 
        # messages we send to the server:
        
        try:
            while not self.done:
                if self._paused:
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
                
        except Exception:
            # If close() was called by the owner of this
            # client, the closing of subsystems eventually
            # causes an exception, such as ConnectionError;
            # we know when a prior close() call was the cause,
            # and just exit this PubSub thread:
            if self.done:
                return
            else:
                # An actual error occurred:
                #print('Exit from client: %s' % `e`)
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
        self.connection_pool.disconnect_all()
        self.oneshot_connection_pool.disconnect_all()
        if self.connection:
            self.connection.clear_connect_callbacks()
            self.connection = None
        self.channels = ConditionedDict()
        self.patterns = ConditionedDict()

    def close(self):
        self.stop()
        self.reset()

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
            if not (connection.retry_on_timeout and isinstance(e, TimeoutError)) \
                or self.done:
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

    def psubscribe(self, *context, **delivery_mechanisms):
        """
        Subscribe to channel patterns. Patterns are specified as keyword arguments.
        The pattern string is the key and the value is either a callable, or
        a Queue.Queue. If a callable, then it will be invoked when a message arrives
        that pattern. The argument will be a BusMessage. If a keyword argument's
        value is a Queue.Queue instance, then the BusMessage that encapsulates an
        incoming message will be placed on the queue.
        
        :param *context: an optional data structure that will be included
            in the BusMessage object that is passed to the callable(s) when
            messages arrive. The context will be available in the BusMessage
            as property 'context'. Used, for instance, to pass an Event object
            that the callable sets when it is done.
        :type *context: <any>
        :param *delivery_mechanisms-keys: keys of keyword arguments are channel patterns
            to subscribe to.
        :type *delivery_mechanisms-keys: string
        :param *delivery_mechanisms-values: either a callable or a Queue.Queue instance
        :type *delivery_mechanisms-values: {callable | Queue.Queue}
        :returns: the number of channels the caller is now subscribed to.
        :rtype: int
        :raises TimeoutError: when server does not respond in time.
        """
        
        if self.connection is None:
            self.connection = self.connection_pool.get_connection()
        
        if context:
            context = context[0]
        else:
            context = None
        new_patterns = {}
        for pattern, handler_or_queue in iteritems(delivery_mechanisms):
            # Map channel to a tuple containing the handler or queue and
            # the context for use in handle_message():
            new_patterns[self.encode(pattern)] = (handler_or_queue, context)

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

    def punsubscribe(self, *channel_patterns):
        '''
        Unsubscribe from the supplied channel_patterns. If empy, unsubscribe from
        all channel_patterns.
        
        :param channel_patterns: channel channel_patterns to unsubscribe from
        :type channel_patterns: string
        :returns: number of remaining subscriptions
        :rtype: int
        :raises TimeoutError: if server does not acknowledge receipt in time 
        '''

        # Get the access lock to the dict that
        # holds the pattern-subscriptions:
        self.patterns.acquire()
        try:
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('PUNSUBSCRIBE', *channel_patterns)
    
            # Now update the self.channel_patterns data structure;
            # acquire the access lock.
            # (update the channel_patterns dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for pattern in channel_patterns:
                try:
                    del self.patterns[pattern]
                except KeyError:
                    # Don't get tripped up by unsubscribes that aren't subscribed:
                    pass
        finally:
            self.patterns.release()
            
        return cmdRes

    def subscribe(self, *context, **delivery_mechanisms):
        """
        Subscribe to channel name. Channel names are specified as keyword arguments.
        The name string is the key and the value is either a callable, or
        a Queue.Queue. If a callable, then it will be invoked when a message arrives
        that channel name. The argument will be a BusMessage. If a keyword argument's
        value is a Queue.Queue instance, then the BusMessage that encapsulates an
        incoming message will be placed on the queue.
        
        :param *context: an optional data structure that will be included
            in the BusMessage object that is passed to the callable(s) when
            messages arrive. The context will be available in the BusMessage
            as property 'context'. Used, for instance, to pass an Event object
            that the callable sets when it is done.
        :type *context: <any>
        :param *delivery_mechanisms-keys: keys of keyword arguments are channel names
            to subscribe to.
        :type *delivery_mechanisms-keys: string
        :param *delivery_mechanisms-values: either a callable or a Queue.Queue instance
        :type *delivery_mechanisms-values: {callable | Queue.Queue}
        :returns: the number of channels the caller is now subscribed to.
        :rtype: int
        :raises TimeoutError: when server does not respond in time.
        """

        if context:
            context = context[0]
        else:
            context = None
            
        new_channels = {}
        
        
        for channel, handler_or_queue in iteritems(delivery_mechanisms):
            # Map channel to a tuple containing the handler or queue and
            # the context for use in handle_message():
            new_channels[self.encode(channel)] = (handler_or_queue, context)

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

    def unsubscribe(self, *channel_names):
        '''
        Unsubscribe from the supplied channel names. If empy, unsubscribe from
        all names.
        
        :param channel_names: channel patterns to unsubscribe from
        :type channel_names: string
        :returns: number of remaining subscriptions
        :rtype: int
        :raises TimeoutError: if server does not acknowledge receipt in time 
        '''

        # Get the access lock to the dict that
        # holds the channel subscriptions:
        self.channels.acquire()

        try:
            # Send unsubscribe command to the server:
            cmdRes = self.execute_command('UNSUBSCRIBE', *channel_names)
    
            # Now update the self.channels data structure;
            # acquire the access lock.
            # (update the channels dict AFTER we send the command. we don't want to
            # subscribe twice to these channels, once for the command and again
            # for the reconnection.):
            
            for channel in channel_names:
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
        
        # Redis does not allow PUBLISH commands on any connection
        # that is subscribed to at least one topic; so get one
        # connection for subscribing, and another for publishing:
        
        sub_unsub_conn = self.oneshot_connection_pool.get_connection()
        pub_conn = self.oneshot_connection_pool.get_connection()
        
        subscribe_cmd   = sub_unsub_conn.pack_subscription_command('SUBSCRIBE', from_channel)
        publish_cmd     = sub_unsub_conn.pack_publish_command(to_channel, msg)
        unsubscribe_cmd = sub_unsub_conn.pack_subscription_command('UNSUBSCRIBE', from_channel)

        try:
            
            sub_unsub_conn.write_socket(subscribe_cmd)
            
            # Read and discard the returned status.
            # The method will throw a timeout error if the
            # server does not send the status. The
            # status to a subscribe consists of six lines,
            # such as: 
            #       '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'
    
            for _ in range(6):
                sub_unsub_conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT)
                        
            # Publish the request; but here is a weird thing:
            # If *****
            pub_conn.write_socket(publish_cmd)
            # Read number of recipients:
            pub_conn.read_int(block=True, timeout=timeout)
            
            # Read the service's response:
            try:
                response_arr = sub_unsub_conn.parse_response(block=True, timeout=timeout)
            finally:
                # Make sure that we unsubscribe even if the
                # response from the remote doesn't come (i.e. timeout):
                
                sub_unsub_conn.write_socket(unsubscribe_cmd)
        
                # Read and discard the returned status.
                # Same expected return format as the subscribe above:
        
                for _ in range(6):
                    sub_unsub_conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT)
            
            # The response_arr now has ['message', <channel>, <payload>].
            # Return the payload: 
            return response_arr[2]
        finally:
            self.oneshot_connection_pool.release(sub_unsub_conn)
            self.oneshot_connection_pool.release(pub_conn)

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
        with a message handler_or_queue, the handler_or_queue is invoked instead of a parsed
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

            # Incoming message.
            # Find the queue or callable, and context that are associated with
            # the incoming pattern/channel-name. Create a BusMessage, and either
            # call the callable with that instance, or place the instance in the
            # queue:
             
            handler_or_queue = None
            
            if message_type == 'pmessage':
                try:
                    (handler_or_queue, context) = self.patterns.get(message['pattern'], None)
                except TypeError:
                    # No callable or queue associated with this pattern:
                    return None
            else:
                try:
                    (handler_or_queue, context) = self.channels.get(message['channel'], None)
                except TypeError:
                    # No callable or queue associated with this channel name:
                    return None

            # Make a bus object, setting isJsonContent to True. This will
            # have the BusMessage __init__() method try to parse the data 
            # as a JSON message that has content/id/time fields. If the message
            # is not proper JSON, the BusMessage init function will just put
            # the data itself into the content field:
            
            busMsg = BusMessage(content=message['data'], topicName=message['channel'], isJsonContent=True, context=context)
            
            if isinstance(handler_or_queue, Queue.Queue):
                handler_or_queue.put(busMsg)
            else:
                try:
                    handler_or_queue(busMsg)
                except TypeError:
                    raise TypeError("Message delivery method for %s was neither a queue nor a callable: %s" %\
                                     (busMsg.topicName, handler_or_queue))
            
            return None
            
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
        
