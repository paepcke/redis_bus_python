'''
Created on Jul 6, 2015

@author: paepcke

TODO:
   o documentation:
        - sync responses now returned on a temporary topic 
             whose name is the incoming msg's ID number

   o TopicWaiter: maybe take out the event facility (e.g. removeTopicEvent())
   o Test unicode topics/ids/content
   o threadedConnection.BlockingConnectionPool.get_connection:
         make it eat orphaned input if needed, as I did in
         corresponding ConnectionPool method.
   
'''

import Queue
import functools
import json
import threading
import time
import types

from bus_message import BusMessage
from redis_bus_python import redis_lib
from redis_bus_python.redis_lib.exceptions import ConnectionError, TimeoutError

# Time to wait in join() for child threads to 
# terminate:
JOIN_WAIT_TIME = 5 # sec

class BusAdapter(object):
    '''
    Provides a simple API to the Redis based SchoolBus.
    The model is a publish/subscribe system onto which 
    messages of different topics may be posted. Clients
    can also subscribe to topics, providing a callback
    function that will be invoked in a separate thread
    that handles only messages for a single topic.
    
    Publishing a message in 'synchronous' mode allows
    the system to be used like an remote procedure call
    facility. 
    
    Once messages arrive, they are automatically 
    encapsulated in an instance of BusMesssage, where its
    content, topic name, and time stamps can be obtained.
    '''
    
    _DO_BLOCK = True
    

    def __init__(self, host='localhost', port=6379, db=0):
        '''
        Initialize the underlying redis-py library.
        '''

        self.resultDeliveryFunc = functools.partial(self._awaitSynchronousReturn)
        self.topicThreads = {}
        
        self.defaultResultCallback = functools.partial(self._defaultResultCallback)
        
        self.rserver =  redis_lib.StrictRedis(host=host, port=port, db=db)
        self.pub_sub = self.rserver.pubsub()
        
    def publish(self, busMessage, sync=False, timeout=None, block=True, auth=None):
        '''
        Main method for publishing a message. 
        If you are publishing a response to a synchronous call, set 
        the topicName in the BusMessage instance to self.get_responseTopic(incoming-bus-msg))
        
        :param busMessage: A BusMessage object.
        :type busMessage: BusMessage
        :param sync: if true, the outgoing message is published. This method will
            then not return until either a return message is received, or a timeout occurs.
        :type sync: bool
        :param timeout: used only if sync == True. Amount of time to wait for
            callee to return a value. If None, wait forever. Value is in (fractional) seconds.
        :type timeout: {float | None}
        :param block: if True, the call will block until the Redis server has returned
            the number of channels to which the message was delivered. If False, 
            the method returns immediately with None, unless sync == True.
        :type block: bool
        :param auth: any authentication to be included in the bus message.
        :type auth: String
        :returns: if sync is False, returns the number of recipients of the publication.
            else returns the server's result.
        :raise TimeoutError if server does not respond in time
        :raise socket.error if low level communication error occurs
        '''
        
        if not isinstance(busMessage, BusMessage):
            raise ValueError('Bus message must be an instance of BusMessage; was %s' % str(busMessage))

        topicName = busMessage.topicName
        if topicName is None:
            raise ValueError('Attempt to publish a BusMessage instance that does not hold a topic name: %s' % str(busMessage))
        msgUuid = busMessage.id
        
        # Get the serialized, UTF-8 encoded message from the BusMessage:
        msg = busMessage.content
            
        # Sanity check on timeout:
        if timeout is not None and type(timeout) != int and type(timeout) != float:
            raise ValueError('Timeout must be None, an int, of a float; was %s' % str(timeout))
        
        # Create a JSON struct (to change time to ISO format
        # use datetime.now().isoformat()):
        msgDict = dict(zip(['id', 'time', 'content'],
                           [msgUuid, int(time.time()), msg]))

        # If synchronous operation requested, wait for response:
        if sync:
            returnTopic = self.getResponseTopic(busMessage)
            # Post the request, and get the response in one command:
            res = self.pub_sub.pub_round_trip(topicName, json.dumps(msgDict), returnTopic, timeout) 
            # Result will be a SchoolBus message, like this:
            #    u'{"content": "Server result here.", 
            #       "id": "4032b287-3e30-4d48-b713-91dd8a0a6e39", 
            #       "time": 1438200349626}'
            
            return json.loads(res)['content']
        else:
            # Not a synchronous call; just publish the request:
            numRecipients = self.rserver.publish(topicName, json.dumps(msgDict), block=block)
            return numRecipients
          
    def subscribeToTopic(self, topicIdentifier, deliveryCallable, threaded=True, context=None):
        '''
        Subscribe to a topic or topic name pattern, specifying how incoming messages on the 
        topic are to be delivered. Two delivery options: if threaded is True then
        this method will spin off a new thread that listens on a Queue.Queue to which
        incoming messages are delivered. The thread will call the deliveryCallable with
        a BusMessage object.

        This is the safest method in that the underlying communications are not
        held up by your message handler. 
        
        The second receiving mechanism is to set threaded to False. In that
        case your deliveryCallable will be called directly by the underlying 
        communication mechanism, passing a BusMessage instance. This method 
        calls your handler faster, but while your handler has control, the 
        underlying communication mechanism cannot process other messages.
        
        If deliveryCallback is None, then method _defaultResultCallback() will be called
        when a message arrives. That method is intended to be a 
        placeholder with no side effects, except that it prints
        information about the message. Just used for testing.
        
        The context is any object or structure that is
        meaningful to the delivery callback. This could be the instance
        of an application on which the deliveryFunc can call
        methods, or an Event object that the deliveryFunc will set()
        when a message has been processed.        
        
        It is a no-op to call this method multiple times for the
        same topic or topic pattern.
        
        The topicIdentifier may be a string, in which case messages matching that
        string exactly will be delivered. The parameter may instead be a Pattern
        instance obtained via re.compile(<(raw)str>). In this case all topics
        matching the pattern are delivered.
                 
        :param topicIdentifier: name or regex Python Pattern instance of topic(s) to listen for.
        :type topicIdentifier: {string | Pattern}
        :param deliveryCallable: a callable that takes a BusMessage instance.
                if None, a default callback is used that just prints information
                from the incoming message.
        :type deliveryCallable: {callable || None}
        :param threaded: if True, messages are delivered through a thread via a queue.
            The thread invokes deliveryCallable
        :type threaded: boolean
        :param context: any stucture that is meaningful to the callback function
        :type context: <any>
        '''
        
        if deliveryCallable is None:
            deliveryCallable = self.defaultResultCallback
        elif type(deliveryCallable) != types.FunctionType and type(deliveryCallable) != functools.partial:
            raise ValueError("Parameter deliveryCallback must be a function, was of type %s" % type(deliveryCallable))            

        try:
            pattern_str = topicIdentifier.pattern
        except AttributeError:
            pattern_str = None
            
        # Already subscribed to this topic/pattern?
        if self.subscribedTo(pattern_str if pattern_str is not None else topicIdentifier):
            return
            
        if threaded:
            delivery_queue = Queue.Queue()
            # Spin off a thread that will listen on the queue:
            deliveryThread = DeliveryThread(pattern_str if pattern_str is not None else topicIdentifier, 
                                            delivery_queue, 
                                            deliveryCallable, 
                                            context)
            deliveryThread.daemon = True
            
            # Remember that we have another thread:            
            self.topicThreads[pattern_str if pattern_str is not None else topicIdentifier] = deliveryThread
            deliveryThread.start()
        else:
            delivery_queue = None

        # Call different underlying methods depending
        # on whether the topic is a pattern:
        if pattern_str is None:
            topic_spec = {topicIdentifier : delivery_queue if threaded else deliveryCallable}
            self.pub_sub.subscribe(context, **topic_spec)
        else:
            topic_spec = {pattern_str : delivery_queue if threaded else deliveryCallable}
            self.pub_sub.psubscribe(context, **topic_spec)

    def _defaultResultCallback(self, inMsg):
        '''
        A callback for incoming message that simply
        prints the message. Used when a caller to
        subscribeToTopic() explicitly specifies None
        for the delivery callback. See also 
        declaration of defaultResultCallback's partial
        function object.
        
        :param inMsg: Any incoming message from the bus.
        :type inMsg: BusMessage
        '''
        msgInfo = '%s (%s): %s' % (inMsg.isoTime, inMsg.topicName, inMsg.content)
        print(msgInfo)

    def unsubscribeFromTopic(self, topicIdentifier=None):
        '''
        Unsubscribes from topic. Stops the topic's thread,
        and removes it from bookkeeping so that the Thread object
        will be garbage collected.
        
        Passing None for topicIdentifier unsubscribes from all topics.
        Passing an re.Pattern for topicIdentifier instead of a string
        properly unsubscribes from the topic.
        
        Calling this method for a topic that is already
        unsubscribed is a no-op.
        
        :param topicIdentifier: name of topic to subscribe from
        :type topicIdentifier: {string | None | pattern}
        '''
        # Is pattern identifier an regex pattern?
        try:
            pattern_str = topicIdentifier.pattern
            # Unsubscribing from a pattern:
            self.pub_sub.punsubscribe(pattern_str)
        except AttributeError:
            # Regular string, not a pattern:
            self.pub_sub.unsubscribe(topicIdentifier)
            pattern_str = topicIdentifier
        
        if topicIdentifier is None:
            # Kill all topic threads:
            for deliveryThread in self.topicThreads.values():
                deliveryThread.stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                deliveryThread.join(JOIN_WAIT_TIME)
                if deliveryThread.is_alive():
                    raise TimeoutError("Unable to stop delivery thread '%s'." % deliveryThread.name)

            self.topicThreads = {}
        else:
            try:
                self.topicThreads[pattern_str].stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                self.topicThreads[topicIdentifier].join(JOIN_WAIT_TIME)
                if self.topicThreads[topicIdentifier].is_alive():
                    raise TimeoutError("Unable to stop topicThreads[%s] thread '%s'." %\
                                       (topicIdentifier, self.topicThreads[topicIdentifier].name))
                
                del self.topicThreads[topicIdentifier]
            except KeyError:
                pass
            
            
    def makeResponseMsg(self, incomingBusMsg, responseContent):
        '''
        Convenience method for responding to a synchronous message.
        The handler that received the incoming message can call this
        method with the original incoming message, and a response
        content. This method will return a new message with its
        content field set to the new content, and the destination
        topic set to the one where the original sender expects the
        response. The SchoolBus protocol specifies that this response
        topic is tmp.<msgId>, where <msgId> is the message ID of the
        incoming message. 
        
        
        :param incomingBusMessage: the BusMessage object that delivered the request to the handler
            from which a response is returned via calling this method.
        :type incomingBusMessage: BusMessage
        :param responseContent: result of the handler's work; this value will be entered
            into the content field of the return message.
        :type responseContent: <any>
        :param timeout: 
        :type timeout:
        '''

        # Response topics are re-stored in BusMessage instances:
        respBusMsg = BusMessage(content=responseContent, 
                                topicName='tmp.' + str(incomingBusMsg.id))
        return respBusMsg
        
        
    def mySubscriptions(self):
        '''
        Return a list of topic names to which this bus adapter is subscribed.
        
        :return: List of topics to which caller is subscribed.
        :rtype: [String]
        '''
        return self.pub_sub.channels.keys() + self.pub_sub.patterns.keys()
        
    def subscribedTo(self, topic):
        return topic in self.mySubscriptions()
        
    def close(self):
        exceptions = []

        try:        
            for subscription in self.mySubscriptions():
                try:
                    self.unsubscribeFromTopic(subscription)
                except Exception as e:
                    if type(e) == ConnectionError:
                        continue
                    else:
                        exceptions.append(`e`)
                
            # Stop all the delivery threads:
            for topicThread in self.topicThreads.values():
                topicThread.stop()
                # Wait just a bit longer than the periodic
                # 'check whether to stop' timeout of the
                # delivery threads' queue hanging: 
                topicThread.join(JOIN_WAIT_TIME)
                if topicThread.is_alive():
                    #raise TimeoutError("Unable to stop topicThread '%s'." % topicThread.name)
                    exceptions.append("Unable to stop topicThread '%s'." % topicThread.name)
                
            self.pub_sub.close()
            self.pub_sub.join(JOIN_WAIT_TIME)
            if self.pub_sub.is_alive():
                #raise TimeoutError("Unable to stop pub_sub thread '%s'." % self.pub_sub.name)
                exceptions.append("Unable to stop pub_sub thread '%s'." % self.pub_sub.name)
        finally:
            if len(exceptions) > 0:
                all_error_msgs = '; '.join(exceptions)
                raise RuntimeError(all_error_msgs)
        

# --------------------------  Private Methods ---------------------

    def getResponseTopic(self, busMsg):
        '''
        Return the topic to which a service must post the result
        of a synchronous call. Currently this is 'tmp.<msgId>' where
        <msgId> is the 'id' field of the request message.
        
        :param busMsg: message to which a response is to be posted.
        :type busMsg: BusMessage
        :return: topic name of topic on which a response may be
            expected by the caller.
        :rtype: string
        :raise AttributeError if the passed-in object did not contain the
            information necessary to construct the temporary topic name.
        '''
        return 'tmp.%s' % busMsg.id
          
    def _awaitSynchronousReturn(self, busMsg, context=None):
        '''
        A callback for the result of a particular synchronous
        publish(). Only the response from that 'RPC' call will
        land here.
        
        :param busMsg: BusMessage object that includes everything about the result.
        :type busMsg: BusMessage
        :param context: an inter-thread queue through which the result in the busMsg
            is delivered to the publish() method that's hanging on that queue.
        :type context: Queue.Queue 
        '''

        # Context must be a queue to which we can deliver
        # the result when it comes in:
        if not isinstance(context, Queue.Queue):
            raise TypeError("Message handler for synchronous-call returns must be passed a Queue.Queue as context; was '%s'" % str(context))

        # Put the result into the queue on which the publish() method is hanging:        
        context.put_nowait(busMsg.content)
            

    def playWithRedis(self):
        
#         self.rserver.set('foo', 'bar')
#         res = self.rserver.get('foo')
#         print('Foo is %s' % res)
#         
#         self.rserver.hset('myDict', 'bluebell', 10)
#         res = self.rserver.hget('myDict', 'bluebell')
#         print('Bluebell in myDict is %s' % res)
        
        # print('RESPONSE_CALLBACKS: %s' % str(StrictRedis.RESPONSE_CALLBACKS))
#         self.rserver.publish('myTopic', 'foobar')

#         threading.Timer(4, ding).start()
#         self.pubsub.listen()
#         print("Got out of listen()")

#        self.publish('Hello world', 'myTopic', False)
#        self.close()

        pass

class DeliveryThread(threading.Thread):
    '''
    Thread that handles message arriving from a single topic.
    The thread will listen for incoming messages on an input queue,
    and call a given handler function whenever a message arrives.
    '''
    
    DO_BLOCK = True
    
    def __init__(self, topicName, deliveryQueue, deliveryFunc, context):
        '''
        Start a thread to handle messages for one given topic.
        The delivery queue is a Queue.Queue on which the
        run() message will receive BusMessage instances from
        TopicWaiter. When such an instance arrives, the
        run() method calls deliveryFunc with the BusMessage and
        context. The context is any object or structure that is
        meaningful to the deliveryFunc. This could be the instance
        of an application on which the deliveryFunc can call
        methods, or an Event object that the deliveryFunc will set()
    
        :param topicName: name of topic for which this thread is responsible
        :type topicName: string
        :param deliveryQueue: queue on which messages will arrive
        :type deliveryQueue: Queue.Queue
        :param deliveryFunc: function to call with an incoming bus message
        :type deliveryFunc: <function>
        :param context: any structure useful to the delivery function
        :type context: <any>
        '''
        threading.Thread.__init__(self, name=topicName + 'Thread')
        self.daemon = True
        
        self.deliveryQueue = deliveryQueue
        self.deliveryFunc  = deliveryFunc
        self.context       = context
        self.done = False
        
    def stop(self):
        self.done = True
        
        # Put anything into the queue to
        # release the get() in the run() method.
        # This method yields a faster 
        # stoppage than using a timeout in the get():
        
        self.deliveryQueue.put_nowait('x')
        
    def run(self):
        
        while not self.done:
            
            # Block indefinitely, or until the stop() method 
            # has been called:
            
            busMsg = self.deliveryQueue.get(DeliveryThread.DO_BLOCK)
            
            # check whether stop() was called; if so,
            # that stop() method will have placed an item
            # into the deliveryQueue to release the above get().
            # Just close down the thread:
            if self.done:
                return
            
            # Call the delivery callback:
            self.deliveryFunc(busMsg)


if __name__ == '__main__':
    
    schoolBus = BusAdapter()
    schoolBus.playWithRedis()