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
from redis_bus_python.topic_waiter import _TopicWaiter
from schoolbus_exceptions import SyncCallTimedOut

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
        self.topicWaiterThread = _TopicWaiter(self, host=host, port=port, db=db, threadName='TopicWaiterThread')
        
        #***********
        #self.topicWaiterThread.start()
        #***********        
        
    def publish(self, busMessage, sync=False, timeout=None, block=True, auth=None):
        '''
        Main method for publishing a message. 
        If you are publishing a response to a synchronous call, set 
        the topicName in the BusMessage instance to self.get(responseTopic(incoming-bus-msg))
        
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
                           [msgUuid, int(time.time()*1000), msg]))

        # If synchronous operation requested, wait for response:
        if sync:
            
            # Before publishing the request, must prepare for 
            # a function that will be invoked with the result.
            
            # Queue through which the thread that waits for
            # the result will deliver the result once it arrives:
            resultDeliveryQueue = Queue.Queue()
            
            returnTopic = self.getResponseTopic(busMessage)
            
            # Create a temporary topic for use just
            # the return result; provide the result queue
            # to the waiting handler as context for it to know where
            # to put the result value:

            self.subscribeToTopic(returnTopic,
                                  deliveryCallback=self.resultDeliveryFunc, 
                                  context=resultDeliveryQueue)
            try:

                # Finally: post the request...; 
                numRecipients = self.topicWaiterThread.rserver.publish(topicName, json.dumps(msgDict), block=block)
                
                # And wait for the result:
                try:
                    res = resultDeliveryQueue.get(BusAdapter._DO_BLOCK, timeout)
                    
                except Queue.Empty:
                    raise SyncCallTimedOut("Synchronous publish to topic %s timed out before result was returned." % topicName)
                    
                return res
            
            finally:
                # Make sure the temporary topic used for returning the
                # result is always left without a subscription so that
                # Redis will destroy it:
                self.unsubscribeFromTopic(returnTopic, block=False)
            
        else:
            # Not a synchronous call; just publish the request:
            numRecipients = self.topicWaiterThread.rserver.publish(topicName, json.dumps(msgDict), block=block)
            return numRecipients
          
    def subscribeToTopic(self, topicName, deliveryCallback=None, context=None):
        '''
        For convenience, a deliveryCallback function may be passed,
        saving a subsequent call to addTopicListener(). See addTopicListener()
        for details.
        
        If deliveryCallback is absent or None, then method _deliverResult()
        in this class will be used. That method is intended to be a 
        placeholder with no side effects.
        
        The context is any object or structure that is
        meaningful to the deliveryFunc. This could be the instance
        of an application on which the deliveryFunc can call
        methods, or an Event object that the deliveryFunc will set()        
        
        It is a no-op to call this method multiple times for the
        same topic.
                 
        :param topicName: official name of topic to listen for.
        :type topicName: string
        :param deliveryCallback: a function that takes two args: a topic
            name, and a topic theContent string.
        :type deliveryCallback: function
        :param context: any stucture that is meaningful to the callback function
        :type context: <any>
        '''
        
        if deliveryCallback is None:
            deliveryCallback = self.resultCallback
            
        if type(deliveryCallback) != types.FunctionType and type(deliveryCallback) != functools.partial:
            raise ValueError("Parameter deliveryCallback must be a function, was of type %s" % type(deliveryCallback))

        msgQueueForTopic = Queue.Queue()
        
        # Spin off a thread that will run the delivery function;
        # that function is expected to take a delivery queue:
        
        deliveryThread = DeliveryThread(topicName, msgQueueForTopic, deliveryCallback, context)
        self.topicThreads[topicName] = deliveryThread

        # All incoming messages on this topic will be
        # delivered through the msgQueueForTopic to the
        # delivery thread, which will read from this
        # queue:
        
        self.topicWaiterThread.addTopic(topicName, msgQueueForTopic)
        deliveryThread.start()


    def unsubscribeFromTopic(self, topicName=None, block=True):
        '''
        Unsubscribes from topic. Stops the topic's thread,
        and removes it from bookkeeping so that the Thread object
        will be garbage collected.
        
        Passing None for topicName unsubscribes from all topics.
        
        Calling this method for a topic that is already
        unsubscribed is a no-op.
        
        :param topicName: name of topic to subscribe from
        :type topicName: {string | None}
        :param block: if true, call blocks until Redis server has acknowledged
            completion of the unsubscribe action.
        :type block: bool
        '''

        # Tell the TopicWaiter to go deaf on the topic:
        self.topicWaiterThread.removeTopic(topicName, block=block)

        if topicName is None:
            # Kill all topic threads:
            for deliveryThread in self.topicThreads.values():
                deliveryThread.stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                #*********deliveryThread.join()
            self.topicThreads = {}
        else:
            try:
                self.topicThreads[topicName].stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                self.topicThreads[topicName].join()
                
                del self.topicThreads[topicName]
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
        response.
        
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
                                topicName=incomingBusMsg.responseTopic)
        return respBusMsg
        
        
    def mySubscriptions(self):
        '''
        Return a list of topic names to which this bus adapter is subscribed.
        
        :return: List of topics to which caller is subscribed.
        :rtype: [String]
        '''
        #return self.pubsub.channels.keys()
        return self.topicWaiterThread.topics()
        
    def close(self):
        for subscription in self.mySubscriptions():
            self.unsubscribeFromTopic(subscription)
            
        self.topicWaiterThread.stop()
        # Wait up to 3sec for the TopicWaiter thread to wind down:
        self.topicWaiterThread.join(3)
        
        # Stop all the delivery threads:
        for topicThread in self.topicThreads.values():
            topicThread.stop()
            # Wait just a bit longer than the periodic
            # 'check whether to stop' timeout of the
            # delivery threads' queue hanging: 
            topicThread.join()

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
        self.setDaemon(True)
        
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
            self.deliveryFunc(busMsg, self.context)


if __name__ == '__main__':
    
    schoolBus = BusAdapter()
    schoolBus.playWithRedis()