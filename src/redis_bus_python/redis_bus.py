'''
Created on Jul 6, 2015

@author: paepcke

TODO:
   o documentation:
        - sync responses now returned on a temporary topic 
             whose name is the incoming msg's ID number

   o TopicWaiter: maybe take out the event facility (e.g. removeTopicEvent())
'''

import Queue
from datetime import datetime
import functools
import json
import threading
import types
import uuid

from bus_message import BusMessage
from redis_bus_python.topic_waiter import _TopicWaiter
from schoolbus_exceptions import SyncCallTimedOut


class BusAdapter(object):
    '''
    classdocs
    '''
    
    # Time interval after which delivery threads will 
    # temporarily stop looking at their delivery queue
    # to check whether someone called stop() on the thread.

    _DELIVERY_THREAD_PAUSE_INTERVAL = 2 # seconds
    
    _DO_BLOCK = True

    _LEGAL_MSG_TYPES = ['req', 'resp']
    _LEGAL_STATUS    = ['OK', 'ERROR']
    
    def __init__(self, host='localhost', port=6379, db=0):
        '''
        Constructor
        '''

        self.resultDeliveryFunc = functools.partial(self._awaitSynchronousReturn)
        self.topicThreads = {}
        self.topicWaiterThread = _TopicWaiter(self, host=host, port=port, db=db)
        self.topicWaiterThread.setDaemon(True)
        self.topicWaiterThread.start()
        
    def publish(self, busMessage, topicName=None, sync=False, msgId=None, msgType='req', timeout=None, auth=None):
        '''
        Main method for publishing a message. For publishing a response
        to a synchronous call, see publishResponse().
        
        :param busMessage: either a string, or a BusMessage object. If a string,
            that string will be the outgoing message's content field value. If
            a BusMessage object, that object's content property will be the outgoing
            message's content field value.
        :type busMessage: {String | BusMessage}
        :param topicName: name of topic to publish to. If busMessage is a BusMessage
            object, then a non-None passed-in topicName takes precedence over the 
            busMessage's topicName. Either the passed-in topicName, or its value
            in the busMessage must be non-None.
        :type topicName: {String | None}
        :param sync: if true, the outgoing message is published. This method will
            then not return until either a return message is received with the 
            outgoing message's id, but type 'resp', or a timeout occurs.
        :type sync: bool
        :param msgId: id field value of the outgoing message. If this passed-in value
            is None, and a possibly passed-in BusMessage is None as well, then a new
            UUID is created and used.
        :type msgId: {String | None}
        :param msgType: one of the BusAdapter._LEGAL_MSG_TYPES indicating the message
            type.
        :type msgType: String
        :param timeout: used only if sync == True. Amount of time to wait for
            callee to return a value. If None, wait forever. Value is in (fractional) seconds.
        :type timeout: {float | None}
        :param auth: any authentication to be included in the bus message.
        :type auth: String
        '''
        
        if not isinstance(busMessage, BusMessage):
            # We were passed a raw string to send. The topic name
            # to publish to better be given:
            if topicName is None:
                raise ValueError('Attempt to publish a string without specifying a topic name.')
            msg = busMessage
            msgUuid = self._createUuid()
        else:
            # the busMessage parm is a BusMessage instance, which
            # may contain several of the values for this method's
            # parameters. The parameters take precedence:
            
            topicName = topicName if topicName is not None else busMessage.topicName
            if topicName is None:
                raise ValueError('Attempt to publish a BusMessage instance that does not hold a topic name: %s' % str(busMessage))
            try:
                msgUuid   = msgId if msgId is not None else busMessage.id
            except AttributeError:
                msgUuid = self._createUuid()
                
            try:
                msgType   = msgType if msgType is not None else busMessage.type
            except AttributeError:
                msgType = 'req'
            
            # Get the serialized, UTF-8 encoded message from the BusMessage:
            msg = busMessage.content
            
        # Now msg contains the msg text.

        # Sanity check on message type:
        if msgType not in BusAdapter._LEGAL_MSG_TYPES:
            raise ValueError('Legal message types are %s' % str(BusAdapter._LEGAL_MSG_TYPES))
        
        # Sanity check on timeout:
        if timeout is not None and type(timeout) != int and type(timeout) != float:
            raise ValueError('Timeout must be None, an int, of a float; was %s' % str(timeout))
        
        # Create a JSON struct:
        msgDict = dict(zip(['id', 'type', 'time', 'content'],
                           [msgUuid, msgType, datetime.now().isoformat(), msg]))

        # If synchronous operation requested, wait for response:
        if sync:
            
            # Before publishing the request, must prepare for 
            # a function that will be invoked with the result.
            
            try:
                # Queue through which the thread that waits for
                # the result will deliver the result once it arrives:
                resultDeliveryQueue = Queue.Queue()
                
                # Create a temporary topic for use just
                # for the return result; we simply use the
                # UUID of the outgoing-message-to-be:
                self.subscribeToTopic(msgUuid, self.resultDeliveryFunc, resultDeliveryQueue)
            
                # Finally: post the request...; provide the result queue
                # to the waiting handler as context for it to know where
                # to put the result value:
                self.topicWaiterThread.rserver.publish(topicName, json.dumps(msgDict))
                
                # And wait for the result:
                try:
                    res = resultDeliveryQueue.get(BusAdapter._DO_BLOCK, timeout)
                except Queue.Empty:
                    raise SyncCallTimedOut("Synchronous publish to %s timed out before result was returned." % topicName)
                    
                return res
            
            finally:
                # Make sure the temporary topic used for returning the
                # result is always left without a subscription so that
                # Redis will destroy it:
                self.unsubscribeFromTopic(msgUuid)
            
        else:
            # Not a synchronous call; just publish the request:
            self.topicWaiterThread.rserver.publish(topicName, json.dumps(msgDict))
        
    def publishResponse(self, incomingBusMessage, responseContent, timeout=None, auth=None):
        '''
        Convenience method for responding to a synchronous message.
        The handler that received the incoming message can call this
        method with the original incoming message, and a response
        content. This method will publish a message that will properly
        be recognized by the original requestor as a result to a
        sync call. 
        
        :param incomingBusMessage: the BusMessage object that deliverd the request to the handler
            from which a response is returned via calling this method.
        :type incomingBusMessage: BusMessage
        :param responseContent: result of the handler's work; this value will be entered
            into the content field of the return message.
        :type responseContent: <any>
        :param timeout: 
        :type timeout:
        :param auth:
        :type auth:
        '''

        # Responses use as the topic the UUID of the request message:
        respBusMsg = BusMessage(content=responseContent, topicName=incomingBusMessage.id, type='resp')
        self.publish(respBusMsg, timeout=timeout, auth=auth)
    
        
    def subscribeToTopic(self, topicName, deliveryCallback=None, context=None, serializeDelivery=False):
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
        :param serializeDelivery: if True, the deliveryCallback will ******
        :type serializeDelivery: bool
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
        
        self.topicWaiterThread.addTopic(topicName, msgQueueForTopic)
        deliveryThread.setDaemon(True)
        deliveryThread.start()


    def unsubscribeFromTopic(self, topicName=None):
        '''
        Unsubscribes from topic. Stops the topic's thread,
        and removes it from bookkeeping so that the Thread object
        will be garbage collected.
        
        Passing None for topicName unsubscribes from all topics.
        
        Calling this method for a topic that is already
        unsubscribed is a no-op.
        
        :param topicName: name of topic to subscribe from
        :type topicName: {string | None}
        '''

        # Tell the TopicWaiter to go deaf on the topic:
        self.topicWaiterThread.removeTopic(topicName)

        if topicName is None:
            # Kill all topic threads:
            for deliveryThread in self.topicThreads.values():
                deliveryThread.stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                deliveryThread.join(BusAdapter._DELIVERY_THREAD_PAUSE_INTERVAL + 0.5)
            self.topicThreads = {}
        else:
            try:
                self.topicThreads[topicName].stop()
                # Wait for thread to finish; timeout is a bit more
                # than the 'stop-looking-at-queue' timeout used to check
                # for periodic thread stoppage:
                self.topicThreads[topicName].join(BusAdapter._DELIVERY_THREAD_PAUSE_INTERVAL + 0.5)
                
                del self.topicThreads[topicName]
            except KeyError:
                pass
        
#     def waitForMsg(self, topicName, timeout=None):
#         '''
#         Hang efficiently for arrival of a message of
#         a particular topic. Return is not the message,
#         if one was received, but just True/False. Any
#         message arrival will already have called the associated
#         callback(s). 
#         
#         :param topicName: topic to wait on
#         :type topicName: String
#         :param timeout: max time to wait; if None, wait forever
#         :type timeout: float
#         :return: True if a message was received within the timeout, else False
#         :rtype: bool
#         '''
#         
#         try:
#             event = self.topicEvents[topicName]
#         except KeyError:
#             # No event was created for this topic. The
#             # semi-legitimate case of such a mishap is that
#             # the caller never subscribed to the associated
#             # topic. The bad case is that subscription occurred,
#             # but we didn't create/save an associated event
#             # object:
#             if self.topicWaiterThread.subscribedTo(topicName):
#                 raise RuntimeError("We are subscribed to topic '%s,' but no event exists (should not happen, call someone)." % topicName)
#             else:
#                 raise NameError("Not subscribed to topic '%s,' so cannot listen to messages on that topic." % topicName)
# 
#         return event.wait(timeout)
        
    
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
            topicThread.join(BusAdapter._DELIVERY_THREAD_PAUSE_INTERVAL + 0.5)

# --------------------------  Private Methods ---------------------
          
    def _createUuid(self):
        return str(uuid.uuid4())
            
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
    
        :param topicName:
        :type topicName:
        :param deliveryQueue:
        :type deliveryQueue:
        :param deliveryFunc:
        :type deliveryFunc:
        :param context:
        :type context:
        '''
        threading.Thread.__init__(self)
        self.deliveryQueue = deliveryQueue
        self.deliveryFunc  = deliveryFunc
        self.context       = context
        self.done = False
        
    def stop(self):
        self.done = True
        
    def run(self):
        
        while not self.done:
            try:
                busMsg = self.deliveryQueue.get(DeliveryThread.DO_BLOCK, BusAdapter._DELIVERY_THREAD_PAUSE_INTERVAL)
            except Queue.Empty:
                # Planned pause in hanging on queue:
                # if anyone called stop(), then we'll
                # fall out of the loop:
                continue
            self.deliveryFunc(busMsg, self.context)


if __name__ == '__main__':
    
    schoolBus = BusAdapter()
    schoolBus.playWithRedis()