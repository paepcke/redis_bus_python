'''
Created on Jul 6, 2015

@author: paepcke
'''

from datetime import datetime
import functools
import json
import threading
import types
import uuid

from bus_message import BusMessage
from redis_bus_python.topic_waiter import _TopicWaiter
from schoolbus_exceptions import SyncCallTimedOut, SyncCallRuntimeError


class BusAdapter(object):
    '''
    classdocs
    '''
    _LEGAL_MSG_TYPES = ['req', 'resp']
    _LEGAL_STATUS    = ['OK', 'ERROR']

    def __init__(self, host='localhost', port=6379, db=0):
        '''
        Constructor
        '''
        
        # Place to save threading.Event objects
        # that will allow the _TopicWaiter thread
        # to raise a topic-specific event for which 
        # clients of this class can wait:
        self.topicEvents = {}
        self.topicWaiterThread = _TopicWaiter(self, host=host, port=port, db=db)
        self.topicWaiterThread.start()
        
    def publish(self, busMessage, topicName=None, sync=False, msgId=None, msgType='req', timeout=None, auth=None):
        
        if not isinstance(busMessage, BusMessage):
            # We were passed a raw string to send. The topic name
            # to publish to better be given:
            if topicName is None:
                raise ValueError('Attempt to publish a string without specifying a topic name.')
            msg = busMessage
        else:
            # the busMessage parm is a BusMessage instance:
            # If topicName was given, it overrides any topic name
            # associated with the BusObject; else:
            if topicName is None:
                # Grab topic name from the BusMessage:
                topicName = busMessage.topicName()
                # If the BusMessage did not include a topic name: error
                if topicName is None:
                    raise ValueError('Attempt to publish a BusMessage instance that does not hold a topic name: %s' % str(busMessage))
            # Get the serialized, UTF-8 encoded message from the BusMessage:
            msg = busMessage.content()
            
        # Now msg contains the msg text.

        # Create a JSON struct:
        if msgId is None:
            msgUuid = str(uuid.uuid4())
        else:
            msgUuid = msgId
        # Sanity check on message type:
        if msgType not in BusAdapter._LEGAL_MSG_TYPES:
            raise ValueError('Legal message types are %s' % str(BusAdapter._LEGAL_MSG_TYPES))
        
        msgDict = dict(zip(['id', 'type', 'time', 'content'],
                           [msgUuid, msgType, datetime.now().isoformat(), msg]))

        # If synchronous operation requested, wait for response:
        if sync:
            
            # Before publishing the request, must prepare for 
            # a function that will be invoked with the result.
            
            # Use instance vars for communication with the result 
            # delivery thread.
            # Use of these instance vars means that publish
            # isn't re-entrant. Fine for now:

            # For the result delivery method to know which msg id
            # we are waiting for:            
            self.uuidToWaitFor   = msgUuid
            
            # For the result delivery method to know which topic
            # we are waiting for:
            self.topicToWaitFor  = topicName

            # For the result delivery method to put a string
            # if an error occurs while processing the result
            # bus message:

            self.syncResultError = None
            
            # Create event that will wake us when result
            # arrived and has been placed in self.resDict:

            self.resultArrivedEvent = threading.Event(timeout)

            # If not subscribed to the topic to which this synchronous
            # call is being published, then subscribe to it temporarily:

            wasSubscribed = topicName in self.mySubscriptions()
            if not wasSubscribed:
                self.subscribeToTopic(topicName, self.syncResultWaiter)
            else:
                self.addTopicListener(topicName, self.syncResultWaiter)
            
            # Finally: post the request...
            self.topicWaiterThread.pubsub.publish(topicName, json.dumps(msgDict))
            
            # ... and wait for the answer message to invoke
            # self._awaitSynchronousReturn():
            resBeforeTimeout = self.resultArrivedEvent.wait(timeout)
            
            # Result arrived, and was placed into
            # self.resDict under the msgUuid. Remove the listener
            # that waited for the result:
            
            self.removeTopicListener(topicName, self.syncResultWaiter)
            
            # If we weren't subscribed to this topic, then
            # restore that condition:

            if not wasSubscribed:
                self.unsubscribeFromTopic(topicName)
            
            # If the 'call' timed out, raise exception:
            if not resBeforeTimeout:
                raise SyncCallTimedOut('Synchronous call on topic %s timed out' % topicName)
            
            # A result arrived from the call:
            res = self.resDict.get(msgUuid, None)
            
            # No longer need the result to be saved:
            try:
                del self.resDict[msgUuid]
            except KeyError:
                pass
            
            # Check whether awaitSynchronousReturn() placed an
            # error message into self.syncResultError:

            if self.syncResultError is not None:
                raise(SyncCallRuntimeError(self.syncResultError)) 
            
            return res
        
        else:
            # Not a synchronous call; just publish the request:
            self.topicWaiterThread.rserver.publish(topicName, json.dumps(msgDict))
        
    def subscribeToTopic(self, topicName, deliveryCallback=None):
        '''
        For convenience, a deliveryCallback function may be passed,
        saving a subsequent call to addTopicListener(). See addTopicListener()
        for details.
        
        If deliveryCallback is absent or None, then method _deliverResult()
        in this class will be used. That method is intended to be a 
        placeholder with no side effects.
        
        It is a no-op to call this method multiple times for the
        same topic.
                 
        :param topicName: official name of topic to listen for.
        :type topicName: string
        :param deliveryCallback: a function that takes two args: a topic
            name, and a topic content string.
        :type deliveryCallback: function
        '''
        
        if deliveryCallback is None:
            deliveryCallback = self.resultCallback
            
        if type(deliveryCallback) != types.FunctionType and type(deliveryCallback) != functools.partial:
            raise ValueError("Parameter deliveryCallback must be a function, was of type %s" % type(deliveryCallback))

        # Create an event object that the thread will set()
        # whenever a msg arrives, even if no listeners exist:
        event = threading.Event()
        self.topicEvents[topicName] = event
        
        self.topicWaiterThread.addTopic(topicName, deliveryCallback)


    def unsubscribeFromTopic(self, topicName=None):
        '''
        Unsubscribes from topic. Stops the topic's thread,
        and removes it from bookkeeping so that the Thread object
        will be garbage collected. Same for the Event object
        used by the thread to signal message arrival.
        
        Passing None for topicName unsubscribes from all topics.
        
        Calling this method for a topic that is already
        unsubscribed is a no-op.
        
        :param topicName: name of topic to subscribe from
        :type topicName: {string | None}
        '''

        # Delete our record of the Event object used by the thread to
        # indicate message arrivals:
        if topicName is None:
            self.topicEvents = {}
        else:
            try:
                del self.topicEvents[topicName]
            except KeyError:
                pass

        self.topicWaiterThread.removeTopic(topicName)

    def waitForMsg(self, topicName, timeout=None):
        '''
        Hang efficiently for arrival of a message of
        a particular topic. Return is not the message,
        if one was received, but just True/False. Any
        message arrival will already have called the associated
        callback(s). 
        
        :param topicName: topic to wait on
        :type topicName: String
        :param timeout: max time to wait; if None, wait forever
        :type timeout: float
        :return: True if a message was received within the timeout, else False
        :rtype: bool
        '''
        
        try:
            event = self.topicEvents[topicName]
        except KeyError:
            # No event was created for this topic. The
            # semi-legitimate case of such a mishap is that
            # the caller never subscribed to the associated
            # topic. The bad case is that subscription occurred,
            # but we didn't create/save an associated event
            # object:
            if self.topicWaiterThread.subscribedTo(topicName):
                raise RuntimeError("We are subscribed to topic '%s,' but no event exists (should not happen, call someone)." % topicName)
            else:
                raise NameError("Not subscribed to topic '%s,' so cannot listen to messages on that topic." % topicName)

        return event.wait(timeout)
        
    
    def mySubscriptions(self):
        '''
        Return a list of topic names to which this bus adapter is subscribed.
        
        :return: List of topics to which caller is subscribed.
        :rtype: [String]
        '''
        #return self.pubsub.channels.keys()
        if self.getTopicWaiterLive():
            return self.topicWaiterThread.topics()
        else:
            return []
        
    def close(self):
        self.topicWaiterThread.stop()

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


if __name__ == '__main__':
    
    schoolBus = BusAdapter()
    schoolBus.playWithRedis()