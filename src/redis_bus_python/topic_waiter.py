'''
Created on May 19, 2015

@author: paepcke
'''
import Queue
import functools
import json
import threading
import uuid

import redis

from redis_bus_python.bus_message import BusMessage


#*************
def tmpCallback(msg):
    print('Msg to pure func: %s' % str(msg))
#*************

class _TopicWaiter(threading.Thread):
    '''
    classdocs
    '''

    DO_BLOCK = True

    # Time interval at which thread will stop waiting
    # for bus messages, and check whether an external
    # caller has called the stop() method:
    
    TOPIC_WAITER_STOPPED_CHECK_INTERVAL = 5 # seconds

    def __init__(self, busAdapter, host='localhost', port=6379, db=0):
        '''
        Initialize list of callback functions. Remember the Event object
        to raise whenever a message arrives.
        
        Assumption: the passed-in parent BusAdapter object contains
        instance variable bootstrapServers, which is initialized to
        an array of strings of the form hostName:port, in which each
        hostName is a Kafka server, and each port is a port on which
        the Kafka server listens. Example: ['myKafkaServer.myplace.org:9092']. 
        
        :param topicName: Kafka topic to listen to
        :type topicName: string
        :param busAdapter: BusAdapter object that created this thread.
        :type busAdapter: BusAdapter
        '''

        threading.Thread.__init__(self)
        self.busModule = busAdapter

        self.msgQueue  = Queue.Queue()
        
        # We maintain a dict of topics, whose
        # values are arrays of callback functions:
        #    'myTopic1' : [myCallback1]
        #    'myTopic2' : [myCallback1, myCallback2]
        
        self.deliveryQueues = {}

        # Optionally, each topic may have a threading.Event objects 
        # associated with it that will be raised when a message 
        # of that topic arrives:
        self.eventsToSet = {}
        
        # Use the recommended way of stopping a thread:
        # Set a variable that the thread checks periodically:
        self.done = False
        
        self.rserver = redis.StrictRedis(host=host, port=port, db=db)
        
        # Create a pubsub instance for all pub/sub needs.
        # The ignore_subscribe_messages=True prevents our message
        # handlers to constantly get called with confirmations of our
        # own publish/subscribe and other commands to the Redis server:
        self.pubsub = self.rserver.pubsub(ignore_subscribe_messages=True)
        
        # Function (rather than method) to use as callback when
        # subscribing to the underlying Redis system:
        self.allTopicsDeliveryFunc = functools.partial(self.busMsgArrived)
        
        # When no topics are subscribed to, the pubsub instance's listen()
        # iterator runs dry, dropping us out of the loop in the run() method.
        # So: a kludge: subscribe to a topic that will never be used:
        self.secretTopic = str(uuid.uuid4())
        self.addTopic(self.secretTopic, self.allTopicsDeliveryFunc)
        
    def addTopic(self, topicName, deliveryQueue):
        '''
        Add a topic to listen to. If the topic already
        exists, then the callbackFunc will be appended to
        the already existing callbacks.
        
        :param topicName: name of topic
        :type topicName: String
        :param deliveryQueue: queue where to add incoming msgs of this topic
        :type deliveryQueue: Queue.Queue
        '''
        
        if self.deliveryQueues.has_key(topicName):
            self.addListener(topicName, deliveryQueue)
        else:
            self.deliveryQueues[topicName] = [deliveryQueue]
        
        self.pubsub.subscribe(**{topicName : self.allTopicsDeliveryFunc})
        
    def removeTopic(self, topicName=None):
        '''
        Stop listening to a topic. If topicName is None,
        stop listening to all topics.
        
        :param topicName: name of topic to stop listening to, or None for stop listening to all.
        :type topicName: {String | None}
        '''
        
        try:
            del self.deliveryQueues[topicName]
        except KeyError:
            pass
        if topicName is not None:
            self.pubsub.unsubscribe(topicName)
        else:
            self.pubsub.unsubscribe()
        
    def addListener(self, topicName, deliveryQueue):
        '''
        Add a queue for a listener that will be fed with any
        message that arrives on the given topic.
        If the topic does not already exist, it will be
        added, with the given queue as the (so far) only
        listener.
        
        :param topicName: name of topic to which callback is to be added.
        :type topicName: String
        :param deliveryQueue: queue to feed with messages to the given topic
        :type deliveryQueue: Queue.Queue 
        '''

        try:
            currQueues = self.deliveryQueues[topicName]
            currQueues.append(deliveryQueue)
        except KeyError:
            self.addTopic(topicName, deliveryQueue)

    def removeListener(self, topicName, deliveryQueue):
        '''
        Remove the specified function from the callbacks to
        notify upon message arrivals. It is a no-op to
        remove a non-existing listener.
        
        :param topicName: topic from which the given callback is to be removed.
        :type topicName: String
        :param deliveryQueue: listener delivery queue to be removed
        :type deliveryQueue: Queue.Queue
        '''

        try:
            self.deliveryQueues[topicName].remove(deliveryQueue)
        except (KeyError, ValueError):
            # This queue func wasn't registered
            # in the first place, or topic isnt' subscribed to:
            return

    def topics(self):
        '''
        Return all topics we are subscribed to.
        
        :return: list of topics to which subscriptions have been established.
        :rtype: (String)
        '''
        
        # We don't want to return the topic we subscribed to
        # just to keep listen() from returning right away:
        trueTopics = self.deliveryQueues.keys()
        trueTopics.remove(self.secretTopic)
        return trueTopics

    def subscribedTo(self, topicName):
        '''
        Return True if currently subscribed to this topic.
        
        :param topicName: topic from that is to be checked
        :type topicName: String
        '''
        
        try:
            self.deliveryQueues[topicName]
            return True
        except KeyError:
            return False


    def listenerQueues(self, topicName):
        '''
        Return all the listener queues that will be fed
        each time a message arrives on the given topic.
        
        :param topicName: topic from which the given callback is to be removed.
        :type topicName: String
        :return: list of registered delivery queues.
        '''
        
        try:
            return self.deliveryQueues[topicName]
        except KeyError:
            return []

    def addTopicEvent(self, topic, eventObj):
        '''
        Add a threading.Event object for a particular topic.
        The event will be raised whenever a message to that6
        topic arrives.
        
        :param topic: topic to notify on 
        :type topic: String
        :param eventObj: event object to set
        :type eventObj: threading.Event
        '''
        self.eventsToSet[topic] = eventObj
    
    def removeTopicEvent(self, topic):
        '''
        Remove a threading.Event object for a particular
        topic.
        
        :param topic: the topic for which an event should no longer exist
        :type topic: String
        '''
        del self.eventsToSet[topic]

    def busMsgArrived(self, rawRedisBusMsg):
        '''
        Callback used for all topics that are subscribed to.
        This is what the Redis client will call for all topics.
        
        :param rawRedisBusMsg: raw message from the Redis system
        :type rawRedisBusMsg: dict
        '''
        # Push the msg into a thread-safe queue;
        # for the message's topic:
        topic   = rawRedisBusMsg['channel']
        content = rawRedisBusMsg['data']
        try:
            # Get list of queues for this topic:
            deliveryQueues = self.deliveryQueues[topic]
        except KeyError:
            # Received message to which we were not subscribed;
            # should not happen:
            raise RuntimeError("Received message on topic '%s' to which no subscription exists: %s" % (topic, str(rawRedisBusMsg)))
        
        # If this is a proper SchoolBus message, the content
        # will look like the following JSON:
        #    {"content": "10", "type": "req", "id": "71d3babb-131e-43ff-943f-e7056714558f", "time": "2015-07-08T09:00:03.112241"}
        # Place these into a BusMessage, making each key an instance variable:
        try:
            busMsg = BusMessage(topicName=topic, moreArgsDict=json.loads(content))
        except (ValueError, TypeError):
            # Not valid JSON: just enter the content into the
            # new BusMessage's content property directly:
            busMsg = BusMessage(content=content, topicName=topic)
        
        for deliveryQueue in deliveryQueues:
            deliveryQueue.put_nowait(busMsg)
            # Was the stop() method called?
            if self.done:
                break

        try:
            event = self.eventsToSet[topic]
            event.set()
        except KeyError:
            pass

    def run(self):
        '''
        Hang on Redis message arrival. Whenever a message arrives,
        set() a possibly existing :class:`threading.Event` object.
        Call all the registered delivery functions in turn.
        
        Periodically check whether self.done is True, indicating that
        thread should stop.
        '''
        while not self.done:
            
            # Hang for a msg to arrive:
            try:
                for _ in self.pubsub.listen():
                    if self.done:
                        break
                    else:
                        continue
            except ValueError:
                pass
        
    def stop(self):
        self.done = True
        # Unsubscribe from all topics, including the kludge one.
        # We call the BusAdapter's unsubscribe() method so
        # that it too gets a chance to clean up. It will
        # in turn call the removeTopic() method of this instance:
        self.busModule.unsubscribeFromTopic()
        
        self.pubsub.close()