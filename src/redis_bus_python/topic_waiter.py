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


class _TopicWaiter(threading.Thread):
    '''
    Class that listens to Redis messages arriving on all topics.
    Clients of this class's singleton instance can register 
    queues onto which the instance will place arriving messages.
    The method _busMsgArrived() is called by the underlying
    redis-py library with any incoming raw Redis message.
    The method will place the message into a BusMessage instance,
    and place that BusMessage onto all queues that are 
    registered for the incoming message's topic. 
    '''

    DO_BLOCK = True

    # Time interval at which thread will stop waiting
    # for bus messages, and check whether an external
    # caller has called the stop() method:
    
    TOPIC_WAITER_STOPPED_CHECK_INTERVAL = 5 # seconds

    def __init__(self, busAdapter, host='localhost', port=6379, db=0):
        '''
        Initialize list queues on which callback functions are waiting
        for incoming messages.
        
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
        self.allTopicsDeliveryFunc = functools.partial(self._busMsgArrived)
        
        # When no topics are subscribed to, the pubsub instance's listen()
        # iterator runs dry, dropping us out of the loop in the run() method.
        # So: a kludge: subscribe to a topic that will never be used:
        self.secretTopic = str(uuid.uuid4())
        self.addTopic(self.secretTopic, self.allTopicsDeliveryFunc)
        
    def addTopic(self, topicName, deliveryQueue):
        '''
        Add a topic to listen to. This means: subscribe
        to the topic in the underlying redis-py library, and
        remember the given queue as associated with that topic.
        If the topic is already subscribed to, then the given 
        delivery queue will be appended to the already existing 
        queue(s).
        
        :param topicName: name of topic
        :type topicName: String
        :param deliveryQueue: queue where to add incoming msgs of this topic
        :type deliveryQueue: Queue.Queue
        '''
        
        try:
            # Are we already subscribed to this topic?
            currQueues = self.deliveryQueues[topicName]
            # If so, add the given queue to the existing ones
            # to be fed whenever a msg arrives on this topic:
            currQueues.append(deliveryQueue)
        except KeyError:
            # Topic has not been subscribed to before:
            self.deliveryQueues[topicName] = [deliveryQueue]
            # Subscribe to the topic, specifying this class's
            # reception function as handler:
            self.pubsub.subscribe(**{topicName : self.allTopicsDeliveryFunc})
        
    def removeTopic(self, topicName=None):
        '''
        Stop listening to a topic. If topicName is None,
        stop listening to all topics. See :meth:`topic_waiter._TopicWaiter.removeListener`
        to remove just one queue from the topic.
        
        :param topicName: name of topic to stop listening to, or None to 
            stop listening to any topic.
        :type topicName: {String | None}
        '''
        
        # Unsubscribe in the underlying redis-py library: 
        if topicName is not None:
            self.pubsub.unsubscribe(topicName)
        else:
            self.pubsub.unsubscribe()
            
        try:
            # Remove the delivery queue(s) associated
            # with this topic:
            del self.deliveryQueues[topicName]
        except KeyError:
            pass
        

    def removeListener(self, topicName, deliveryQueue):
        '''
        Remove the specified delivery queue from the queues
        to feed arriving message on the given topic. It is a no-op to
        remove a non-existing listener.
        
        :param topicName: topic from which the given queue is to be removed.
        :type topicName: String
        :param deliveryQueue: listener delivery queue to be removed
        :type deliveryQueue: Queue.Queue
        '''

        try:
            self.deliveryQueues[topicName].remove(deliveryQueue)
        except (KeyError, ValueError):
            # This queue wasn't registered in the first place, 
            # or topic wasn't ever subscribed to:
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
        
    # ----------------------- Private Methods ---------------------- 

    def _busMsgArrived(self, rawRedisBusMsg):
        '''
        Callback used for all topics that are subscribed to.
        This is what the Redis client will call for all topics.
        
        :param rawRedisBusMsg: raw message from the Redis system
        :type rawRedisBusMsg: dict
        '''
        # Push the msg into a thread-safe queue;
        # for the message's topic:
        topic   = rawRedisBusMsg['channel']
        # Entire message text:
        totalContent = rawRedisBusMsg['data']
        try:
            # Get list of queues for this topic:
            deliveryQueues = self.deliveryQueues[topic]
        except KeyError:
            # Received message to which we were not subscribed.
            # This can happen when a message was delivered, b/c
            # we used to be subscribed, but we unsubscribed before
            # this method was called (race condition). Harmless:
            return
            #raise RuntimeError("Received message on topic '%s' to which no subscription exists: %s" % (topic, str(rawRedisBusMsg)))
        
        # If this is a proper SchoolBus message, the content
        # will look like the following JSON:
        #    {"id": "71d3babb-131e-43ff-943f-e7056714558f", "content": "10",  "time": "1436571099"}
        # Place these into a BusMessage, making each key an instance variable:
        try:
            busMsg = BusMessage(topicName=topic, moreArgsDict=json.loads(totalContent))
        except (ValueError, TypeError):
            # Not valid JSON: just enter the content into the
            # new BusMessage's content property directly:
            busMsg = BusMessage(content=totalContent, topicName=topic)
        else:
            # Bus message was at least a parsable JSON message. If
            # it was also a proper SchoolBus message, then the busMsg 
            # will have an 'id' field that's the message ID. By convention,
            # this id is also used as the basis for the topic name on
            # which any synchronous response to this message is to be
            # returned:
            try:
                busMsg.responseTopic = self.busModule.getResponseTopic(busMsg)
            except AttributeError:
                busMsg.responseTopic = None

        # Finally, place the message on all the queues
        # on which handlers are waiting:        
        for deliveryQueue in deliveryQueues:
            deliveryQueue.put_nowait(busMsg)
            # Was the stop() method called?
            if self.done:
                break

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
            except AttributeError:
                # When a message arrives after
                # we unsubscribed to a topic, b/c it
                # was already delivered by the server,
                # and was being processed in redis-py's
                # client.py, then listen() will throw
                # an AttributeError when trying to read
                # on a connection that was closed as
                # part of the unsubscribe. We just check
                # whether stop() was called on this thread,
                # and re-enter the loop:
                if self.done:
                    break
                continue
        
    def stop(self):
        self.done = True
        # Unsubscribe from all topics, including the kludge one.
        # We call the BusAdapter's unsubscribe() method so
        # that it too gets a chance to clean up. It will
        # in turn call the removeTopic() method of this instance:
        self.busModule.unsubscribeFromTopic()
        
        # Shut down the pubsub subsystem; this *should*
        # release the run() loop from its listen() call,
        # but it doesn't seem to...
        #
        # The underlying redis-py closes connections to the
        # Redis server when doing the unsubscribes above, and then
        # gets confused when pubsub.close() gets called: it
        # calls close() on a variable that used to hold a socket,
        # but is now None: 
        try:
            self.pubsub.close()
        except AttributeError:
            pass