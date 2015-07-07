'''
Created on May 19, 2015

@author: paepcke
'''
import Queue
import functools
import threading


class _TopicWaiter(threading.Thread):
    '''
    classdocs
    '''

    DO_BLOCK = True

    # Time interval at which thread will stop waiting
    # for bus messages, and check whether an external
    # caller has called the stop() method:
    
    TOPIC_WAITER_STOPPED_CHECK_INTERVAL = 5 # seconds

    def __init__(self, busAdapter):
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
        
        self.deliveryCallbacks = {}

        # Optionally, each topic may have a threading.Event objects 
        # associated with it that will be raised when a message 
        # of that topic arrives:
        self.eventsToSet = {}
        
        # Function (rather than method) to use as callback when
        # subscribing to the underlying Redis system:
        self.allTopicsDeliveryFunc = functools.partial(self.busMsgArrived, self)
        
        
        # Use the recommended way of stopping a thread:
        # Set a variable that the thread checks periodically:
        self.done = False
        
    def addTopic(self, topicName, callbackFunc):
        '''
        Add a topic to listen to. If the topic already
        exists, then the callbackFunc will be appended to
        the already existing callbacks.
        
        :param topicName: name of topic
        :type topicName: String
        :param callbackFunc: function taking a topic and BusMessage as arguments
        :type callbackFunc: <function(String, BusMessage>
        '''
        
        if self.deliveryCallbacks.has_key(topicName):
            self.addListener(topicName, callbackFunc)
        else:
            self.deliveryCallbacks[topicName] = [callbackFunc]
        
    def removeTopic(self, topicName):
        
        try:
            del self.deliveryCallbacks[topicName]
        except KeyError:
            pass
        
    def addListener(self, topicName, callbackFunc):
        '''
        Add a listener who will be notified with any
        message that arrives on the given topic. See :func:`addTopicListener` 
        in :class:`BusAdapter` for details on parameters.
        If the topic does not already exist, it will be
        added, with the given callback as the (so far) only
        listener.
        
        :param topicName: name of topic to which callback is to be added.
        :type topicName: String
        :param callbackFunc: function with two args: a topic name, and
            a string that is the message content.
        :type callbackFunc: function 
        '''

        try:
            currCallbacks = self.deliveryCallbacks[topicName]
            currCallbacks.append(callbackFunc)
        except KeyError:
            self.addTopic(topicName, callbackFunc)

    def removeListener(self, topicName, callbackFunc):
        '''
        Remove the specified function from the callbacks to
        notify upon message arrivals. It is a no-op to
        remove a non-existing listener.
        
        :param topicName: topic from which the given callback is to be removed.
        :type topicName: String
        :param callbackFunc: callback function to remove. 
        :type callbackFunc: Function
        '''

        try:
            self.deliveryCallbacks[topicName].remove(callbackFunc)
        except (KeyError, ValueError):
            # This callback func wasn't registered
            # in the first place, or topic isnt' subscribed to:
            return

    def topics(self):
        '''
        Return all topics we are subscribed to
        
        :return: list of topics to which subscriptions have been established.
        :rtype: (String)
        '''

        return self.deliveryCallbacks.keys()

    def listeners(self, topicName):
        '''
        Return all the callback functions that will be called
        each time a message arrives on the given topic.
        
        :param topicName: topic from which the given callback is to be removed.
        :type topicName: String
        :return: list of registered callback functions.
        '''
        
        try:
            return self.deliveryCallbacks[topicName]
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

    def busMsgArrived(self, busMsg):
        '''
        Callback used for all topics that are subscribed to.
        This is what the Redis client will call for all topics.
        
        :param busMsg:
        :type busMsg:
        '''
        
        # Push the msg into a thread-safe queue;
        # that will wake the loop in run():
        self.msgQueue.put(busMsg)
    
    def run(self):
        '''
        Hang on Redis message arrival. Whenever a message arrives,
        set() a possibly existing :class:`threading.Event` object.
        Call all the registered delivery functions in turn.
        
        Periodically check whether self.done is True, indicating that
        thread should stop.
        '''
        
        self.busModule.setTopicWaiterLive(True)
        try:
            while not self.done:
                
                # Hang for a msg to arrive:
                try:
                    busMsg = self.msgQueue.get(_TopicWaiter.DO_BLOCK, _TopicWaiter.TOPIC_WAITER_STOPPED_CHECK_INTERVAL)
                except Queue.Empty:
                    # Timeout; check whether someone called
                    # the stop() method while we were hanging: 
                    if self.done:
                        break
                    else:
                        continue

                topic   = busMsg['channel']
                content = busMsg['data']
                try:
                    deliveryCallbacks = self.deliveryCallbacks[topic]
                except KeyError:
                    # Received message to which we were not subscribed;
                    # should not happen:
                    raise RuntimeError("Received message on topic '%s' to which no subscription exists: %s" % (topic, str(busMsg)))
                
                for deliveryFunc in deliveryCallbacks:
                    deliveryFunc(topic, content)
                    # Was the stop() method called?
                    if self.done:
                        break
                try:
                    event = self.eventsToSet[topic]
                    event.set()
                except KeyError:
                    pass
        finally:
            self.busModule.setTopicWaiterLive(False)
        
    def stop(self):
        self.done = True