'''
Created on Jul 7, 2015

@author: paepcke
'''
import Queue
import functools
import threading
import unittest

import redis

from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.bus_message import BusMessage

TEST_ALL = True

def myCallback(busMsg, context=None):
    RedisBusTest.msgQueue.put_nowait(busMsg)


class RedisBusTest(unittest.TestCase):

    msgQueue = Queue.Queue()
    DO_BLOCK = True
    #ACTION_TIMEOUT = 1 #sec
    ACTION_TIMEOUT = 9 #sec
    
    bus = None

    @classmethod
    def setUpClass(cls):
        cls.bus = BusAdapter()
        
    @classmethod
    def tearDownClass(cls):
        cls.bus.close()

    def setUp(self):
        self.bus = RedisBusTest.bus


    def tearDown(self):
        pass

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")    
    def testRoundTripPlusBasicPubSub(self):
        
        self.bus.subscribeToTopic('myTopic', myCallback)
        try:
            # Start a thread that publishes a msg to the topic
            # we just subscribed to. The msg will be delivered to
            # the myCallback() function, which will place it into
            # a queue. The following try block picks it up from 
            # there; the Publisher thread dies by itself after
            # publishing the msg:
            Publisher('Unittest roundtrip', 'myTopic').start()
            try:
                # Wait for myCallback() to place the received msg
                # into the queue:
                roundTripMsg = RedisBusTest.msgQueue.get(RedisBusTest.DO_BLOCK, RedisBusTest.ACTION_TIMEOUT)
            except Queue.Empty:
                raise ValueError("Timeout in round trip test (%d seconds)." % RedisBusTest.ACTION_TIMEOUT)
            self.assertEqual(roundTripMsg.topicName, 'myTopic')
            self.assertEqual(roundTripMsg.content, 'Unittest roundtrip')
            
            # Test mySubscriptions while we're here:
            self.assertEqual(self.bus.mySubscriptions(), ['myTopic'])
        finally:
            # Test unsubscribe:
            self.bus.unsubscribeFromTopic('myTopic')
            self.assertEqual(len(self.bus.mySubscriptions()), 0)
        
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")
    def testPublishASync(self):
        try:
            # Start a thread that will listen to 'myTopic',
            # and check that the correct value arrives.
            # The argument to the thread instantiation is
            # the value that it should expect:
            receiveCheckService = ReceptionTester(correctValue="10")
            receiveCheckService.start()
            
            # Send msg to be received by that thread:
            self.bus.publish(BusMessage(10, topicName='myTopic'))
        finally:
            receiveCheckService.stop()

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")
    def testPublishSync(self):
        
        try:
            # Start a thread that will listen to 'myTopic',
            # and check that the correct value arrives.
            # The argument to the thread instantiation is
            # the value that it should expect:
            receiveCheckService = ReceptionTester(beSynchronous=True)
            receiveCheckService.start()
            
            # Send msg to be received by that thread:
            msg = BusMessage(10, 'myTopic')
            doubleResult = self.bus.publish(msg, sync=True)
            self.assertEqual(doubleResult, '20')
        finally:
            receiveCheckService.stop()
            
        

# ------------------------------ Support Thread Classes -------------------

#------------------------
# Publisher
#----------------

class Publisher(threading.Thread):
    '''
    Thread that publishes a given message on the given
    topic. Used in unittests as a source for messages.
    '''
    
    def __init__(self, msg, topicName):
        threading.Thread.__init__(self)
        self.msg = msg
        self.topicName = topicName
        self.rserver = redis.StrictRedis()
        
    def run(self):
        self.rserver.publish(self.topicName, self.msg)

#------------------------
# ReceptionTester
#-----------------

class ReceptionTester(threading.Thread):
    '''
    Thread that receives messages, and asserts
    that the received values are what was passed
    into the thread's init method. Keeps listening
    till stop() is called.
    '''
    
    def __init__(self, correctValue=None, beSynchronous=False):
        threading.Thread.__init__(self)
        self.correctValue = correctValue
        self.beSynchronous = beSynchronous
        
        self.testBus = BusAdapter()
        
        # Subscribe, and ensure that context is delivered
        # with each message:
        self.testBus.subscribeToTopic('myTopic', 
                                      deliveryCallback=functools.partial(self.messageReceiver), 
                                      context={'foo' : 10, 'bar' : 'my string'})
        self.eventForStopping = threading.Event()
        self.done = False
        
    def messageReceiver(self, busMsg, context=None):
        '''
        Method that is called with each received message.
        
        :param busMsg: bus message object
        :type busMsg: BusMessage
        :param context: context Python structure, if subscribeToTopic() was
            called with one.
        :type context: <any>
        '''
        
        # Check that the context was delivered:
        assert(context['foo'] == 10)
        assert(context['bar'] == 'my string')
        
        if self.beSynchronous:
            # Publish a response: doubling the int-ified content
            # of the incoming msg:
            response = int(busMsg.content) * 2
            self.testBus.publish(self.testBus.makeResponseMsg(busMsg, response))
        else:
            assert(busMsg.content == self.correctValue)

    
    def stop(self):
        self.eventForStopping.set()
            
    def run(self):
        self.eventForStopping.wait()
        self.testBus.close()
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()