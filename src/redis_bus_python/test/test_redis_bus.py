'''
Created on Jul 7, 2015

@author: paepcke
'''
import Queue
from threading import Thread
import threading
import unittest

import redis

from redis_bus_python.redis_bus import BusAdapter


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


    def testRoundTripPubSub(self):
        self.bus.subscribeToTopic('myTopic', myCallback)
        Publisher('Unittest roundtrip', 'myTopic').start()
        try:
            roundTripMsg = RedisBusTest.msgQueue.get(RedisBusTest.DO_BLOCK, RedisBusTest.ACTION_TIMEOUT)
        except Queue.Empty:
            raise ValueError("Timeout in round trip test (%d seconds)." % RedisBusTest.ACTION_TIMEOUT)
        self.assertEqual(roundTripMsg.topicName(), 'myTopic')
        self.assertEqual(roundTripMsg.content(), 'Unittest roundtrip')

class Publisher(threading.Thread):
    
    def __init__(self, msg, topicName):
        threading.Thread.__init__(self)
        self.msg = msg
        self.topicName = topicName
        self.rserver = redis.StrictRedis()
        
    def run(self):
        self.rserver.publish(self.topicName, self.msg)
        
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()