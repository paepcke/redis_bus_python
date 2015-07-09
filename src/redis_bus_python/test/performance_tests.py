#!/usr/bin/env python

'''
Created on Jul 8, 2015

@author: paepcke
'''

import functools
import hashlib
import random
import string
import threading
import time

from redis_bus_python.redis_bus import BusAdapter


class RedisPerformanceTester(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.letterChoice = string.letters
        self.bus = BusAdapter()
        
    def close(self):
        self.bus.close()
    
    def publishToUnsubscribedTopic(self, numMsgs, msgLen):
        '''
        Publish given number of messages to a topic that
        nobody is subscribed to. Each msg is of length msgLen.
        
        :param numMsgs:
        :type numMsgs:
        :param msgLen:
        :type msgLen:
        '''
        (msg, md5) = self.createMessage(msgLen)
        startTime = time.time()
        for _ in range(numMsgs):
            self.bus.publish(msg, topicName='test')
        endTime = time.time()
        self.printResult('Publishing %s msgs to empty space: ' % str(numMsgs), startTime, endTime, numMsgs)
    
    def publishToSubscribedTopic(self, numMsgs, msgLen):
        '''
        Publish given number of messages to a topic that
        a thread in this same process is subscribed to.
        These are asynchronous publish() calls.
        
        :param numMsgs:
        :type numMsgs:
        :param msgLen:
        :type msgLen:
        '''

        (msg, md5) = self.createMessage(msgLen)
        try:
            listenerThread = ReceptionTester(msgMd5=md5, beSynchronous=False)
            listenerThread.start()
            
            startTime = time.time()
            for _ in range(numMsgs):
                self.bus.publish(msg, topicName='test')
            endTime = time.time()
            self.printResult('Publishing %s msgs to a subscribed topic in same process: ' % str(numMsgs), startTime, endTime, numMsgs)
        finally:
            listenerThread.stop()
    
    def createMessage(self, msgLen):
        '''
        Returns a string of a given length, and its checksum
        
        :param msgLen: desired str length
        :type msgLen: int
        '''
        
        msg = bytearray()
        for _ in range(msgLen):
            msg.append(random.choice(self.letterChoice))
        return (str(msg), hashlib.md5(str(msg)).hexdigest())
    
    def printResult(self, headerMsg, startTime, endTime, numMsgs):
        totalTime  = endTime - startTime
        timePerMsg = totalTime/numMsgs
        msgPerSec  = numMsgs/totalTime
        print(headerMsg)
        print('msgsPerSec: %s' % str(msgPerSec))
        print('timePerMsg: %s' % str(timePerMsg))

# ---------------------------  Reception Thread ---------------

class ReceptionTester(threading.Thread):
    '''
    Thread that receives messages, and asserts
    that the received values are what was passed
    into the thread's init method. Keeps listening
    till stop() is called.
    '''
    
    def __init__(self, msgMd5=None, beSynchronous=False):
        threading.Thread.__init__(self)
        self.beSynchronous = beSynchronous
        
        self.testBus = BusAdapter()
        
        # Subscribe, and ensure that context is delivered
        # with each message:
        self.testBus.subscribeToTopic('test', 
                                      deliveryCallback=functools.partial(self.messageReceiver), 
                                      context=msgMd5)
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
        
        inMd5 = hashlib.md5(str(busMsg.content)).hexdigest()
        # Check that the context was delivered:
        assert(inMd5 == context)
                
        if self.beSynchronous:
            # Publish a response:
            self.testBus.publishResponse(busMsg, busMsg.content)

    
    def stop(self):
        self.eventForStopping.set()
            
    def run(self):
        self.eventForStopping.wait()
        self.testBus.unsubscribeFromTopic('test')
        self.testBus.close()

        
if __name__ == '__main__':
    tester = RedisPerformanceTester()
    # Send 10k msg of 100 bytes each to an unsubscribed topic:
    #****tester.publishToUnsubscribedTopic(10000, 100)
    tester.publishToSubscribedTopic(100,100)
    tester.close()
    
        
        