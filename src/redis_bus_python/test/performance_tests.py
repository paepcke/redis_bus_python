#!/usr/bin/env python

'''
Created on Jul 8, 2015

@author: paepcke
'''

import functools
import hashlib
import random
import string
import sys
import threading
import time
import traceback

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.schoolbus_exceptions import SyncCallTimedOut


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
        (msg, md5) = self.createMessage(msgLen) #@UnusedVariable
        busMsg = BusMessage(content=msg, topicName='test')
        startTime = time.time()
        for _ in range(numMsgs):
            self.bus.publish(busMsg)
        endTime = time.time()
        self.printResult('Publishing %s msgs to empty space: ' % str(numMsgs), startTime, endTime, numMsgs)
    
    def publishToSubscribedTopic(self, numMsgs, msgLen):
        '''
        Publish given number of messages to a topic that
        a thread in this same process is subscribed to.
        These are asynchronous publish() calls. The receiving
        thread computes MD5 of the received msg to verify.
        
        :param numMsgs:
        :type numMsgs:
        :param msgLen:
        :type msgLen:
        '''

        (msg, md5) = self.createMessage(msgLen)
        busMsg = BusMessage(content=msg, topicName='test')
        try:
            listenerThread = ReceptionTester(msgMd5=md5, beSynchronous=False)
            listenerThread.setDaemon(True)
            listenerThread.start()
            
            startTime = time.time()
            for _ in range(numMsgs):
                self.bus.publish(busMsg)
            endTime = time.time()
            self.printResult('Publishing %s msgs to a subscribed topic in same process: ' % str(numMsgs), startTime, endTime, numMsgs)
        except Exception:
            raise
        finally:
            listenerThread.stop()
            listenerThread.join(3)
            
    def syncPublishing(self, numMsgs, msgLen):

        (msg, md5) = self.createMessage(msgLen) #@UnusedVariable

        busMsg = BusMessage(content=msg, topicName='test')

        startTime = time.time()
        for _ in range(numMsgs):
            try:
                res = self.bus.publish(busMsg, sync=True, timeout=3600) #@UnusedVariable
            except SyncCallTimedOut:
                #printThreadTraces()
                raise
                
        endTime = time.time()
        self.printResult('Publishing %s msgs to a subscribed topic in same process: ' % str(numMsgs), startTime, endTime, numMsgs)
        
    
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
        if inMd5 != context:
            raise ValueError("md5 in msg should be %s, but was %s" % (inMd5, context))
                
        if self.beSynchronous:
            # Publish a response:
            self.testBus.publish(self.testBus.makeResponseMsg(busMsg), busMsg.content)

    
    def stop(self):
        self.eventForStopping.set()
            
    def run(self):
        self.eventForStopping.wait()
        self.testBus.unsubscribeFromTopic('test')
        self.testBus.close()

#**********
def printThreadTraces():
    sys.stderr, "\n*** STACKTRACE - START ***\n"
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename,
                                                        lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
     
    for line in code:
        print >> sys.stderr, line
    print >> sys.stderr, "\n*** STACKTRACE - END ***\n"   
    
    while True:
        time.sleep(5)
#**********
        
if __name__ == '__main__':
    tester = RedisPerformanceTester()
    # Send 10k msg of 100 bytes each to an unsubscribed topic:
    #****tester.publishToUnsubscribedTopic(10000, 100)
    #****tester.publishToSubscribedTopic(10000,100)
    tester.syncPublishing(10000,100)
    tester.close()
