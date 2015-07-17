#!/usr/bin/env python

'''
Created on Jul 9, 2015

@author: paepcke
'''
import functools
import signal
import sys
import threading
import time

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter


class PerformanceTesterEchoServer(threading.Thread):
    '''
    Thread that receives messages, and asserts
    that the received values are what was passed
    into the thread's init method. Keeps listening
    till stop() is called.
    '''
    
    # Maximum time for no message to arrive before
    # starting over counting messages:
    MAX_IDLE_TIME = 5
    
    def __init__(self, beSynchronous=False):
        threading.Thread.__init__(self)
        self.beSynchronous = beSynchronous
        
        self.testBus = BusAdapter()
        
        # Subscribe, and ensure that context is delivered
        # with each message:
        self.testBus.subscribeToTopic('test', 
                                      deliveryCallback=functools.partial(self.messageReceiver)) 
        self.eventForStopping = threading.Event()
        self.numEchoed = 0
        self.mostRecentRxTime = None
        self.printedResetting = False
        
        signal.signal(signal.SIGINT, functools.partial(self.stop))
        
        self.done = False
        
    def messageReceiver(self, busMsg, context=None):
        '''
        Method that is called with each received message.
        
        :param busMsg: bus message object
        :type busMsg: BusMessage
        :param context: context Python structure, if subscribeToTopic() was
            called with one.uu
        :type context: <any>
        '''
        
        self.mostRecentRxTime = time.time()
        self.printedResetting = False
        
        if self.beSynchronous:
            respMsg = self.testBus.makeResponseMsg(busMsg, busMsg.content)

            # Publish a response:
            self.testBus.publish(respMsg)
            self.numEchoed += 1

            if self.numEchoed % 1000 == 0:
                print('Echoed %d' % self.numEchoed)
            
    def resetEchoedCounter(self):
        currTime = time.time()
        
        if self.mostRecentRxTime is None:
            # Nothing received yet:
            self.startTime = time.time()
            self.startIdleTimer()
            return
        
        if currTime - self.mostRecentRxTime <= PerformanceTesterEchoServer.MAX_IDLE_TIME:
            # Received msgs during more recently than idle time:
            self.startIdleTimer() 
            return
        
        # Did not receive msgs within idle time:
        self.printTiming()
        if not self.printedResetting:
            print('Resetting')
            self.printedResetting = True
        self.numEchoed = 0
        self.startTime = time.time()
        self.timer = self.startIdleTimer()

    def startIdleTimer(self):
        threading.Timer(PerformanceTesterEchoServer.MAX_IDLE_TIME, functools.partial(self.resetEchoedCounter)).start()

    def stop(self):
        self.eventForStopping.set()
        
    def printTiming(self, startTime=None):
        currTime = time.time()
        if startTime is None:
            startTime = self.startTime
            return
        print('Echoed %d messages' % self.numEchoed)
        if self.numEchoed == 0:
            print ('No messages echoed.')
        else:
            timeElapsed = float(currTime) - float(self.startTime)
            print('Msgs per second: %d' % (timeElapsed / self.numEchoed))
            
    def run(self):
        self.startTime = time.time()
        self.startIdleTimer()
        self.eventForStopping.wait()
        self.testBus.unsubscribeFromTopic('test')
        self.testBus.close()

def signal_handler(signal, frame):
        print('Stopping SchoolBus echo service.')
        echoServer.stop()
        sys.exit(0)

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    
    echoServer = PerformanceTesterEchoServer(beSynchronous=True)
    print("Starting to echo msgs on topic 'test'; cnt^C to stop...")
    echoServer.start()
