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
import traceback

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
        self.interruptEvent = threading.Event()
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

            #************
            #print('Echoed one.')
            #************

            if self.numEchoed % 1000 == 0:
                print('Echoed %d' % self.numEchoed)
            
    def resetEchoedCounter(self):
        currTime = time.time()
        
        if self.mostRecentRxTime is None:
            #**********
            #printThreadTraces()
            #sys.exit()
            #**********
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
            print('Resetting (echoed %d)' % self.numEchoed)
            self.printedResetting = True
        self.numEchoed = 0
        self.startTime = time.time()
        self.timer = self.startIdleTimer()

    def startIdleTimer(self):
        threading.Timer(PerformanceTesterEchoServer.MAX_IDLE_TIME, functools.partial(self.resetEchoedCounter)).start()

    def stop(self, signum=None, frame=None):
        self.interruptEvent.set()
        
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
        self.interruptEvent.wait()
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
    echoServer = PerformanceTesterEchoServer(beSynchronous=True)
    signal.signal(signal.SIGINT, functools.partial(echoServer.stop))
    print("Starting to echo msgs on topic 'test'; cnt-C to stop...")
    echoServer.start()
    signal.pause()
    echoServer.join()
