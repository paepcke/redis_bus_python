'''
Created on Jul 27, 2015

@author: paepcke
'''
import functools
import signal
import threading
import time

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.test.performance_test_echo_server import \
    PerformanceTesterEchoServer

# Topic on which echo server listens:
ECHO_CHANNEL = 'echo'

class OnDemandPublisher(threading.Thread):
    '''
    Server thread for testing Redis bus. Serves two
    functions: echoes messages it receives on topic
    ECHO_CHANNEL, or sends a single 
    message to a given topic if its send_message()
    method is called.
    
    The echo function acts like a synch-call service
    should: echoing on the response topic, which it
    derives from the msg ID.
    
    Keeps track of how many messages it echoes.
    But after not receiving any messages for 
    OnDemandPublisher.MAX_IDLE_TIME seconds, the
    current count is printed, and the counter is 
    reset to zero.
    
    '''

    # Maximum time for no message to arrive before
    # starting over counting messages:
    MAX_IDLE_TIME = 5
    
    def __init__(self, beSynchronous=True):
        threading.Thread.__init__(self)
        
        self.beSynchronous = beSynchronous
        
        self.daemon = True
        self.testBus = BusAdapter()
        
        # Subscribe, and ensure that context is delivered
        # with each message:
        self.testBus.subscribeToTopic(ECHO_CHANNEL, functools.partial(self.messageReceiver))
        self.interruptEvent = threading.Event()
        self.numEchoed = 0
        self.mostRecentRxTime = None
        self.printedResetting = False
        
        # Not asked to send a message yet.
        self.sendMsg = False
        
        signal.signal(signal.SIGINT, functools.partial(self.stop))
        
        self.done = False
        
    def sendMessage(self, bus_msg):
        '''
        Trigger interrupt, which will have the run() loop
        send a standard message.
        '''
        self.outMsg = bus_msg
        self.sendMsg = True
        self.interruptEvent.set()
        
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
        self.done = True
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
        while not self.done:
            self.startTime = time.time()
            self.startIdleTimer()
            self.interruptEvent.wait()
            if not self.done and self.sendMsg:
                self.testBus.publish(self.outMsg)
                self.interruptEvent.clear()
                self.sendMsg = False
            continue
        
        self.testBus.unsubscribeFromTopic(ECHO_CHANNEL)
        self.testBus.close()

if __name__ == '__main__':
    echoServer = OnDemandPublisher() 
    signal.signal(signal.SIGINT, echoServer.stop)
    echoServer.start()
    print('Started echo and on-demand publish server; cnt-C to stop...')
    # Pause till cnt-C causes stop() to be called on the thread:
    signal.pause()
    echoServer.join()
