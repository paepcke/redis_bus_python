#!/usr/bin/env python

'''
Created on Jul 9, 2015

@author: paepcke
'''
import functools
import signal
import sys
import threading

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter


class PerformanceTesterEchoServer(threading.Thread):
    '''
    Thread that receives messages, and asserts
    that the received values are what was passed
    into the thread's init method. Keeps listening
    till stop() is called.
    '''
    
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
        #**********
        print("Got msg.")
        #**********
        if self.beSynchronous:
            respMsg = BusMessage(content=busMsg.content, topicName=self.testBus.getResponseTopic(busMsg))
            # Publish a response:
            self.testBus.publish(respMsg)
            self.numEchoed += 1

    
    def stop(self):
        self.eventForStopping.set()
            
    def run(self):
        self.eventForStopping.wait()
        self.testBus.unsubscribeFromTopic('test')
        self.testBus.close()

def signal_handler(signal, frame):
        print('Stopping SchoolBus echo service.')
        sys.exit(0)

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)
    
    echoServer = PerformanceTesterEchoServer(beSynchronous=True)
    print("Starting to echo msgs on topic 'test'; cnt^C to stop...")
    echoServer.start()
