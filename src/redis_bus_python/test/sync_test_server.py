
'''
Created on Jul 26, 2015

@author: paepcke
'''
import functools
import hashlib
import signal
import sys
import threading

from redis_bus_python.redis_bus import BusAdapter


class ReceptionTester(threading.Thread):
    '''
    Thread that receives messages, and asserts
    that the received values are what was passed
    into the thread's init method. Keeps listening
    till stop() is called.
    '''
    
    def __init__(self, msgMd5=None, beSynchronous=False, topic_to_wait_on='test'):
        threading.Thread.__init__(self, name='PerfTestReceptor')
        self.setDaemon(True)
        
        self.beSynchronous = beSynchronous
        self.topic_to_wait_on = topic_to_wait_on
        
        self.testBus = BusAdapter()
        
        # Subscribe, and ensure that context is delivered
        # with each message:
        self.testBus.subscribeToTopic(topic_to_wait_on, 
                                      functools.partial(self.messageReceiver), 
                                      context=msgMd5)
        self.interruptEvent = threading.Event()
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
        
#         inMd5 = hashlib.md5(str(busMsg.content)).hexdigest()
#         # Check that the context was delivered:
#         if inMd5 != context:
#             raise ValueError("md5 in msg should be %s, but was %s" % (inMd5, context))
                
        if self.beSynchronous:
            # Publish a response:
            self.testBus.publish(self.testBus.makeResponseMsg(busMsg, busMsg.content))

    
    def stop(self, signum=None, frame=None):
        #**********
        print('Cntr-C called')
        #**********
        self.interruptEvent.set()
            
    def run(self):
        print("Sync-call test server started; listening on %s; Cnt-C to quit" % self.topic_to_wait_on)
        self.interruptEvent.wait()
        self.testBus.unsubscribeFromTopic('test')
        self.testBus.close()

if __name__ == '__main__':
    testServer = ReceptionTester(beSynchronous=True)
    signal.signal(signal.SIGINT, testServer.stop)
    testServer.start()
    # Pause till cnt-C causes stop() to be called on the thread:
    signal.pause()
    testServer.join()
