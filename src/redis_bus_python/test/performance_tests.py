

'''
Created on Jul 8, 2015

@author: paepcke
'''

import functools
import hashlib
import random
import signal
import socket
import string
import sys
import threading
import time
import traceback

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.redis_lib.exceptions import TimeoutError
from redis_bus_python.redis_lib.pubsub_listener import PubSubListener
from redis_bus_python.schoolbus_exceptions import SyncCallTimedOut
from redis_bus_python.test.test_harness_server import ECHO_TOPIC


class RedisPerformanceTester(object):
    '''
    classdocs
    '''

    PUBLISH_TOPIC = 'test'

    def __init__(self):
        '''
        Constructor
        '''
        self.host = 'localhost'
        self.port = 6379
        
        self.letterChoice = string.letters
        self.bus = BusAdapter()
        
    def close(self):
        self.bus.close()
    
    def publishToUnsubscribedTopic(self, numMsgs, msgLen, block=True):
        '''
        Publish given number of messages to a topic that
        nobody is subscribed to. Each msg is of length msgLen.
        
        :param numMsgs:
        :type numMsgs:
        :param msgLen:
        :type msgLen:
        '''
        (msg, md5) = self.createMessage(msgLen) #@UnusedVariable
        busMsg = BusMessage(content=msg, topicName=RedisPerformanceTester.PUBLISH_TOPIC)
        startTime = time.time()
        for _ in range(numMsgs):
            self.bus.publish(busMsg, block=block)
        endTime = time.time()
        self.printResult('Publishing %s msgs to empty space (block==%s): ' % (str(numMsgs),str(block)), startTime, endTime, numMsgs)
    
    def publishToSubscribedTopic(self, numMsgs, msgLen, block=True, sameProcessListener=True):
        '''
        Publish given number of messages to a topic that
        a thread in this same process is subscribed to.
        These are asynchronous publish() calls. The receiving
        thread computes MD5 of the received msg to verify.
        
        :param numMsgs:
        :type numMsgs:
        :param msgLen:
        :type msgLen:
        :param block: if True, publish() will wait for server confirmation.
        :type block: bool
        :param sameProcessListener: if True, the listener to messages will be a thread in this
            Python process (see below). Else an outside process is expected to be subscribed
            to RedisPerformanceTester.PUBLISH_TOPIC.
        :type sameProcessListener: bool
        '''

        (msg, md5) = self.createMessage(msgLen)
        busMsg = BusMessage(content=msg, topicName=RedisPerformanceTester.PUBLISH_TOPIC)
        try:
            listenerThread = ReceptionTester(msgMd5=md5, beSynchronous=False)
            listenerThread.daemon = True
            listenerThread.start()
            
            startTime = time.time()
            for _ in range(numMsgs):
                self.bus.publish(busMsg, block=block)
            endTime = time.time()
            self.printResult('Publishing %s msgs to a subscribed topic (block==%s): ' % (str(numMsgs), str(block)), startTime, endTime, numMsgs)
        except Exception:
            raise
        finally:
            listenerThread.stop()
            listenerThread.join(3)
            
    def syncPublishing(self, numMsgs, msgLen, block=True):

        #sys.stdout.write('Run python src/redis_bus_python/test/test_harness_server.py echo and hit ENTER...')
        #sys.stdin.readline()
        
        (msg, md5) = self.createMessage(msgLen) #@UnusedVariable

        busMsg = BusMessage(content=msg, topicName=ECHO_TOPIC)


        startTime = time.time()
        try:
            for serialNum in range(numMsgs):
                try:
                    busMsg.id = serialNum
                    res = self.bus.publish(busMsg, sync=True, timeout=5, block=block) #@UnusedVariable
                except SyncCallTimedOut:
                    #printThreadTraces()
                    raise
                    
            endTime = time.time()
            self.printResult('Publishing %s synch msgs (block==%s): ' % (str(numMsgs), str(block)), startTime, endTime, numMsgs)
        finally:
            pass
        
        
    def rawIronPublish(self, numMsgs, msgLen, block=True):
        
        sock = self.bus.topicWaiterThread.pubsub.connection._sock
        sock.setblocking(1)
        sock.settimeout(2)
        
        (msg, md5) = self.createMessage(msgLen) #@UnusedVariable
        wireMsg = '*3\r\n$7\r\nPUBLISH\r\n$4\r\ntest\r\n$190\r\n{"content": \r\n "dRkLSUQxFVSHuVnEekLtfPsXULtWEESQwaRYZtxpFGYRGphNTkRQAMPJfDxoGKOKPCMmptZBriVVfV\r\n LvYisehirsYSHdDrhXRgGl", "id": "a62b8cde-6bf6-4f75-a1f3-e768bec4d5e1", "time": 1437516321946}\r\n'
        num_sent = 0
        start_time = time.time()
        try:
            for _ in range(numMsgs):
                sock.sendall(wireMsg)
                num_sent += 1
    #             if num_sent % 1000 == 0:
    #                 print('Sent %d' % num_sent)
                if block:
                    #time.sleep(0.01)
                    numListeners = sock.recv(1024) #@UnusedVariable
        except socket.timeout:
            end_time = time.time()
            self.printResult('Sending on raw socket; result timeout after %d msgs.' % num_sent, start_time, end_time, numMsgs)
            sys.exit()  
        except Exception:
            end_time = time.time()
            self.printResult('Sending on raw socket; error %d msgs.' % num_sent, start_time, end_time, numMsgs)
            raise
            
        end_time = time.time()
        self.printResult('Sent %d msgs on raw socket; block==%s' % (numMsgs, block), start_time, end_time, numMsgs)
    
    def justListenAndCount(self):
        listenerThread = ReceptionTester(beSynchronous=False)
        listenerThread.daemon = True
        listenerThread.start()
        signal.pause()
        listenerThread.stop()
        listenerThread.join()
    
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

    def _connect(self):

        err = None
        # Get addr options for Redis host/port, with arbitrary
        # socket family (the 0), and stream type:
        for res in socket.getaddrinfo(self.host, self.port, 0,
                                      socket.SOCK_STREAM):
            family, socktype, proto, canonname, socket_address = res  #@UnusedVariable
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                
                sock.connect((self.host, self.port))                
                return sock

            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        raise socket.error("socket.getaddrinfo returned an empty list")

    
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
    
    def __init__(self, msgMd5=None, beSynchronous=False, topic_to_wait_on=RedisPerformanceTester.PUBLISH_TOPIC):
        threading.Thread.__init__(self, name='PerfTestReceptor')
        self.daemon = True
       
        self.rxed_count = 0
        self.beSynchronous = beSynchronous
        
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
        if context is not None:
            inMd5 = hashlib.md5(str(busMsg.content)).hexdigest()
            # Check that the context was delivered:
            if inMd5 != context:
                raise ValueError("md5 in msg should be %s, but was %s" % (inMd5, context))

        self.rxed_count += 1
        if self.rxed_count % 1000 == 0:
            print(self.rxed_count)
        
        if self.beSynchronous:
            # Publish a response:
            self.testBus.publish(self.testBus.makeResponseMsg(busMsg), busMsg.content)

    
    def stop(self):
        self.interruptEvent.set()
            
    def run(self):
        self.interruptEvent.wait()
        self.testBus.unsubscribeFromTopic(RedisPerformanceTester.PUBLISH_TOPIC)
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
        
    
        
if __name__ == '__main__':

    tester = RedisPerformanceTester()
    
    usage = 'Usage: {listen | publish | syncpublish}' 
    
    if len(sys.argv) > 1:
        to_do = sys.argv[1]
    else:
        print(usage)
        sys.exit()

#     print('Raw iron')
#     # Keep run() in PubSub from stealing return values:
#     tester.bus.topicWaiterThread.pubsub.pauseInTraffic()
#     tester.rawIronPublish(10000, 100, block=True)
#     # Re-establish normal operations in PubSub run() loop:
#     tester.bus.topicWaiterThread.pubsub.continueInTraffic()
#     
#     sys.exit()
    
    if to_do == 'publish':
        print("------Publish to '%s'; block == False------" % RedisPerformanceTester.PUBLISH_TOPIC)
        tester.publishToUnsubscribedTopic(10000, 100, block=False)
        print("------Publish to '%s'; block == True------" % RedisPerformanceTester.PUBLISH_TOPIC)
        tester.publishToUnsubscribedTopic(10000, 100, block=True)
        print('--------------------')
     
        sys.exit()
    
#    sys.stdout.write('Run python src/redis_bus_python/test/performance_test_echo_server.py and hit ENTER...')
#    sys.stdin.readline()

#    print('------Publish 10,000 msgs of len 100 to a subscribed topic; block=False------')    
#    tester.publishToSubscribedTopic(10000,100, block=False, sameProcessListener=False)
#    print('------Publish 10,000 msgs of len 100 to a subscribed topic; block=True------')    
#    tester.publishToSubscribedTopic(10000,100, block=True, sameProcessListener=False)
#    print('--------------------')    
    
    if to_do == 'syncpublish':
        try:
            print('------Synch-Publish 10,000 msgs of len 100 to echo server; block=False------')    
            tester.syncPublishing(10000,100, block=False)
            print('------Synch-Publish 10,000 msgs of len 100 to echo server; block=True------')
            tester.syncPublishing(10000,100, block=True)
        except TimeoutError:
            "Didn't get echo messages; is <redis_code_root>/src/redis_bus_python/test/test_harness_server.py running?"
        
        sys.exit()    

    
#    tester.syncPublishing(10000,100, block=True)

#     while True:
#         tester.publishToUnsubscribedTopic(10000,100,block=True)
    
    if to_do == 'listen':
        try:
            tester.justListenAndCount()
            print('--------------------')
        except TimeoutError:
            "Didn't get any messages; is <redis_code_root>/src/redis_bus_python/test/test_harness_server.py running?"
        
        sys.exit()    
        

    tester.close()
