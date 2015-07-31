'''
Created on Jul 27, 2015

@author: paepcke
'''
import argparse
import functools
import hashlib
import json
import os
import random
import signal
import string
import sys
import threading
import time
import traceback

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.test.performance_test_echo_server import \
    PerformanceTesterEchoServer


# Topic on which echo server listens:
ECHO_TOPIC = 'echo'

# Topic to which sendMessageStream() publishes:
STREAM_TOPIC = 'test'
SYNTAX_TOPIC = 'bus_syntax'

# Standard message length:
STANDARD_MSG_LENGTH = 100

class OnDemandPublisher(threading.Thread):
    '''
    Server thread for testing Redis bus. Serves four
    functions, individually or together:
    
        1. echoes messages it receives on topic
           ECHO_TOPIC. 
        2. sends a continuous stream of message
        3. listens to messages without doing anything
        4. if used as an import, send one message
           on demand.

    usage: test_harness_server [echo [send_stream]]

    The thread always listens on the ECHO_TOPIC. But
    received messages are only echoed if the service was
    started with the 'echo' option
    
    The echo function acts like a synch-call service
    should: echoing on the response topic, which it
    derives from the msg ID.
    
    When echoing the service keeps track of how many messages 
    it has echoed. But after not receiving any messages for 
    OnDemandPublisher.MAX_IDLE_TIME seconds, the
    current count is printed, and the counter is 
    reset to zero.
    
    Messages sent in a stream by sendMessageStream()
    contain their content's MD5 in the context field. This constant
    message sending is initiated only if the service is
    started with the 'send_stream' option.
    '''

    # Maximum time for no message to arrive before
    # starting over counting messages:
    MAX_IDLE_TIME = 5
    
    def __init__(self, serveEchos=True, listenOn=None, streamMsgs=False, check_syntax=False):
        threading.Thread.__init__(self)
        
        self.serveEchos = serveEchos
        
        self.daemon = True
        self.testBus = BusAdapter()
        
        # Start time of a 10,000 messages batch:
        self.batch_start_time = time.time()        
        
        # Subscribe, and ensure that context is delivered
        # with each message:

        if serveEchos:
            # Have incoming messages delivered to messageReceiver() not
            # via a queue, but by direct call from the library:
            self.testBus.subscribeToTopic(ECHO_TOPIC, functools.partial(self.messageReceiver), threaded=False)

        if listenOn is not None:
            # If we are to listen to some (additional) topic,
            # on which no action is taken, subscribt to it now:
            self.testBus.subscribeToTopic(listenOn, functools.partial(self.messageReceiver))

        if check_syntax:
            self.testBus.subscribeToTopic(SYNTAX_TOPIC, functools.partial(self.syntaxCheckReceiver))
            
        self.interruptEvent = threading.Event()
        
        # Number of messages received and echoed:
        self.numEchoed = 0
        # Alternative to above when not echoing: Number of msgs just received
        # and discarded:
        self.numReceived = 0
        self.mostRecentRxTime = None
        self.printedResetting = False
        
        # Not asked to send a message yet.
        self.sendMsg = False
        
        # Currently not streaming:
        self.nowStreaming = False
        # Supposed to stream messages?
        self.shouldStream = streamMsgs
        
        signal.signal(signal.SIGINT, functools.partial(self.stop))
        
        self.done = False
        
        if streamMsgs:
            MessageOutStreamer(msgLen=STANDARD_MSG_LENGTH).start()
        
    def sendMessage(self, bus_msg):
        '''
        Trigger interrupt, which will have the run() loop
        send one standard message.
        '''
        self.outMsg = bus_msg
        self.sendMsg = True
        self.interruptEvent.set()
        
    def syntaxCheckReceiver(self, busMsg, context=None):
        '''
        Given a BusMessage instance, determine whether it contains 
        all necessary attributes: ID, and time. This method 
        is a callback for topic specified in module variable
        SYNTAX_TOPIC. Constructs a string that lists any
        errors or warnings, and returns that string as a
        synchronous response.
        
        :param busMsg: the message to be evaluated
        :type busMsg: BusMessage
        :param context: not used
        :type context: <any>
        '''
        errors = []
        
        if busMsg.id is None:
            errors.append('Error: No ID field')
        if type(busMsg.time) != int and type(busMsg.time) != float:
            errors.append("Error: Time must be an int, was '%s'" % busMsg.time)
            try:
                busMsg.isoTime
            except (ValueError, TypeError):
                errors.append("Error: Time value not transformable to ISO time '%s'" % busMsg.time)
        try:
            json.loads(busMsg.content)
        except AttributeError:
            errors.append("Warning: no 'content' field.")
        except ValueError:
            errors.append("Warning: 'content' field present, but not valid JSON.")

        if len(errors) == 0:
            response = 'Message OK'
        else:
            response = self.testBus.makeResponseMsg(busMsg, '; '.join(errors))
        
        self.testBus.publish(response)
        
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
        
        if self.serveEchos:
            respMsg = self.testBus.makeResponseMsg(busMsg, busMsg.content)

            # Publish a response:
            self.testBus.publish(respMsg)
            self.numEchoed += 1

            if self.numEchoed % 1000 == 0:
                print('Echoed %d' % self.numEchoed)
                
            if self.numEchoed % 10000 == 0:
                # Messages per second:
                msgs_per_sec = 10000.0 / (time.time() - self.batch_start_time)
                print('Echoing: %f Msgs/sec' % msgs_per_sec)
                self.batch_start_time = time.time()
              
        else:
            self.numReceived += 1
            if self.numReceived % 1000 == 0:
                print('Rxed %d' % self.numReceived)
                
            if self.numReceived % 10000 == 0:
                # Messages per second:
                msgs_per_sec = 10000.0 / (time.time() - self.batch_start_time)
                print('Rx (no echo): %f Msgs/sec' % msgs_per_sec)
                self.batch_start_time = time.time()
                        
    def resetEchoedCounter(self):
        currTime = time.time()
        
        if self.mostRecentRxTime is None:
            # Nothing received yet:
            self.startTime = time.time()
            self.batch_start_time = time.time()
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
        self.batch_start_time = time.time()
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
        
        self.testBus.unsubscribeFromTopic(ECHO_TOPIC)
        self.testBus.close()

class MessageOutStreamer(threading.Thread):
    '''
    Thread that keeps sending the same message over and over,
    except for changing the time field.
    '''
    
    def __init__(self, msgLen=STANDARD_MSG_LENGTH):
        super(MessageOutStreamer, self).__init__()
        
        self.daemon = True
        self.streamBus = BusAdapter()
        self.busMsg = self.createMessage(STREAM_TOPIC, msgLen) #@UnusedVariable
    
        self.done = False

    
    def run(self):
        '''
        Keep publishing one message over and over,
        
        :param msgLen: length of payload
        :type msgLen: int
        '''
        while not self.done:
            self.busMsg.time = time.time()
            self.streamBus.publish(self.busMsg)

    def createMessage(self, topic, msgLen=STANDARD_MSG_LENGTH):
        '''
        Returns a BusMessage whose content is the given length,
        and whose topicName is the given topic.
        
        :param topic: topic for which this message is destined.
        :type topic: string
        :param msgLen: desired str length
        :type msgLen: int
        :return: BusMessage object ready to publish. The context field of the
            message instance will be the MD5 of the content.
        :rtype: BusMessage
        '''
        
        content = bytearray()
        for _ in range(msgLen):
            content.append(random.choice(string.letters))
        
        msg = BusMessage(content=content, topicName=STREAM_TOPIC, context=hashlib.md5(str(content)).hexdigest())

        return msg
            
    def stop(self, signum, frame):
        self.done = True
        

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
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-e', '--echo', 
                        help="Echo messages arriving on topic '%s' as sychronous replies." % ECHO_TOPIC,
                        action='store_true',
                        default=False)
    parser.add_argument('-s', '--streamMsgs', 
                        help="Send the same message over and over to topic '%s'." % STREAM_TOPIC, 
                        action='store_true',
                        default=False);
    parser.add_argument('-c', '--checkSyntax', 
                        help="Check syntax of messages arriving on topic '%s'; synchronously return result report." % SYNTAX_TOPIC, 
                        action='store_true',
                        default=False)
    parser.add_argument('-l', '--listenTo', 
                        help="Subscribe to given topic, and throw the messages away", 
                        dest='topic__to',
                        default=None)
    parser.add_argument('-o', '--oneshot', 
                        help="Given a topic and content (in quotes), send a bus message, and exit.",
                        dest='topic_and_content',
                        default=None)
    
    args = parser.parse_args();

    do_echo = send_stream = check_syntax = False
    listen_also = additional_topic = None

    services_to_start = []
    do_echo = args.echo
    if do_echo:
        services_to_start.append("echo (incoming on '%s')" % ECHO_TOPIC)

    send_stream = args.streamMsgs
    if send_stream:
        services_to_start.append('send_stream (to %s)' % STREAM_TOPIC)

    msg_syntax = args.checkSyntax
    if msg_syntax:
        services_to_start.append('msg_syntax (synchronous on %s)' % SYNTAX_TOPIC)
        
    listen_to = True if args.topic_to is not None else False 
    if listen_to:
        services_to_start.append("listen_to '%s'" % args.topic_to)
    
    one_shot = True if args.topic_and_content is not None else False
    if one_shot:
        (one_shot_content, one_shot_topic) = args.topic_and_content.split()
        services_to_start.append("oneshot message to '%s'" % one_shot_topic)
            
    echoServer = OnDemandPublisher(serveEchos=do_echo, listenOn=additional_topic, streamMsgs=send_stream, check_syntax=check_syntax)
     
    print('Started services: %s; cnt-C to stop...' % ', '.join(services_to_start))
         
    signal.signal(signal.SIGINT, echoServer.stop)
    echoServer.start()
    
    # Pause till cnt-C causes stop() to be called on the thread:
    signal.pause()
    echoServer.join()
