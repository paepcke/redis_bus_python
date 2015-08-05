#!/usr/bin/env python

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

# Topic on which echo server listens:
ECHO_TOPIC = 'echo'

# Topic to which sendMessageStream() publishes:
STREAM_TOPIC = 'test'
SYNTAX_TOPIC = 'bus_syntax'

# Standard message length:
STANDARD_MSG_LENGTH = 100

class OnDemandPublisher(threading.Thread):
    '''
    Server for testing Redis bus modules. Started from the
    command line, or imported into applications. Serves multiple
    functions, individually or together:
    
        1. echoes messages it receives on topic ECHO_TOPIC.
        2. listens to messages without doing anything
        3. sends a continuous stream of messages
        4. check the syntax of an incoming msg, returning the result
        5. send one message on demand.

	USAGE: test_harness_server.py [-h] [-e] [-s [topic [content ...]]] [-c]
	                              [-l topic [topic ...]]
	                              [-o [topic [content ...]]]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -e, --echo            Echo messages arriving on topic 'echo' as sychronous replies.
	  -s [topic [content ...]], --streamMsgs [topic [content ...]]
	                        Send the same bus message over and over. Topic, or both,
	                        topic and content may be provided. If content is omitted,
	                        a random string of 100 characters will be used.
	                        If topic is omitted as well, messages will be streamed to 'test'
	  -c, --checkSyntax     Check syntax of messages arriving on topic 'bus_syntax'; 
	                        synchronously return result report.
	  -l topic [topic ...], --listenOn topic [topic ...]
	                        Subscribe to given topic(s), and throw the messages away
	  -o [topic [content ...]], --oneshot [topic [content ...]]
	                        Given a topic and a content string, send a bus message. Topic, or both 
	                        topic and content may be provided. If content is omitted,
	                        a random string of 100 characters will be used.
	                        If topic is omitted as well, messages will be streamed to 'test'
	    

    The thread always listens on the ECHO_TOPIC. But
    received messages are only **echoed** if the service was
    started with the 'echo' option
    
    The echo function acts like a synch-call service
    should: echoing on the response topic, which it
    derives from the msg ID. The content will be the
    content of the incoming message, the timestamp
    will be the receipt time.
    
    When echoing, the service keeps track of how many messages 
    it has echoed. But after not receiving any messages for 
    OnDemandPublisher.MAX_IDLE_TIME seconds, the
    current count is printed, and the counter is 
    reset to zero. After every 1000 messages, the total
    number echoed is printed to the console. Every 10,000
    messages, the message/sec rate is printed as well.
    
    When asked to **listen** to some topic, messages on that
    topic are received and counted. Then the message is
    discarded. Statistics printing is as for echoing.
    
    Messages sent in a **stream** by sendMessageStream()
    contain their content's MD5 in the context field. This constant
    message sending is initiated only if the service is
    started with the 'send_stream' option.

    When asked to check message **syntax**, the service listens
    on SYNTAX_TOPIC, and synchronously returns a string 
    reporting on violations of SchoolBus specifications in 
    the incoming message.
    
    A **oneshot message** is sent if the server is started
    with the --oneshot option. If topic and/or content are provided,
    the message is loaded with the given content and published to
    the given topic. Defaults are to fill the content with a random
    string, and to publish to STREAM_TOPIC.
    
    '''

    # Maximum time for no message to arrive before
    # starting over counting messages:
    MAX_IDLE_TIME = 5
    
    def __init__(self, serveEchos=True, 
                 listenOn=None, 
                 streamMsgs=False, 
                 checkSyntax=True,
                 oneShotMsg=None):
        '''
        Initialize the test harness server. 
        
        :param serveEchos: if True receives messages on ECHO_TOPIC, and returns  
            message with the same content, but different timestamp as a synchronous call. 
        :type serveEchos: bool
        :param listenOn: if this parameter provides an array of topic(s), messages on these
            topics are received and counted, but discarded. Intermittent reception counts
            and reception rates are printed to the console.
        :type listenOn: {None | [string]}
        :param streamMsgs: if True, a continuous stream of messages are sent to
            STREAM_TOPIC. The timestamp is changed each time.
        :type streamMsgs: bool
        :param checkSyntax: if true, listens on SYNTAX_TOPIC, checks incoming messages
            for conformance to SchoolBus message format, and synchronously returns a result.
        :type checkSyntax: bool
        :param oneShotMsg: if non-none this parameter is expected to be a two-tuple
            containing a topic and a content. Either may be None. If topic is None,
            the message is sent to STREAM_TOPIC. If content is None, content will be
            a random string of length STANDARD_MSG_LENGTH.
        :type oneShotMsg: { None | ({string | None}, {string | None})}
        '''
        threading.Thread.__init__(self)
        
        #***********
#         print ('serveEchos %s' % serveEchos)
#         print ('listenOn %s' % listenOn)
#         print ('streamMsgs %s' % streamMsgs)
#         print('checkSyntax %s' % checkSyntax)
#         print('oneShotMsg %s' % str(oneShotMsg))
#         sys.exit()
        #***********
        
        self._serveEchos = serveEchos
        
        self.daemon = True
        self.testBus = BusAdapter()
        self.done = False
        self.listen_on = []
        
        self.echo_topic = ECHO_TOPIC
        self.syntax_check_topic = SYNTAX_TOPIC
        self.standard_msg_len = STANDARD_MSG_LENGTH
        
        self._stream_topic = STREAM_TOPIC
        self.standard_bus_msg = self.createMessage(STREAM_TOPIC, STANDARD_MSG_LENGTH, content=None)
        
        self.one_shot_topic = STREAM_TOPIC
        self.one_shot_content = self.standard_bus_msg
        

        # Handle Cnt-C properly:
        signal.signal(signal.SIGINT, functools.partial(self.stop))
        
        # Create a 'standard' message to send out
        # when asked to via SIGUSR1 or the Web:

        if oneShotMsg is not None:
            # The oneShotMsg arg may have one of the following values: 
            #   - None if oneshot was not requested. 
            #   - An empty array if -o (or --oneshot) 
            #        was listed in the command line, 
            #        but neither topic nor message content
            #        were specified.
            #   - A one-element array if the topic was
            #        specified
            #   - A two-element array if both topic and
            #        content were specified.

            (one_shot_topic, self.one_shot_content) = oneShotMsg
            if one_shot_topic is not None:
                self.one_shot_topic = one_shot_topic
            
            # Create a message that one can have sent on demand
            # by sending a SIGUSR1 to the server, or using
            # the Web UI:

            self.standard_oneshot_msg = self.createMessage(self.one_shot_topic, content=self.one_shot_content)
            # Send the standard msg:
            self.sendMessage(self.standard_oneshot_msg)
        else:
            # Create a standard message with default topic/content:
            self.standard_oneshot_msg = self.createMessage()
       
        # Start time of a 10,000 messages batch:
        self.batch_start_time = time.time()        
        
        # Subscribe, and ensure that context is delivered
        # with each message:

        if serveEchos:
            self.serve_echo = True
        
        if listenOn is not None:
            # Array of topics to listen to (and discard): 
            # If we are to listen to some (additional) topic(s),
            # on which no action is taken, subscribe to it/them now:
            self['discardTopics'] = listenOn

        if checkSyntax:
            self.check_syntax = True
            
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
        
        if streamMsgs is not None:
            # The streamMsgs arg may have one of the following values: 
            #   - None if streaming messages was not requested. 
            #   - An empty array if -s (or --streamMsgs) 
            #        was listed in the command line, 
            #        but neither topic nor message content
            #        were specified.
            #   - A one-element array if the topic was
            #        specified
            #   - A two-element array if both topic and
            #        content were specified.

            self.msg_streamer = MessageOutStreamer(self.standard_bus_msg)
            
            # The caller passed in a tuple with
            # topic and content for streaming. If they
            # are non-None, update the streamer:
            
            (stream_topic, stream_content) = streamMsgs
            if stream_topic is not None:
                self.stream_topic = stream_topic
            if stream_content is not None:
                self.stream_content = stream_content

            # Start streaming:
            
            self.msg_streamer.pause(True)
            self.msg_streamer.start()
            
        else:
            # Create a standard message with default topic/content:
            self.standard_stream_msg = self.createMessage()

    @property
    def running(self):
        return not self.done

    @property
    def echo_topic(self):
        return self._echo_topic
    
    @echo_topic.setter
    def echo_topic(self, new_echo_topic):
        self._echo_topic = new_echo_topic

    @property
    def serve_echo(self):
        return self._serveEchos

    @serve_echo.setter
    def serve_echo(self, do_serve):
        if do_serve:
            # Have incoming messages delivered to messageReceiver() not
            # via a queue, but by direct call from the library:
            self.testBus.subscribeToTopic(self.echo_topic, functools.partial(self.messageReceiver), threaded=False)
            self._serveEchos = True
        else:
            self.testBus.unsubscribeFromTopic(self.echo_topic)
            self._serveEchos = False

    @property
    def stream_topic(self):
        return self.msg_streamer.busMsg.topicName
    
    @stream_topic.setter
    def stream_topic(self, new_stream_topic):
        self.msg_streamer.change_stream_topic(new_stream_topic)

    @property
    def stream_content(self):
        return self.msg_streamer.busMsg.content
    
    @stream_content.setter
    def stream_content(self, new_stream_content):
        self.msg_streamer.change_stream_content(new_stream_content)

    @property
    def streaming(self):
        return not self.msg_streamer.paused
    
    @streaming.setter
    def streaming(self, should_stream):
        if not (should_stream == False or should_stream == True):
            raise ValueError('New state for streaming must be True or False')

        should_pause = not should_stream
        self.msg_streamer.pause(should_pause)

    @property
    def one_shot_topic(self):
        return self._one_shot_topic
    
    @one_shot_topic.setter
    def one_shot_topic(self, new_one_shot_topic):
        self._one_shot_topic = new_one_shot_topic
        
    @property
    def one_shot_content(self):
        return self._one_shot_content
    
    @one_shot_content.setter
    def one_shot_content(self, new_one_shot_topic):
        self._one_shot_content = new_one_shot_topic


    @property
    def syntax_check_topic(self):
        return self._syntax_check_topic
    
    @syntax_check_topic.setter
    def syntax_check_topic(self, new_syntax_check_topic):
        self._syntax_check_topic = new_syntax_check_topic

    @property
    def check_syntax(self):
        return self._checkSyntax

    @check_syntax.setter
    def check_syntax(self, do_check):
        if do_check:
            self.testBus.subscribeToTopic(self.syntax_check_topic, functools.partial(self.syntaxCheckReceiver))
            self._checkSyntax = True
        else:
            self.testBus.unsubscribeFromTopic(self.syntax_check_topic)
            self._checkSyntax = False

    @property
    def standard_msg_len(self):
        return self._standard_msg_len
    
    @standard_msg_len.setter
    def standard_msg_len(self, new_standard_msg_len):
        self._standard_msg_len = new_standard_msg_len
    
    def __getitem__(self, item):
        if item == 'echoTopic':
            return self.echo_topic

        elif item == 'echo':
            self.testBus.subscribedTo(self.echoTopic)
        
        elif item == 'streamTopic':
            return self.stream_topic
        elif item == 'streamContent':
            return self.stream_content
        
        elif item == 'oneShotTopic':
            return self.one_shot_topic
        elif item == 'oneShotContent':
            if isinstance(self.one_shot_content, BusMessage):
                return self.one_shot_content.content
            else:
                return self.one_shot_content
        
        elif item == 'strLen':
            return self.standard_msg_len
        
        elif item == 'syntaxTopic':
            return self.syntax_check_topic
        
        elif item == 'chkSyntax':
            self.testBus.subscribedTo(self.syntaxTopic)

        
        elif item == 'discardTopics':
            return self.listen_on
        
        else:
            raise KeyError('Key %s is not in schoolbus tester' % item)
    
    def __setitem__(self, item, new_val):
        if item == 'echoTopic':
            self.echo_topic = new_val
            
        elif item == 'echo':
            self.serve_echo = new_val

        elif item == 'streamTopic':
            self.stream_topic = new_val
        elif item == 'streamContent':
            self.stream_conent = new_val
            
        elif item == 'oneShotContent':
            self.one_shot_content = new_val
        
        elif item == 'oneShotTopic':
            self.one_shot_topic = new_val
        elif item == 'oneShotContent':
            self.one_shot_content = new_val
            
        elif item == 'strLen':
            self.standard_msg_len = new_val
            
        elif item == 'syntaxTopic':
            self.syntax_check_topic = new_val
            
        elif item == 'chkSyntax':
            self.check_syntax = new_val
            
        elif item == 'discardTopics':
            # Unsubscribe from all topics:
            self.testBus.unsubscribeFromTopic()
            
            if type(new_val) != list:
                new_val = [new_val]
            for topic in new_val:
                self.testBus.subscribeToTopic(topic, functools.partial(self.messageReceiver))
                
            self.listen_on = new_val
            
        else:
            raise KeyError('Key %s is not in schoolbus tester' % item)
        return new_val

    
    def createMessage(self, topic=None, msgLen=None, content=None):
        '''
        Returns a BusMessage whose content is the given length,
        and whose topicName is the given topic.
        
        :param topic: topic for which this message is destined.
        :type topic: string
        :param msgLen: desired str length
        :type msgLen: int
        :param content: content of the message. If None, a random content
            of length msgLen will be created
        :return: BusMessage object ready to publish. The context field of the
            message instance will be the MD5 of the content.
        :rtype: BusMessage
        '''
    
        if topic is None:
            topic = self._stream_topic
            
        if msgLen is None:
            msgLen = self.standard_msg_len 
    
        if content is None:    
            content = bytearray()
            for _ in range(msgLen):
                content.append(random.choice(string.letters))
        
        msg = BusMessage(content=content, topicName=topic, context=hashlib.md5(str(content)).hexdigest())

        return msg
        
    def sendMessage(self, bus_msg=None):
        '''
        Publish the given BusMessage, or
        the standard message:
        '''
        if bus_msg is None:
            bus_msg = self.standard_oneshot_msg
        self.testBus.publish(bus_msg)
        
    def syntaxCheckReceiver(self, busMsg, context=None):
        '''
        Given a BusMessage instance, determine whether it contains 
        all necessary attributes: ID, and time. This method 
        is a callback for topic specified in module variable
        self.syntaxTopic. Constructs a string that lists any
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
        
        if self.serve_echos:
            # Create a message with the same content as the incoming
            # message, timestamp the new message, and publish it
            # on the response topic (i.e. tmp.<msgId>):
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
        
        if currTime - self.mostRecentRxTime <= OnDemandPublisher.MAX_IDLE_TIME:
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
        threading.Timer(OnDemandPublisher.MAX_IDLE_TIME, functools.partial(self.resetEchoedCounter)).start()

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
        
        self.testBus.unsubscribeFromTopic(self._echo_topic)
        self.testBus.close()

class MessageOutStreamer(threading.Thread):
    '''
    Thread that keeps sending the same message over and over,
    except for changing the time field.
    '''
    
    def __init__(self, busMsg=None):
        super(MessageOutStreamer, self).__init__()
        
        self.daemon = True
        self.streamBus = BusAdapter()
        self.busMsg = BusMessage() if busMsg is None else busMsg
    
        self.done = False
        self._paused = False
        
    def pause(self, do_pause):
        if do_pause:
            self._paused = True
        else:
            self._paused = False

    @property
    def paused(self):
        return self._paused
    
    def change_stream_topic(self, newTopic):
        self.busMsg.stream_topic = newTopic
    
    def change_stream_content(self, newContent):
        self.busMsg.stream_content = newContent

    def run(self):
        '''
        Keep publishing one message over and over,
        
        :param msgLen: length of payload
        :type msgLen: int
        '''
        while not self.done:
            self.busMsg.time = time.time()
            self.streamBus.publish(self.busMsg)
            while self._paused:
                time.sleep(1)
            
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
                        default=True)
    parser.add_argument('-s', '--streamMsgs', 
                        help="Send the same bus message over and over. Topic, or both,\n" +\
                             "topic and content may be provided. If content is omitted,\n" +\
                             "a random string of %d characters will be used.\n" % STANDARD_MSG_LENGTH +\
                             "If topic is omitted as well, messages will be streamed to '%s'" % STREAM_TOPIC,
                        dest='stream_topic_and_content',
                        nargs='*',
                        metavar=('topic', 'content'),
                        default=None);
    parser.add_argument('-c', '--checkSyntax', 
                        help="Check syntax of messages arriving on topic '%s'; \n" % SYNTAX_TOPIC +\
                             "synchronously return result report.", 
                        action='store_true',
                        default=True)
    parser.add_argument('-l', '--listenOn', 
                        help="Subscribe to given topic(s), and throw the messages away", 
                        dest='topic_to',
                        nargs=('+'),
                        metavar='topic',
                        default=None)
    parser.add_argument('-o', '--oneshot', 
                        help="Given a topic and a content string, send a bus message. Topic, or both \n" +\
                             "topic and content may be provided. If content is omitted,\n" +\
                             "a random string of %d characters will be used.\n" % STANDARD_MSG_LENGTH +\
                             "If topic is omitted as well, messages will be streamed to '%s'" % STREAM_TOPIC,
                        dest='oneshot_topic_and_content',
                        nargs='*',
                        metavar=('topic','content'),
                        default=None)
    
    args = parser.parse_args();

    #**********
    #print(args)
    #sys.exit()
    #**********

    do_echo = send_stream = check_syntax = False
    listen_also = additional_topic = None

    services_to_start = []
    do_echo = args.echo
    if do_echo:
        services_to_start.append("echo (incoming on '%s')" % ECHO_TOPIC)

    # The stream_topic_and_content message CLI argument topic_and_content
    # may have one of the following values: 
    #   - None if stream_topic_and_content was not requested. 
    #   - An empty array if -s (or --stream) 
    #        was listed in the command line, 
    #        but neither topic nor message content
    #        were specified.
    #   - A one-element array if the topic was
    #        specified
    #   - A two-element array if both topic and
    #        content were specified.
     
    streaming = args.stream_topic_and_content
    if streaming is not None:
        if len(args.stream_topic_and_content) == 0:
            (stream_topic, stream_content) = [None, None]
        elif len(args.stream_topic_and_content) == 1:
            # Create tuple with topic and None as content:
            (stream_topic, stream_content) = streaming + [None]
        else:
            # Both topic and content were provided:
            (streaming_topic, streaming_content) = streaming
        
        display_topic = streaming_topic if streaming_topic is not None else STREAM_TOPIC
        display_content = streaming_content if streaming_content is not None else 'random %d chars' % STANDARD_MSG_LENGTH
        services_to_start.append('streamMsg (content %s) (to %s)' % (display_content, display_topic))

    listen_on = args.topic_to
    if listen_on is not None:
        services_to_start.append("Listen for and discard messages on '%s')" % str(listen_on))

    check_syntax = args.checkSyntax
    if check_syntax:
        services_to_start.append('check_syntax (synchronous on %s)' % SYNTAX_TOPIC)
        
    listen_to = True if args.topic_to is not None else False 
    if listen_to:
        services_to_start.append("listen_to '%s'" % args.topic_to)
    
    # The oneshot message CLI argument oneshot_topic_and_content
    # may have one of the following values: 
    #   - None if oneshot was not requested. 
    #   - An empty array if -o (or --oneshot) 
    #        was listed in the command line, 
    #        but neither topic nor message content
    #        were specified.
    #   - A one-element array if the topic was
    #        specified
    #   - A two-element array if both topic and
    #        content were specified.
     
    one_shot = args.oneshot_topic_and_content
    if one_shot is not None:
        if len(args.oneshot_topic_and_content) == 0:
            (one_shot_topic, one_shot_content) = [None, None]
        elif len(args.oneshot_topic_and_content) == 1:
            # Create tuple with topic and None as content:
            (one_shot_topic, one_shot_content) = one_shot + [None]
        else:
            (one_shot_topic, one_shot_content) = one_shot
            
        display_topic = one_shot_topic if one_shot_topic is not None else STREAM_TOPIC
        display_content = one_shot_content if one_shot_content is not None else 'random %d chars' % STANDARD_MSG_LENGTH
        services_to_start.append('oneshot message ( %s) (to %s)' % (display_content, display_topic))
            
    testServer = OnDemandPublisher(serveEchos=do_echo, 
                                   listenOn=listen_on, 
                                   streamMsgs=None if streaming is None else (streaming_topic, streaming_content),
                                   checkSyntax=check_syntax, 
                                   oneShotMsg=None if one_shot is None else (one_shot_topic, one_shot_content)
                                   )
    if streaming:
        # Streaming starts in a _paused state; let it rip:
        testServer.streaming = False
        
    print('Started services: %s; cnt-C to stop...' % ', '.join(services_to_start))
         
    signal.signal(signal.SIGINT, testServer.stop)
    testServer.start()
    
    # Pause till cnt-C causes stop() to be called on the thread:
    signal.pause()
    testServer.join()


