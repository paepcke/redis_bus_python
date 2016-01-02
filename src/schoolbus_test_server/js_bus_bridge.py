'''
Created on Jan 2, 2016

@author: paepcke
'''
#!/usr/bin/env python
'''
Created on Aug 1, 2015

@author: paepcke

Generic bridge connecting JavaScript-running
browsers with the SchoolBus. Basic bus interactions
are implemented: (un)subscribe, publish. The
generic counterpart is the JavaScript bus_interactor.js.
It knows to talk to this bridge.

Protocol:
   - All messages between browser and this bridge are JSON formatted.
   - Subscribe:         {"cmd" : "subscribe", "topic" : "myTopic"}
        -> no response
   - Unsubscribe:       {"cmd" : "unsubscribe", "topic" : "myTopic"}
        -> no response
   - Publish:           {"cmd" : "publish", "msg" : "msgTxt", "topic" : "myTopic} 
        -> no response
   - Subscription list: {"cmd" : "subscribed_to"}
        -> response:    {"resp": ["topic1", "topic2", ...]}
   
   - Message delivery: when messages arrive from the bus the browser
       is called with   {"resp" : "theMessageContent", 
                         "topic" : "theTopic",
                         "time" : "isoSendTimeStr"} 
   - Errors: errors flow from the bridge to the browser. Error msgs
             look like this:
     Error delivery:    {"error" : "errMsg"}
             where details are optional.
'''

import Queue
from __builtin__ import True
import datetime
import functools
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading

import argparse
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.websocket import WebSocketHandler, WebSocketClosedError

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.redis_lib.exceptions import TimeoutError


sys.path.append(os.path.dirname(__file__))

JS_BRIDGE_WEBSOCKET_PORT = 4363

DO_BLOCK = True

# Time to wait in join() for child threads to 
# terminate:
JOIN_WAIT_TIME = 5 # sec


class JsBusBridge(WebSocketHandler):
    '''
    Awaits websocket connections, and forwards
    SchoolBus commands to the bus.
    '''

    # ----------------------------- Class-Level Constants ---------------------
    
    # Periodic check for incoming messages to forward to browser:
    
    PERIODIC_IN_MSG_CHECK = 100 # milliseconds
    
    LOG_LEVEL_NONE  = 0
    LOG_LEVEL_ERR   = 1
    LOG_LEVEL_INFO  = 2
    LOG_LEVEL_DEBUG = 3
    
    IMPLEMENTED_CMDS = ['subscribe',
                        'unsubscribe',
                        'publish',
                        'subscribed_to'
                        ]

    # ----------------------------- Class Variables ---------------------
    
    instantiation_lock = threading.Lock()
    
    # SchoolBus server (a.k.a. redis server)
    bus = None

    # ----------------------------- Methods ---------------------
    
    def __init__(self, application, request, **kwargs):
        
        with JsBusBridge.instantiation_lock:
            self.title = "SchoolBus Tester"
            super(JsBusBridge, self).__init__(application, request, **kwargs)
            
            self.loglevel = JsBusBridge.LOG_LEVEL_DEBUG
            #self.loglevel = BusTesterWebController.LOG_LEVEL_INFO
            #self.loglevel = BusTesterWebController.LOG_LEVEL_NONE
            
            # Queue to pass requests from the browser, which come in
            # through the websocket, to the thread that handles them
            # asynchronously:
            
            self.browser_request_queue = Queue.Queue() 
            
            # Callback from ioloop to this instance
            # to check the above queues of messages
            # from the schoolbus:
            
            self.periodic_callback = None
            
            # Thread that handles all requests from the browser;
            # pass self for that thread to refer back to this instance:
            
            self.browser_interactor_thread = BrowserInteractorThread(self, self.browser_request_queue)
            # Make thread die if this parent instance goes away:
            self.browser_interactor_thread.daemon = True
            self.browser_interactor_thread.start()
        
    def allow_draft76(self):
        '''
        Allow WebSocket connections via the old Draft-76 protocol. It has some
        security issues, and was replaced. However, Safari (i.e. e.g. iPad)
        don't implement the new protocols yet. Overriding this method, and
        returning True will allow those connections.
        '''
        return True

    def check_origin(self, origin):
        return True

    def open(self): #@ReservedAssignment
        '''
        Called by WebSocket/tornado when a client connects. Method must
        be named 'open'
        '''
        self.logDebug("Open called")
    
    def on_message(self, msg):
        if (msg == 'keepAlive'):
            return
        
        # Get the JSON in the post body, and replace unicode
        # with str types:
        try:
            msg_dict = self.byteify(json.loads(msg))
        except ValueError:
            # Bad JSON:
            self.return_error({}, "Message from client not good JSON: '%s'" % str(msg))
            return
        
        # Have the service thread deal with the details:
        self.browser_request_queue.put_nowait(msg_dict)
        
        return
    
    def on_close(self):
        self.logDebug('Websocket was closed; shutting down school test server...')
        
        self.browser_interactor_thread.stop()
        self.browser_interactor_thread.join(JOIN_WAIT_TIME)
        if self.browser_interactor_thread.is_alive():
            raise TimeoutError("Unable to stop browser interactor thread '%s'." % self.browser_interactor_thread.name)
        
        self.browser_interactor_thread = None
        self.logDebug('School test server is shut down...')
        #**********
        #threadStacktraces()
        #threading.enumerate()
        #**********
        
    def _write_to_browser(self, msg):
        '''
        Write msg to the browser that is connected through
        the websocket. Don't call this method directly. It
        is a callback for the tornado ioloop.
        
        :param msg: text to send
        :type msg: str
        '''
        try:
            self.write_message(msg)
        except WebSocketClosedError:
            self.logErr("Attempt to write to socket after it was closed.")
    
    def byteify(self, the_input):
        '''
        Turn unicode buried in data structures
        to str types:
        
        :param the_input: data structure to convert
        :type the_input: <any>
        '''
        if isinstance(the_input, dict):
            return {self.byteify(key):self.byteify(value) for key,value in the_input.iteritems()}
        elif isinstance(the_input, list):
            return [self.byteify(element) for element in the_input]
        elif isinstance(the_input, unicode):
            return the_input.encode('utf-8')
        else:
            return the_input

    def logInfo(self, msg):
        if self.loglevel >= JsBusBridge.LOG_LEVEL_INFO:
            print(str(datetime.datetime.now()) + ' info: ' + msg)

    def logErr(self, msg):
        if self.loglevel >= JsBusBridge.LOG_LEVEL_ERR:
            print(str(datetime.datetime.now()) + ' error: ' + msg)

    def logDebug(self, msg):
        if self.loglevel >= JsBusBridge.LOG_LEVEL_DEBUG:
            print(str(datetime.datetime.now()) + ' debug: ' + msg)
    
    @classmethod
    def getCertAndKey(self):
        '''
        Return a 2-tuple with full paths, respectively to
        the SSL certificate, and private key.
        To find the SSL certificate location, we assume
        that it is stored in dir '.ssl' in the current
        user's home dir.
        We assume the cert file either ends in .cer, or
        in .crt, and that the key file ends in .key.
        The first matching files in the .ssl directory
        are grabbed.

        @return: two-tuple with full path to SSL certificate, and key files.
        @rtype: (str,str)
        @raise ValueError: if either of the files are not found.

        '''
        homeDir = os.path.expanduser("~")
        sslDir = '%s/.ssl/' % homeDir
        try:
            certFileName = next(fileName for fileName in os.listdir(sslDir)
	                               if fileName.endswith('.cer') or fileName.endswith('.crt'))
        except StopIteration:
            raise(ValueError("Could not find ssl certificate file in %s" % sslDir))

        try:
            privateKeyFileName = next(fileName for fileName in os.listdir(sslDir)
	                                     if fileName.endswith('.key'))
        except StopIteration:
            raise(ValueError("Could not find ssl private key file in %s" % sslDir))
        return (os.path.join(sslDir, certFileName),
                os.path.join(sslDir, privateKeyFileName))

            
class BrowserInteractorThread(threading.Thread):
    '''
    Thread responsible for servicing requests coming in from
    the Web UI. Requests are fed through to this thread via 
    a queue that is passed in to __init__()
    
    Return strings are delivered to the main thread with scheduling
    a Tornado callback to _write_to_browser().
    '''

    # How often to check whether someone called stop()
    # on this thread:    
    CHECK_DONE_PERIOD = 1 # second
    
    def __init__(self, websocket_comm_obj, browser_request_queue):
        '''
        Start service of requests from the Web UI.
        
        :param websocket_comm_obj: instance of BusTesterWebController in the main thread.
        :type websocket_comm_obj: BusTesterWebController
        :param browser_request_queue: message queue from which requests from the Web UI
            are passed into this instance.
        :type browser_request_queue: Queue.Queue
        '''
        super(BrowserInteractorThread, self).__init__(name='BrowserReqServerThread')
        
        self.websocket_comm_obj = websocket_comm_obj
        self.browser_request_queue = browser_request_queue
        self.bus = JsBusBridge.bus

        # Queues through which other threads provide
        # messages that arrive from the SchoolBus to this instance
        # for transfer over the websocket to the browser:
        
        self.bus_msg_queue = None
        self.bus_stats_queue = None
        
        # Callback for bus adapter to deliver an incoming bus
        # msg to this thread:
        self.bus_msg_delivery = functools.partial(self.on_bus_message)
        
        # Callback in parent that safely writes to websocket:
        self._write_to_browser_callback = functools.partial(self.websocket_comm_obj._write_to_browser)
        
        # Create a periodic callback that checks the in-msg and in-stats
        # queues for msgs/stats to forward to the browser. The
        # instance() method enforces a singleton ioloop instance:
        
        self.periodic_callback = tornado.ioloop.PeriodicCallback(functools.partial(self.on_bus_message),
                                                                 JsBusBridge.PERIODIC_IN_MSG_CHECK

                                                                 )
        self.done = False
        self.periodic_callback.start()
        
    def stop(self):
        '''
        Stop the Web UI request servicing, cleaning up all underlying
        threads.
        '''

        # Stop the checks for incoming messages:
        if self.periodic_callback is not None:
            self.periodic_callback.stop()
            
        self.done = True
        # Immediately unblock the queue
        self.websocket_comm_obj.browser_request_queue.put_nowait('\0')
        
        if self.server_running():
            self.my_server.stop()

    def write_to_browser(self, msg):
        '''
        Send a message to the browser on the other end
        of the websocket. You may call this method from
        any thread. It schedules the actual write to happen
        during the next ioloop iteration, which is a 
        threadsafe procedure. Always use this method to 
        write to the websocket, don't use self.write_message 
        anywhere.
        
        :param msg: text to send
        :type msg: string
        '''
        tornado.ioloop.IOLoop.instance().add_callback(self._write_to_browser_callback, msg)

    def run(self):
        
        while not self.done:
            
            try:
                # Wait for requests from the Web UI:
                msg_dict = self.browser_request_queue.get(DO_BLOCK, BrowserInteractorThread.CHECK_DONE_PERIOD)
                # Double check: another way to release
                # the block of this queue is to feed it a '\0':
                if msg_dict == '\0':
                    continue
            except Queue.Empty:
                continue 
            
            cmd = msg_dict.get('cmd', None)
            if cmd is None:
                self.return_error("Protocol error: no 'cmd' field.")
                continue
            if cmd not in JsBusBridge.IMPLEMENTED_CMDS:
                self.return_error("Not implemented: %s" % cmd)
                continue
    
            # Workhorse for servicing the requests:
            response_dict = self.service_browser_request(msg_dict) #@UnusedVariable
            if response_dict is not None and len(response_dict) > 0:
                self.write_to_browser(json.dumps(response_dict))
            continue
        return
        
    def service_browser_request(self, msg_dict):
        '''
        Given a dict of request key/values from the Web UI,
        set parameters in the test instrument, or return the
        current parameter values, if the values from the UI
        are empty strings. The following example would
        set the topic to which 'one-shot' messages are sent
        to 'myTopic,' and the content of one-shots to a
        string. Given the empty string for strLen, the 
        return dict will contain the current instrument
        length of random strings:
        
        Example:  { 'cmd'     : 'subscribe',
                    'topic'   : 'myTopic',
                   }
        returns None if nothing is to be returned to the
        browser. Else returns a dict to be sent JSON encoded.
        If error occurs, schedules error message to be sent to
        browser, and returns None.
        
        Assumes that a cmd field is present, and that it contains
        one of the implemented cmds.
        
        :param msg_dict: parameter names and values
        :type msg_dict: {str : str}
        :returns None or dict to send back to browser.
        :rtype {str : str}
        '''
        try:
            
            cmd = msg_dict['cmd']
            if cmd == 'subscribe':
                topic = msg_dict.get('topic', None)
                if topic is None:
                    self.return_error("Subscription cmd requires a topic parameter.")
                    return None
                self.bus.subscribeToTopic(topic, self.bus_msg_delivery)
                return None
            elif cmd == 'unsubscribe':
                topic = msg_dict.get('topic', None)
                if topic is None:
                    self.return_error("Unsubscribe cmd requires a topic parameter.")
                    return None
                self.bus.unsubscribeFromTopic(topic)
                return None
            elif cmd == 'publish':
                topic = msg_dict.get('topic', None)
                if topic is None:
                    self.return_error("Publish cmd requires a topic parameter.")
                    return None
                msg = msg_dict.get('msg', None)
                if topic is None:
                    self.return_error("Publish cmd requires a message string in field 'msg'.")
                    return None
                bus_msg = BusMessage(content=msg, topicName=topic)
                self.bus.publish(bus_msg)
                return None
            elif cmd == 'subscribed_to':
                subscription_arr = self.bus.mySubscriptions()
                return {"resp" : subscription_arr}
            else:
                return None
        except Exception as e:
            err_msg = "Error while interacting with bus: %s" % `e`
            self.logErr(err_msg)
            self.return_error(err_msg)

    def on_bus_message(self, msg):
        '''
        Called when a message to which we are subscribed
        comes in. These are msgs on topics explicitly subscribed
        to via the Web UI.
        
        :param msg: message to write to browser.
        :type msgs: {string | None}
        '''
        try:
            self.write_to_browser({"resp" : msg.content, 
                                   "topic" : msg.topicName,
                                   "time" : msg.isoTime
                                   })
        except Exception as e:
            error_msg = "Error during in-msg forwarding: '%s'" % `e`
            self.logErr(error_msg)
            self.return_error(error_msg)
            return
        
    def return_error(self, error_str):
        '''
        Creates {'error' : <error_str>} and causes
        that dict to be  to be written to the browser 
        via the standing websocket.
        
        :param error_str: error message
        :type error_str: str
        '''
        response_dict = {"error" : error_str}
        self.write_to_browser(json.dumps(response_dict))

    def ensure_lower_non_array(self, val):
        '''
        Given a string, or array of strings, return
        an array (possibly containing just the passed-in
        string), in which all strings are lower case:
        
        :param val: input value
        :type val: {str | [str]}
        '''
        if type(val) == list:
            new_val = val[0].lower()
        else:
            new_val = val.lower()
        return new_val
        
    def logInfo(self, msg):
        self.websocket_comm_obj.logInfo(msg)

    def logErr(self, msg):
        self.websocket_comm_obj.logErr(msg)

    def logDebug(self, msg):
        self.websocket_comm_obj.logDebug(msg)


def makeApp():
    '''
    Create the tornado application, making it 
    callable via https://myServer.stanford.edu:<port>/schoolbus
    '''
    handlers = [
         (r"/schoolbus", JsBusBridge)
         ]

    application = tornado.web.Application(handlers , debug=False)
    
    return application


# Note: function not method:
def sig_handler(sig, frame):
    # Schedule call to shutdown, so that all ioloop
    # related calls are from main thread:
    #****** 
    print('sig handler called')
    #****** 
    tornado.ioloop.IOLoop.instance().add_callback(shutdown) 

# Note: function not method:
def shutdown():
    '''
    Carefully shut everything down.
    '''

    io_loop = tornado.ioloop.IOLoop.instance()
    # Schedule the shutdown for after all pending
    # requests have been services:
    io_loop.add_callback(io_loop.stop)

def is_running(process):
    '''
    Return true if Linux process with given name is
    running.
    
    :param process: process name as appears in ps -axw
    :type process: string
    '''
    search_proc = subprocess.Popen(['ps', 'axw'],stdout=subprocess.PIPE)
    for ps_line in search_proc.stdout:
        if re.search(process, ps_line):
            return True 
    return False


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), 
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s', '--server',
                        dest='bushost', 
                        help="SchoolBus server (same as the redis-server); default localhost.",
                        default='localhost')

    args = parser.parse_args();
    bus_server = args.bushost

    # Catch SIGTERM (cnt-C):
    signal.signal(signal.SIGTERM, sig_handler)

    application = makeApp()
    
    (certFile,keyFile) = JsBusBridge.getCertAndKey()
    sslArgsDict = {'certfile' : certFile,
                   'keyfile'  : keyFile}

    # For SSL:
    ssl_used = True
    http_server = tornado.httpserver.HTTPServer(application,ssl_options=sslArgsDict)
    application.listen(JS_BRIDGE_WEBSOCKET_PORT, ssl_options=sslArgsDict)
    
    
    # For non-ssl:
    #ssl_used = False
    #http_server = tornado.httpserver.HTTPServer(application)
    #http_server.listen(BUS_TESTER_WEBSOCKET_PORT)

    if ssl_used:
        protocol_spec = 'wss'
    else:
        protocol_spec = 'ws'

    JsBusBridge.bus = BusAdapter(host=bus_server)

    start_msg = 'Starting JavaScript/SchoolBus bridge server on %s://%s:%d/schoolbus/' % \
        (protocol_spec, socket.gethostname(), JS_BRIDGE_WEBSOCKET_PORT)

    print(start_msg)
    
    try:
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except KeyboardInterrupt:
        shutdown()
    except Exception as e:
        print('Bombed out of tornado IO loop: %s' % `e`)
    
    print('School bus test server has shut down.')
