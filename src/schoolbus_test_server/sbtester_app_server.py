#!/usr/bin/env python

'''
Created on Aug 1, 2015

@author: paepcke

TODO:
    o Documentation: mention what server should return
        in returned dict, incl. 
            'error', 'success', 'inmsg', 'stats'

'''

import Queue
from __builtin__ import True
import datetime
import functools
import json
import os
import re
import socket
from subprocess import Popen
import subprocess
import sys
import threading
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.websocket import WebSocketHandler, WebSocketClosedError
import traceback

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_lib.exceptions import TimeoutError
from schoolbus_test_server import OnDemandPublisher


sys.path.append(os.path.dirname(__file__))

BUS_TESTER_WEBSOCKET_PORT = 8001

DO_BLOCK = True

# Time to wait in join() for child threads to 
# terminate:
JOIN_WAIT_TIME = 5 # sec


class BusTesterWebController(WebSocketHandler):
    '''
    This class is a Web service that controls a
    schoolbus test instrument: OnDemandPublisher.
    The UI is index.html, in conjunction with sbtester.js
    and sbtester.css.
    
    The HTML form provides input elements that are
    sent via a Websocket between an instance of this
    class and the browser.
    
	     * streaming        : whether or not to stream continuously
	     * streamInterval   : time between streamed messages
	     * echo             : whether to listen on 'echo', and echo msgs back on tmp.*
	     * chkSyntax        : whether to listen on bus_syntax, and return syntax check on tmp.*
	     * strLen           : length of random strings optionally used for one-shot and streaming msgs.
	     * oneShotTopic     : topic to which 'one-shot' messages are sent
	     * streamTopic      : topic to which message streams are sent
	     * streamContent    : content of stream messages
	     * syntaxTopic      : topic to which syntax check requests are sent (default bus_syntax)
	     * topicsToRx       : topics to receive and display on the tester instrument Web UI 
	     '''

    # ----------------------------- Class-Level Constants ---------------------
    
    # Periodic check for incoming messages to forward to browser:
    
    PERIODIC_IN_MSG_CHECK = 100 # milliseconds
    
    LOG_LEVEL_NONE  = 0
    LOG_LEVEL_ERR   = 1
    LOG_LEVEL_INFO  = 2
    LOG_LEVEL_DEBUG = 3

    # ----------------------------- Class Variables ---------------------
    
    instantiation_lock = threading.Lock()

    # ----------------------------- Methods ---------------------
    
    def __init__(self, application, request, **kwargs):
        
        with BusTesterWebController.instantiation_lock:
            self.title = "SchoolBus Tester"
            super(BusTesterWebController, self).__init__(application, request, **kwargs)
            
            self.loglevel = BusTesterWebController.LOG_LEVEL_DEBUG
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
        if self.loglevel >= BusTesterWebController.LOG_LEVEL_INFO:
            print(str(datetime.datetime.now()) + ' info: ' + msg)

    def logErr(self, msg):
        if self.loglevel >= BusTesterWebController.LOG_LEVEL_ERR:
            print(str(datetime.datetime.now()) + ' error: ' + msg)

    def logDebug(self, msg):
        if self.loglevel >= BusTesterWebController.LOG_LEVEL_DEBUG:
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
    the Web UI. The requests are controls for the test instrument.
    Requests are fed through a queue that is passed in to __init__()
    
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
        self.bus_service = None

        # Queues through which other threads provide
        # messages that arrive from the SchoolBus to this instance
        # for transfer over the websocket to the browser:
        
        self.bus_msg_queue = None
        self.bus_stats_queue = None
        
        # Callback in parent that safely writes to websocket:
        self._write_to_browser_callback = functools.partial(self.websocket_comm_obj._write_to_browser)
        
        # Create a periodic callback that checks the in-msg and in-stats
        # queues for msgs/stats to forward to the browser. The
        # instance() method enforces a singleton ioloop instance:
        
        self.periodic_callback = tornado.ioloop.PeriodicCallback(functools.partial(self.on_bus_message),
                                                                 BusTesterWebController.PERIODIC_IN_MSG_CHECK

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
        
            # Turn string values 'True', 'False', 'on', 'off' into bools:
            chkSyntax = msg_dict.get('chkSyntax', None)
            echo      = msg_dict.get('echo', None)
            streaming = msg_dict.get('streaming', None)
            
            # Normalize the checkbox values to Python booleans:
            if type(chkSyntax) == str:
                msg_dict['chkSyntax'] = True if chkSyntax.lower() == 'true' or chkSyntax == 'on' else False
            if type(echo) == str:            
                msg_dict['echo'] = True if echo.lower() == 'true' or echo == 'on' else False
            if type(streaming) == str:            
                msg_dict['streaming'] = True if streaming.lower() == 'true' or streaming == 'on' else False
                
            # Ensure that streamInterval is a float:
            try:
                interval = msg_dict.get('streamInterval', None)
                # If empty string, indicating request for current
                # value, the call to get_or_set_server_parm() will
                # take care of it. But otherwise, floatify:
                if interval != '':
                    msg_dict['streamInterval'] =  float(interval)
            except (ValueError, TypeError):
                self.return_error({}, "Received a non-float for streamInterval from browser: '%s'" % str(interval))
                continue
    
            # Workhorse for servicing the requests:
            response_dict = self.service_browser_request(msg_dict) #@UnusedVariable
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
        
        Example:  {'oneShotTopic'   : 'myTopic',
                   'oneShotContent' : 'my oneshot message content',
                   'strLen'         : ''
                   }
        
        :param msg_dict: parameter names and values
        :type msg_dict: {str : str}
        '''
        
        response_dict = {}
        try:
            
            # Are we to fire a one-shot message?
            if msg_dict.get('oneShot', None) is not None:
                one_shot_msg = BusMessage(topicName=self.my_server['oneShotTopic'], content=self.my_server['oneShotContent'])
                self.my_server.testBus.publish(one_shot_msg)
                response_dict['success'] = 'OK'
                self.write_to_browser(json.dumps(response_dict))
                return
            
            # Go through each server parm in the request dict,
            # and update the server to the respective value;
            # also fill the response_dict with the new value.
            # One UI optimization: do the "length of random string"
            # first, because if subsequent one-shot content or 
            # message-stream content fields are to be changed,
            # They will be the correct length. Without this 
            # order switch, the user needs to hit submit twice:
            
            if msg_dict.get('strLen', None) is not None:
                response_dict = self.get_or_set_server_parm('strLen', msg_dict['strLen'], response_dict)
                del msg_dict['strLen']
                
            for (parm_key, parm_value) in msg_dict.items():
                response_dict = self.get_or_set_server_parm(parm_key, parm_value, response_dict)
            
            # Send a dict with keys being the HTML parameter
            # names, and values being the current server
            # parameter values:
            self.write_to_browser(json.dumps(response_dict))
            
        except ValueError:
            # Was handled in one of the functions called above:
            #******
            raise
            #******
            return response_dict 
        except Exception as e:
            print('Exception in GET: %s' % `e`)
            self.return_error(response_dict, `e`);
            #*********
            raise
            #*********
            return response_dict
        
    def on_bus_message(self, msg=None):
        '''
        Called when a message to which we are subscribed
        comes in. These are msgs on topics explicitly subscribed
        to via the Web UI. If msg is None, check self.bus_msg_queue
        for newly arrived msgs from the bus:
        
        :param msg: message to write to browser.
        :type msgs: {string | None}
        '''
        
        if msg is None:
            # If message/stats queues have not been initialized
            # yet, just return:
            if self.bus_msg_queue is None:
                return
            # We check the msg queue for messages:
            while not self.bus_msg_queue.empty():
                msg = self.bus_msg_queue.get_nowait()
                try:
                    self.write_to_browser({"inmsg" : msg + '\r\n'})
                except Exception as e:
                    self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)
                    return
        else:
            try:
                self.write_to_browser({"inmsg" : msg + '\r\n'})
            except Exception as e:
                self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)
                return
            
        # Check for new bus statistics to forward to browser:        
        self.on_bus_stats()

    def on_bus_stats(self, msg=None):
        '''
        Called when stats about messages to which we are subscribed
        come in. These are msgs on topics explicitly subscribed
        to via the Web UI. If msg is None, check self.bus_stats_queue
        for newly arrived msgs from the bus:
        
        :param msg: stats message to write to browser.
        :type msgs: {string | None}
        '''
        
        if msg is None:
            # We check the stats queue for messages:
            while not self.bus_stats_queue.empty():
                msg = self.bus_stats_queue.get_nowait()
                try:
                    self.write_to_browser({"instat" : msg + '\r\n'})
                except Exception as e:
                    self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)              
                    return
        else:
            try:
                self.write_to_browser({"instat" : msg + '\r\n'})
            except Exception as e:
                self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)              
                return
            
    def return_error(self, response_dict, error_str):
        '''
        Add entry {'error' : <error_str>} to the 
        passed-in response_dict, and cause
        that dict to be  to be written to the browser via the standing
        websocket.
        
        :param response_dict: dict with parameter name/values
            that worked before the error occurred.
        :type response_dict: {str : str}
        :param error_str: error message
        :type error_str: str
        '''
        response_dict['error'] = error_str
        self.write_to_browser(json.dumps(response_dict))

    def get_or_set_server_parm(self, parm_name, parm_val, response_dict):
        '''
        Given one parm/value pair, get or set the 
        corresponding parameter in the test instrument.
        
        :param parm_name: name of instrument parameter
        :type parm_name: str
        :param parm_val: new value or empty string, if current value is requested
        :type parm_val: str
        :param response_dict: current response dict. This method will add
            to the passed-in dict, and return the augmented dict.
        :type response_dict: {str : str}
        :return: augmented result dict destined for the browser once all
            requests have been processed.
        :rtype: {str : str}
        '''
        
        # Most parameters are singleton arrays.
        # Make those into non-arrays:
        if type(parm_val) == list and len(parm_val) == 1:
            parm_val = parm_val[0]
            
        try:
            # Is this a request for the current value of the server setting?
            # Special cases: oneShotContent and echoContent: setting
            # values of length 0 means: set to default strings of standard
            # length, so the next branch of this conditional is the
            # one to take for these cases; other zero-length values
            # indicate request for current value:
            if len(str(parm_val)) == 0 and \
                   self.my_server is not None and \
                   parm_name != 'oneShotContent' and \
                   parm_name != 'echoContent' and \
                   parm_name != 'streamContent' and \
                   parm_name != 'topicsToRx':
                # Return current value:
                response_dict[parm_name] =  self.my_server[parm_name]
                return response_dict
            
            # We are to set the server parm:
            if self.my_server is not None:
                # Only change if different:
                if self.my_server[parm_name] != parm_val:
                    
                    #********
                    # Because of the instability of subscribe/unsubscribe
                    # during stream publishing: pause streaming if it's
                    # on:
                    was_streaming = False
                    if parm_name != 'streaming' and self.my_server.streaming:
                        was_streaming = True
                        self.my_server.streaming = False
                    #********
                    
                    self.my_server[parm_name] = parm_val
                    
                    #************
                    # Turn streaming back on if it was happening:
                    if was_streaming:
                        self.my_server.streaming = True
                    #************
                    
                    # Read value back out from the server,
                    # b/c the setters may modify values (e.g.
                    # entering defaults when strings are empty):
                    response_dict[parm_name] =  self.my_server[parm_name]
                else:
                    response_dict[parm_name] =  parm_val
            else:
                # It's a request for current value, but no server is running;
                # report back to the browser, and close the connection:
                self.return_error('SchoolBus server is not running.')
                # The following will be ignored in the POST method:
                raise ValueError('SchoolBus server is not running.')
        except KeyError:
            self.return_error(response_dict, "Server parameter '%s' does not exist" % parm_name)
            raise ValueError("Server parameter '%s' does not exist" % parm_name)
        except ValueError as e:
            self.return_error(response_dict, `e`)
            # Re-raising ValueError ensures that caller
            # knows we already returned an error to the
            # browser
            raise
            
        return response_dict

    @property
    def my_server(self):
        '''
        Return an OnDemandServer bus server singleton.
        '''
        
        if self.bus_service is not None:
            return self.bus_service
        
        # Create an OnDemandPubliser that will stream on
        # the standard topic/content:
         
        self.bus_service = OnDemandPublisher(streamMsgs=(None,None))
        
        # Queues into which the server will place msgs that
        # arrive on topics subscribed to through the Web UI:
        self.bus_msg_queue = self.bus_service.bus_msg_queue
        self.bus_stats_queue = self.bus_service.bus_stats_queue
        
        # Before starting the bus server, pause its message streaming server:
        self.bus_service.streaming = False
        self.bus_service.start()
        self.bus_service.serve_echo = True
        self.bus_service.check_syntax = True
        return self.bus_service
        
    def server_running(self):
        '''
        Return True/False for whether the underlying 
        OnDemandPublisher instance, i.e. the test instrument
        is running.
        :return: test instrument running status
        :rtype: bool
        '''
        try:
            return self.bus_service is not None and self.bus_service.running
        except Exception:
            return False 

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
    callable via http://myServer.stanford.edu:<port>/bus
    '''
    handlers = [
         (r"/controller", BusTesterWebController)
         ]

    application = tornado.web.Application(handlers , debug=False)
    
    return application


# Note: function not method:
def sig_handler(sig, frame):
    # Schedule call to shutdown, so that all ioloop
    # related calls are from main thread: 
    tornado.ioloop.IOLoop.instance().add_callback(shutdown) 

# Note: function not method:
def shutdown():
    '''
    Carefully shut everything down.
    '''
    
    # Time for ioloop to shut everything down:
    MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3

    for server in BusTesterWebController.test_servers:
        print("Stopping %s" % str(server))
        server.stop()
        server.join(5)
        if server.is_alive():
            raise TimeoutError("Unable to stop BusTesterWebController thread '%s'." % server.name)
            raise TimeoutError('Unable to stop BusTesterWebController thread.')
        

    global http_server
    print("Stopping HTTP server...")
    http_server.stop()
    
    print('Shutdown in %s seconds ...' % MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
    io_loop = tornado.ioloop.IOLoop.instance()

    deadline = time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN

    def stop_loop():
        now = time.time()
        if now < deadline and (io_loop._callbacks or io_loop._timeouts):
            io_loop.add_timeout(now + 1, stop_loop)
        else:
            io_loop.stop()
            print('Shutdown complete.')
    stop_loop()    
    

#****signal.signal(signal.SIGINT, sig_handler)

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

    # Make sure the UI content server is running:
    if not is_running('sbtester_content_server.py'):
        content_server_pid = Popen(['python', 'src/schoolbus_test_server/sbtester_content_server.py']).pid

    application = makeApp()
    
    # Starting multiple Python processes on multiple cores:
#     server = tornado.httpserver.HTTPServer(application)
#     server.bind(BUS_TESTER_WEBSOCKET_PORT)
#     server.start(0)  # Forks multiple sub-processes
#     print('Starting SchoolBus test server and Web controller on port %d' % BUS_TESTER_WEBSOCKET_PORT)
#     tornado.ioloop.IOLoop.instance().start()
    
    (certFile,keyFile) = BusTesterWebController.getCertAndKey()
    sslArgsDict = {'certfile' : certFile,
                   'keyfile'  : keyFile}

    # For SSL:
    ssl_used = True
    http_server = tornado.httpserver.HTTPServer(application,ssl_options=sslArgsDict)
    application.listen(BUS_TESTER_WEBSOCKET_PORT, ssl_options=sslArgsDict)
    
    
    # For non-ssl:
    #ssl_used = False
    #http_server = tornado.httpserver.HTTPServer(application)
    #http_server.listen(BUS_TESTER_WEBSOCKET_PORT)

    if ssl_used:
        protocol_spec = 'wss'
    else:
        protocol_spec = 'ws'

    start_msg = 'Starting SchoolBus Web UI websocket server on %s://%s:%d/bus/' % \
        (protocol_spec, socket.gethostname(), BUS_TESTER_WEBSOCKET_PORT)

    print(start_msg)
    
    try:
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except Exception as e:
        print('Bombed out of tornado IO loop: %s' % `e`)
    
    print('School bus test server has shut down.')
    