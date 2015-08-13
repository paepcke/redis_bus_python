#!/usr/bin/env python

'''
Created on Aug 1, 2015

@author: paepcke

TODO:
    o Kill server after alloted time 
    o JS inter-msg time: in web_app_server_control
    o Additiona servers
    o Initial submit button 'push'
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
import signal
import sys
import threading
import time
import tornado.httpserver
import tornado.ioloop
import tornado.web
from  tornado.websocket import WebSocketHandler
import uuid

from redis_bus_python.bus_message import BusMessage
from schoolbus_test_server import OnDemandPublisher


sys.path.append(os.path.dirname(__file__))




#****from schoolbus_test_server.tornado.websocket import WebSocketHandler
#**********
BUS_TESTER_SERVER_PORT = 8000
#BUS_TESTER_SERVER_PORT = 80
#**********

class BusTesterWebController(WebSocketHandler):
    '''
    This class is a Web service that listens to GET
    requests. The parameters control one or more
    schoolbus_test_server.py instances.
    
    uid : <some unique user token>
    start_server: {1 | 0}
    session_len : <int-hrs>
    kill_server : {1 | 0}
    stream_topic : {<topic> | None}
    stream_content : {content> | None}
    one_shot_topic : {<topic> | None}
    one_shot_content : {<content> | None}
    syntax_check : {<msg_str> | None}
    def_msg_len : {<int> | None} 
    '''

    # ----------------------------- Constants ---------------------
    # Number of seconds after which test server
    # instances get killed, because we assume that
    # the browser that created that server is
    # abandoned:
    
    DEFAULT_TIME_TO_LIVE = 2.0 * 3600.0 # hrs * sec/hr
    
    LOG_LEVEL_NONE  = 0
    LOG_LEVEL_ERR   = 1
    LOG_LEVEL_INFO  = 2
    LOG_LEVEL_DEBUG = 3

    # ----------------------------- Class Variables ---------------------
    
    # UUID --> (OnDemandPublisher, kill_time):
    test_servers = {}
    
    # Class variable to hold all instances of
    # this class (i.e. of BusTesterWebController).
    # Needed so that the periodic callback from the
    # tornado ioloop to function (not method) check_for_bus_msgs()
    # can find all  BusTesterWebController instances to
    # call on_bus_message() on. Those methods will then check
    # their queue that holds the in-messages captured by
    # their OnDemandPublishers:
     
    bus_testers = []
    
    def __init__(self, application, request, **kwargs):
        self.title = "SchoolBus Tester"
        super(BusTesterWebController, self).__init__(application, request, **kwargs)
        
        self.loglevel = BusTesterWebController.LOG_LEVEL_DEBUG
        #self.loglevel = BusTesterWebController.LOG_LEVEL_INFO
        #self.loglevel = BusTesterWebController.LOG_LEVEL_NONE
        
        # This instance is created to serve one request
        # from a browser. If that browser never called
        # before, my_server() will invent a UUID,
        # create an OnDemandPublisher instance, and save
        # it in the class var test_servers as value of the UUID key. 
        # If the browser called before, the request will include a UUID
        # with which to retrieve the already existing OnDemandPublisher:
        
        self.test_server_id = None
        
        self.msg_queue = None
        self.stats_queue = None
        
        # Protection against re-entry into tornado's write_message():
        self.browser_write_lock = threading.Lock()

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
        
     
    def on_close(self):
        self.logDebug('Websocket was closed; shutting down school test server...')
        try:
            (server, kill_time) = BusTesterWebController.test_servers[self.test_server_id] #@UnusedVariable
            server.stop()
            server.join(2)
        except Exception as e:
            print('Could not shut school test server down: %s' % `e`)
            
        
    def on_message (self, msg):
        
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
        
        # Check whether browser included a test server id; if 
        # such an ID is absent, then a new server will be 
        # created as soon as my_server() is called below. But
        # if the browser does pass a server id, my_server() will
        # find and use the existing one:
        
        server_id_in_req =  msg_dict.get('server_id', '')
        self.test_server_id = None if len(server_id_in_req) == 0 else server_id_in_req
        # Remove the server_id from the reqDict b/c we
        # are taking care of it here:
        try:
            del(msg_dict['server_id'])
        except:
            pass
        
        # Special command: killServer takes a server ID, which hopefully
        # keys to a running schoolbus test server (OnDemandPublisher).
        # Find that server, close it, and return:
        
        if msg_dict.get('killServer', None) is not None:
            self.kill_a_server(msg_dict['killServer'])
            self.write_to_browser(json.dumps({'success' : 'OK'}))
            return 
        
        if self.test_server_id is None or len(self.test_server_id) == 0:
            # First contact by this browser tab; 
            # Have my_server() create an OnDemandPublisher,
            # and associate it with a UUID in test_servers:
            self.my_server

        # Turn string values 'True', 'False', 'on', 'off' into bools:
        chkSyntax = msg_dict.get('chkSyntax', None)
        echo      = msg_dict.get('echo', None)
        streaming = msg_dict.get('streaming', None)
        
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
            return

        response_dict = {}
        
        # Init the server UUID in the response:
        response_dict['server_id'] = self.test_server_id
        
        #******** Exploring problem of hanging sometimes
        #         when (un)subscribing while publishing:
#         response_dict['server_id'] = self.test_server_id
#         self.write_to_browser(json.dumps(response_dict))
#         print ('got one; stream')
#         self.my_server.streaming = True
#         time.sleep(2)
#         self.my_server.streaming = False
#         print ('got one; stop stream')
#         # Danger!!!!:
#         
#         print('Stream off')
#         self.my_server.streaming = False
#         print('Echo off')
#         self.my_server.serve_echo = False
#         print('Echo on')
#         self.my_server.serve_echo = True
#         print('Stream on')
#         self.my_server.streaming = True
#         
#         print('Syntax off')
#         self.my_server.check_syntax = False
#         self.my_server.check_syntax = True
#         print('Syntax on')
# 
#         print('Getting constants')
#         self.my_server.standard_msg_len
#         self.my_server.one_shot_topic
#         self.my_server.one_shot_content
#         self.my_server.stream_topic
#         self.my_server.stream_content
#         self.my_server.syntax_check_topic
#         print('Finished getting')
#         
#         return
        #********
        
        
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
            
            #******
            # If a real HTML page were to be sent,
            # we'd close it out here:
            # self.write("</body></html>")
            #******
        except ValueError:
            # Was handled in one of the functions called above:
            return 
        except Exception as e:
            print('Exception in GET: %s' % `e`)
            self.return_error(response_dict, `e`);
            #*********
            raise
            #*********
            return
        
    def on_bus_message(self, msg):
        '''
        Called when a message to which we are subscribed
        comes in. These are msgs on topics explicitly subscribed
        to via the Web UI.
        
        :param msg: message to write to browser.
        :type msgs: string
        '''
        try:
            self.write_to_browser({"inmsg" : msg + '\r\n'})
        except Exception as e:
            self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)              

    def on_stats_message(self, msg):
        '''
        Called when stats about messages to which we are subscribed
        come in. These are msgs on topics explicitly subscribed
        to via the Web UI.
        
        :param msg: stats message to write to browser.
        :type msgs: string
        '''
        try:
            self.write_to_browser({"instat" : msg + '\r\n'})
        except Exception as e:
            self.logErr("Error during read of msg or stats queue from OnDemandServer: '%s'" % `e`)              

    def return_error(self, response_dict, error_str):
        response_dict['error'] = error_str
        self.write_to_browser(json.dumps(response_dict))

    def write_to_browser(self, msg):
        with self.browser_write_lock:
            self.write_message(msg)

    def get_or_set_server_parm(self, parm_name, parm_val, response_dict):
        
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
                   parm_name != 'streamContent':
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
            # browser, and closed the connection via
            # finish(), which happens in return_error()
            raise
            
        return response_dict

    @property
    def my_server(self):
        
        # Did the request from the browser that
        # created this instance include a test
        # server UID? If so, there already is
        # an OnDemandPublisher instance for that
        # browser:
        
        if self.test_server_id is not None:
            (server, time_to_kill) = BusTesterWebController.test_servers[self.test_server_id] #@UnusedVariable
            return server
        
        # Create an OnDemandPubliser that will stream on
        # the standard topic/content; store it in the
        # class var topic_servers in a two-tuple:
        #   (serverInstance, timeToKill)
        self.test_server_id = uuid.uuid1().hex
         
        server = OnDemandPublisher(streamMsgs=(None,None))
        
        # Queues into which the server will place msgs that
        # arrive on topics subscribed to through the Web UI:
        self.msg_queue = server.msg_queue
        self.stats_queue = server.stats_queue
        
        # Start a thread that hangs on msg and stats queues
        # of this new OnDemandPublisher:
        BusInMsgChecker(self, self.msg_queue, self.stats_queue).start()
        
        # If this instance of BusTesterWebController isn't
        # in the list of BusTesterWebController instances,
        # add it now that we have msg and stats queues; that
        # the periodic call to on_bus_message() via check_for_bus_msgs()
        # has queues to look at:
        if self not in BusTesterWebController.bus_testers:
            BusTesterWebController.bus_testers.append(self)
        
        
        BusTesterWebController.test_servers[self.test_server_id] = \
            (server, time.time() + BusTesterWebController.DEFAULT_TIME_TO_LIVE)
            
        # Before starting the server, pause the message streaming server:
        server.streaming = False
        server.start()
        server.serve_echo = True
        server.check_syntax = True
        return server
        
    def kill_a_server(self, serverId):
        (server_to_kill, kill_time) = self.test_servers.get(serverId, (None, None)) #@UnusedVariable
        if server_to_kill is None:
            # Browser app passed a server id that we don't know about:
            self.return_error({}, "Warning: attempt to kill server '%s', which is not running." % str(serverId))
            raise ValueError("Warning: attempt to kill server '%s', which is not running.")
        if server_to_kill.is_running():
            server_to_kill.stop()
            server_to_kill.join(2)
            del self.test_servers[serverId]
                                       
    def server_running(self):
        try:
            return self.test_servers[self].running
        except Exception:
            return False 
        

    def ensure_lower_non_array(self, val):
        if type(val) == list:
            new_val = val[0].lower()
        else:
            new_val = val.lower()
        return new_val
        
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
    def shutdown(cls, signum, frame):
        for server in BusTesterWebController.test_servers:
            #*********
            print("calling stop on %s" % str(server))
            #*********
            server.stop()
            server.join(2)
            sys.exit()

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


    @classmethod  
    def makeApp(self):
        '''
        Create the tornado application, making it 
        callable via http://myServer.stanford.edu:<port>/bus
        '''
#*****************
#         handlers = [
#             (r"/controller", BusTesterWebController),
#             (r"/bus/(.*)", tornado.web.StaticFileHandler, {'path' : './html',  "default_filename": "index.html"}),
#             ]
        handlers = [
            (r"/controller", BusTesterWebController),
            ]
#*****************

        application = tornado.web.Application(handlers , debug=False)
        
        return application

class BusInMsgChecker(threading.Thread):
    '''
    Thread that hangs on one OnDemandPublisher instance's
    incoming messages/stats queues. When an in message arrives,
    this thread instance calls on_bus_message() on the 
    BusTesterWebController instance that started this thread
    instance.
    '''
    def __init__(self, bus_tester, in_msg_queue, in_stats_queue):
        super(BusInMsgChecker, self).__init__()
        
        self.bus_tester = bus_tester
        self.in_msg_queue = in_msg_queue
        self.in_stats_queue = in_stats_queue
        self.done = False
        # Die when parent dies:
        self.daemon = True
        
    def stop(self):
        self.done = True
        
    def run(self):
        while not self.done:
            try:
                msg = self.in_msg_queue.get(1.0) # second
                #********self.bus_tester.on_bus_message(msg)
                ioloop.add_callback(self.bus_tester.on_bus_message, msg)
            except Queue.Empty:
                if self.done:
                    return
            while not self.in_stats_queue.empty():
                #******self.bus_tester.on_bus_stats(self.in_stats_queue.get())
                stats = self.in_stats_queue.get()
                ioloop.add_callback(self.bus_tester.on_bus_stats, stats)

signal.signal(signal.SIGINT, BusTesterWebController.shutdown)

if __name__ == "__main__":

    application = BusTesterWebController.makeApp()
    
    # Starting multiple Python processes on multiple cores:
#     server = tornado.httpserver.HTTPServer(application)
#     server.bind(BUS_TESTER_SERVER_PORT)
#     server.start(0)  # Forks multiple sub-processes
#     print('Starting SchoolBus test server and Web controller on port %d' % BUS_TESTER_SERVER_PORT)
#     tornado.ioloop.IOLoop.instance().start()
    
    (certFile,keyFile) = BusTesterWebController.getCertAndKey()
    sslArgsDict = {'certfile' : certFile,
                   'keyfile'  : keyFile}

    http_server = tornado.httpserver.HTTPServer(application,ssl_options=sslArgsDict)

    application.listen(8000, ssl_options=sslArgsDict)
    
    
    # For non-ssl:
    # ****http_server = tornado.httpserver.HTTPServer(application)
    # ****http_server.listen(BUS_TESTER_SERVER_PORT)

    print('Starting SchoolBus test server and Web controller on port %d' % BUS_TESTER_SERVER_PORT)
    try:
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except Exception as e:
        print('Bombed out of tornado IO loop: %s' % `e`)
    