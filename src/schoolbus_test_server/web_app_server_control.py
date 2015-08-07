#!/usr/bin/env python

'''
Created on Aug 1, 2015

@author: paepcke

TODO:
    o Kill server after alloted time 
    o JS: one-shot button
    o Why web_app_server dies with lots of 'Submit Changes' clicking
    

'''

import datetime
import json
import signal
import time
import tornado.ioloop
import tornado.web
import uuid

from schoolbus_test_server import OnDemandPublisher
from  websocket import WebSocketHandler


#****from schoolbus_test_server.tornado.websocket import WebSocketHandler
BUS_TESTER_SERVER_PORT = 8000

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
    
    HTML_CLOSE = "</body></html>"
    
    LOG_LEVEL_NONE  = 0
    LOG_LEVEL_ERR   = 1
    LOG_LEVEL_INFO  = 2
    LOG_LEVEL_DEBUG = 3

    # ----------------------------- Class Variables ---------------------
    
    # UUID --> (OnDemandPubliser, kill_time):
    test_servers = {}

    
    def __init__(self, application, request, **kwargs):
        self.title = "SchoolBus Tester"
        super(BusTesterWebController, self).__init__(application, request, **kwargs)
        
        #self.loglevel = BusTesterWebController.LOG_LEVEL_DEBUG
        self.loglevel = BusTesterWebController.LOG_LEVEL_INFO
        #self.loglevel = BusTesterWebController.LOG_LEVEL_NONE
        
        # This instance is created to serve one request
        # from a browser. If that browser never called
        # before, my_server() will invent a UUID,
        # create an OnDemandPublisher instance, and save
        # it in the class var test_servers as value of the UUID key. 
        # If the browser called before, the request will include a UUID
        # with which to retrieve the already existing OnDemandPublisher:
        
        self.test_server_id = None

    def allow_draft76(self):
        '''
        Allow WebSocket connections via the old Draft-76 protocol. It has some
        security issues, and was replaced. However, Safari (i.e. e.g. iPad)
        don't implement the new protocols yet. Overriding this method, and
        returning True will allow those connections.
        '''
        return True

    def open(self): #@ReservedAssignment
        '''
        Called by WebSocket/tornado when a client connects. Method must
        be named 'open'
        '''
        self.logDebug("Open called")
        
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
        # are now dealing with it:
        try:
            del(msg_dict['server_id'])
        except:
            pass
        if self.test_server_id is None or len(self.test_server_id) == 0:
            # First contact by this browser tab; 
            # Have my_server() create an OnDemandPublisher,
            # and associate it with a UUID in test_servers:
            self.my_server

        # Turn 'True' and 'False' values into bools:
        msg_dict['chkSyntax'] = True if msg_dict.get('chkSyntax', None) == 'True' else False
        msg_dict['echo'] = True if msg_dict.get('echo', None) == 'True' else False
        msg_dict['streaming'] = True if msg_dict.get('streaming', None) == 'True' else False
            

        response_dict = {}
        
        # Init the server UUID in the response:
        response_dict['server_id'] = self.test_server_id
        
        #******** Exploring problem of hanging sometimes
        #         when (un)subscribing while publishing:
#         response_dict['server_id'] = self.test_server_id
#         self.write_message(json.dumps(response_dict))
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
            self.write_message(json.dumps(response_dict))
            
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

    def return_error(self, response_dict, error_str):
        response_dict['error'] = error_str
        self.write_message(json.dumps(response_dict))

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
            # one to take for these two cases; other zero-length values
            # indicate request for current value:
            if len(str(parm_val)) == 0 and \
                   self.my_server is not None and \
                   parm_name != 'oneShotContent' and \
                   parm_name != 'echoContent':
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
        
        BusTesterWebController.test_servers[self.test_server_id] = \
            (server, time.time() + BusTesterWebController.DEFAULT_TIME_TO_LIVE)
            
        # Before starting the server, pause the message streaming server:
        server.streaming = False
        server.start()
        server.serve_echo = True
        server.check_syntax = True
        return server
        
    def stopServer(self, responseDict):
        # Truly stop the server; to start again, a new
        # instance will be created, b/c the server is a
        # thread:
        if self.server_running():
            self.my_server.stop()
        self.test_servers[self] = None

        responseDict['server'] = 'OFF'
        return responseDict
    
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
            server.join()

    @classmethod  
    def makeApp(self):
        '''
        Create the tornado application, making it 
        callable via http://myServer.stanford.edu:<port>/bus
        '''

        handlers = [
            (r"/bus/controller", BusTesterWebController),
            (r"/bus/(.*)", tornado.web.StaticFileHandler, {'path' : './html',  "default_filename": "index.html"}),
            ]

        application = tornado.web.Application(handlers , debug=True)
        
        return application

signal.signal(signal.SIGINT, BusTesterWebController.shutdown)

if __name__ == "__main__":

    application = BusTesterWebController.makeApp()

    # We need an SSL capable HTTP server:
    # For configuration without a cert, add "cert_reqs"  : ssl.CERT_NONE
    # to the ssl_options (though I haven't tried it out.):

    application.listen(BUS_TESTER_SERVER_PORT)
    print('Starting SchoolBest test server and Web controller on port %d' % BUS_TESTER_SERVER_PORT)
    tornado.ioloop.IOLoop.instance().start()