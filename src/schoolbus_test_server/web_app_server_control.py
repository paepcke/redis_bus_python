'''
Created on Aug 1, 2015

@author: paepcke
'''

import json
import tornado.ioloop
import tornado.web

from schoolbus_test_server import OnDemandPublisher


BUS_TESTER_SERVER_PORT = 8000


class BusTesterWebController(tornado.web.RequestHandler):
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
    
    # String used by sbtester.js when requesting the
    # current value of a quantity, rather than setting
    # a new value:
    REQUEST_STR = '_'
    
    HTML_CLOSE = "</body></html>"

    def __init__(self, application, request, **kwargs):
        self.title = "SchoolBus Tester"
        super(BusTesterWebController, self).__init__(application, request, **kwargs)
        
        # UID --> (OnDemandPubliser, kill_time)
        self.test_servers = {}

    def get(self):
        
        html_parms = self.request.arguments
        self.write("<html><body>GET method was called: %s.<br>" %str(html_parms))
        response_dict = {}
        try:
            for (parm_key, parm_value) in html_parms.items():
                
                # Server start/stop?
                
                if parm_key == 'server':
                    
                    new_server_state = self.ensure_lower_non_array(parm_value)
                    
                    if new_server_state == 'on':
                        # Just ask for the server, and it will be started
                        # if it isn't already:
                        self.my_server
                        response_dict['server'] = 'on'
                    elif new_server_state == BusTesterWebController.REQUEST_STR:
                        # Just return whether server is currently running:
                        response_dict['server'] = self.server_running()
                    elif new_server_state == 'off':
                        response_dict = self.stopServer(response_dict)
                    else:
                        self.return_error(response_dict, "Server command must be 'on', 'off', or '%s'" % BusTesterWebController.REQUEST_STR)
                        return
                
                elif parm_key == 'pauseStream':
                    # Pause, or unpause the message stream:
                    
                    # Normalize up/lower case:
                    new_stream_state = self.ensure_lower_non_array(parm_value)
                                        
                    curr_pause_state = self.my_server.stream_paused
                    
                    if new_stream_state == BusTesterWebController.REQUEST_STR:
                        # Just wants to know current state:
                        response_dict['pauseStream'] = curr_pause_state
                    else:
                        # Wants to set state
                        
                        # Save the caller's 'ON'/'on'/'OFF'/'off':
                        orig_stream_state_req = new_stream_state
                        
                        # Turn new state into boolean:
                        new_stream_state = True if new_stream_state == 'on' else False
                        
                        # Change the stream state:
                        self.set_stream_paused_state(new_stream_state)
                        # Report success:
                        response_dict['pauseStream'] = orig_stream_state_req
                else:
                    # One of the server parameter settings/state-queries: topics or contents, etc.:
                    response_dict = self.get_or_set_server_parm(parm_key, parm_value, response_dict)
    
            self.write(json.dumps(response_dict))
            self.write("</body></html>")
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
        self.write(json.dumps(response_dict))
        self.write(BusTesterWebController.HTML_CLOSE)

    def get_or_set_server_parm(self, parm_name, parm_val, response_dict):
        
        # Most parameters are singleton arrays.
        # Make those into non-arrays:
        if len(parm_val) == 1:
            parm_val = parm_val[0]
            
        try:
            # Is this a request for the current value of the server setting?
            if parm_val == BusTesterWebController.REQUEST_STR and self.my_server is not None:
                # Return current current value:
                response_dict[parm_name] =  self.my_server[parm_name]
    
            # We are to set the server parm:
            elif self.my_server is not None:
                self.my_server[parm_name] = parm_val
                response_dict[parm_name] =  parm_val
            else:
                # It's a request for current value, but no server is running:
                response_dict[parm_name] =  BusTesterWebController.REQUEST_STR
        except KeyError:
            self.return_error(response_dict, 'Server parameter %s does not exist' % parm_name)
            raise ValueError('Server parameter %s does not exist' % parm_name)
            
        return response_dict

    @property
    def my_server(self):
        if self.server_running():
            return self.test_servers[self]
        
        # Create an OnDemandPubliser that will stream one
        # the standard topic/content:
        self.test_servers[self] = OnDemandPublisher(streamMsgs=(None,None))
        # Before starting the server, pause the message streaming server:
        self.set_stream_paused_state(True)
        self.test_servers[self].start()
        return self.test_servers[self]
        
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

    def set_stream_paused_state(self, new_state):
        '''
        Pause or unpause streaming of msgs by the 
        schoolbus tester.
        
        :param new_state: True or False
        :type new_state: string
        '''
        self.my_server.stream_paused = new_state
        

    def ensure_lower_non_array(self, val):
        if type(val) == list:
            new_val = val[0].lower()
        else:
            new_val = val.lower()
        return new_val
        

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

        application = tornado.web.Application(handlers)
        return application

if __name__ == "__main__":

    application = BusTesterWebController.makeApp()

    # We need an SSL capable HTTP server:
    # For configuration without a cert, add "cert_reqs"  : ssl.CERT_NONE
    # to the ssl_options (though I haven't tried it out.):

    application.listen(BUS_TESTER_SERVER_PORT)
    tornado.ioloop.IOLoop.instance().start()                