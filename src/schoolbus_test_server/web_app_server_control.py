'''
Created on Aug 1, 2015

@author: paepcke
'''

import tornado.ioloop
import tornado.web

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

    def __init__(self, application, request, **kwargs):
        self.title = "SchoolBus Tester"
        super(BusTesterWebController, self).__init__(application, request, **kwargs)
        
        # UID --> (OnDemandPubliser, kill_time)
        self.test_servers = {}

    def get(self):
        getParms = self.request.arguments
        self.write("<html><body>GET method was called: %s.</body></html>" %str(getParms))

    @classmethod  
    def makeApp(self):
        '''
        Create the tornado application, making it 
        callable via http://myServer.stanford.edu:<port>/bus
        '''
        application = tornado.web.Application([
            (r"/bus", BusTesterWebController),
            ])
        return application

if __name__ == "__main__":

    application = BusTesterWebController.makeApp()

    # We need an SSL capable HTTP server:
    # For configuration without a cert, add "cert_reqs"  : ssl.CERT_NONE
    # to the ssl_options (though I haven't tried it out.):

    application.listen(BUS_TESTER_SERVER_PORT)
    tornado.ioloop.IOLoop.instance().start()                