'''
Created on Aug 1, 2015

@author: paepcke
'''

import tornado.ioloop
import tornado.web

from redis_bus_python.redis_bus import OnDemandPublisher


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
        html_parms = self.request.arguments
        self.write("<html><body>GET method was called: %s.<br>" %str(html_parms))
        response_dict = {}
        
        for (parm_key, parm_value) in html_parms.items:
            if parm_key == 'server':
                if parm_value == ['ON']:
                    self.startServer()
                else:
                    self.stopServer()
                response_dict['server'] = parm_value[0]
        
        self.write(response_dict)
        self.write("</body></html>")


    def startServer(self):
        self.test_servers[self] = OnDemandPublisher()
        self.write('')

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