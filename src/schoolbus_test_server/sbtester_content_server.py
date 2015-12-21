#!/usr/bin/env python

'''
Created on Aug 14, 2015

@author: paepcke
'''
import os
import socket
import tornado.httpserver
import tornado.ioloop
import tornado.web


BUS_TESTER_WEBSOCKET_PORT = 8000

class UiContentServer(tornado.web.RequestHandler):
    '''
    Simple Tornado static Web server that serves the
    UI of the SchoolScope. It is started by sbtester_app_server.py.
    '''


    def __init__(self):
        '''
        Constructor
        '''
        pass

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
        callable via http://myServer.stanford.edu:<port>/bus/
        '''
        
        script_dir = os.path.dirname(__file__)
        html_path    = {'path' : os.path.join(script_dir, 'html'), "default_filename": "index.html"}
        favicon_path = {'path' : os.path.join(script_dir, 'html')}
        
        handlers = [
             (r"/(favicon\.ico)", tornado.web.StaticFileHandler, favicon_path),
             (r"/bus/(.*)", tornado.web.StaticFileHandler, html_path),
            ]
        application = tornado.web.Application(handlers, debug=False)
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

    io_loop = tornado.ioloop.IOLoop.instance()
    # Schedule the shutdown for after all pending
    # requests have been services:
    io_loop.add_callback(io_loop.stop)
    
    
if __name__ == '__main__':

    application = UiContentServer.makeApp()
    
    (certFile,keyFile) = UiContentServer.getCertAndKey()
    sslArgsDict = {'certfile' : certFile,
                   'keyfile'  : keyFile}

    # For ssl:
    ssl_used = True
    http_server = tornado.httpserver.HTTPServer(application,ssl_options=sslArgsDict)
    application.listen(8000, ssl_options=sslArgsDict)
        
    # For non-ssl:
    #ssl_used = False
    #http_server = tornado.httpserver.HTTPServer(application)
    #http_server.listen(BUS_TESTER_WEBSOCKET_PORT)

    if ssl_used:
        protocol_spec = 'https'
    else:
        protocol_spec = 'http'
    start_msg = 'Starting SchoolBus Web UI server on %s://%s:%d/bus/' % \
        (protocol_spec, socket.gethostname(), BUS_TESTER_WEBSOCKET_PORT)

    print(start_msg)
    try:
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except KeyboardInterrupt:
        shutdown()
    except Exception as e:
        print('Bombed out of tornado IO loop: %s' % `e`)
    
    print('SchoolBus Web UI server has shut down.')
    