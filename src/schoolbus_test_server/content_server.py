#!/usr/bin/env python

'''
Created on Aug 14, 2015

@author: paepcke
'''
import os
import tornado.httpserver
import tornado.ioloop
import tornado.web

BUS_TESTER_SERVER_PORT = 8001

class UiContentServer(tornado.web.RequestHandler):
    '''
    classdocs
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

        handlers = [
            (r"/bus/(.*)", tornado.web.StaticFileHandler, 
             {'path' : './html',  "default_filename": "index.html"}),
            ]
        application = tornado.web.Application(handlers, debug=False)
        return application
    
if __name__ == '__main__':

    application = UiContentServer.makeApp()
    
    (certFile,keyFile) = UiContentServer.getCertAndKey()
    sslArgsDict = {'certfile' : certFile,
                   'keyfile'  : keyFile}

    http_server = tornado.httpserver.HTTPServer(application,ssl_options=sslArgsDict)

    application.listen(8000, ssl_options=sslArgsDict)
        
    # For non-ssl:
    # ****http_server = tornado.httpserver.HTTPServer(application)
    # ****http_server.listen(BUS_TESTER_SERVER_PORT)

    print('Starting SchoolBus Web UI server on port %d' % BUS_TESTER_SERVER_PORT)
    try:
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except Exception as e:
        print('Bombed out of tornado IO loop: %s' % `e`)
    
    print('SchoolBus Web UI server has shut down.')
    