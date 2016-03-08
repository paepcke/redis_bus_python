#!/usr/bin/env python
'''
Created on Mar 8, 2016

@author: paepcke
'''
import functools
import os
import sys
import time

from redis_bus_python.redis_bus import BusAdapter


class MsgFileWriter(object):
    '''
    classdocs
    '''


    def __init__(self, topic_to_follow):
        '''
        Constructor
        '''
        target_file = '/tmp/msgFile.txt'
        self.bus = BusAdapter()
        
        # Make sure to start with a new file
        try:
            os.remove(target_file)
        except:
            pass
        
        self.fd  = open(target_file, 'a') 
        self.bus.subscribeToTopic(topic_to_follow, functools.partial(self.handle_bus_msg))
        
    def handle_bus_msg(self, bus_msg):
        self.fd.write(bus_msg.content + '\n')
        
    def shutdown(self):
        self.bus.close()

if __name__ == '__main__':
    
    msg_file_writer = MsgFileWriter('studentAction')
    print('Starting SchoolBus message file writer: %s-message to file %s...' %\
          ('studentAction', '/tmp/msgFile.txt'))
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            msg_file_writer.shutdown()
            print('Exiting SchoolBus message file writer...')
            sys.exit()