'''
Created on Jul 24, 2015

@author: paepcke
'''
import unittest

from redis_bus_python.redis_lib.connection import OneShotConnection


class TestConnection(unittest.TestCase):
    '''
    Tests connection facilities of Redis libraray.
    Note: assumes a working Redis server running
    at localhost at 6379
    '''
    
    PUBLISH_STR = "*3\r\n$7\r\nPUBLISH\r\n$4\r\ntest\r\n$3\r\nfoo\r\n"    

    def setUp(self):
        self.conn = OneShotConnection()
    
    def tearDown(self):
        self.conn.disconnect()

    def testSimpleStringReturn(self):
        self.conn.send_command('PING')
        res = self.conn.read_string()
        self.assertEqual('PONG', res)
        
    def testIntReturn(self):
        self.conn._sock.sendall(TestConnection.PUBLISH_STR)
        res = self.conn.read_int()
        self.assertEqual(0, res)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()