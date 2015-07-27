'''
Created on Jul 27, 2015

@author: paepcke
'''
import codecs
import unittest

from redis_bus_python.redis_lib.connection import OneShotConnection, \
    ConnectionPool, Connection
from redis_bus_python.test.test_harness_server import OnDemandPublisher


TEST_ALL = True

class OneShotTester(unittest.TestCase):

    to_channel = 'tmp.10'
    from_channel = 'tmp.20'
    test_msg = 'Hello world'
    
    @classmethod
    def setUpClass(cls):
        cls.answer_server = OnDemandPublisher()
        cls.answer_server.start()
        
    @classmethod
    def tearDownClass(cls):
        cls.answer_server.stop()
        cls.answer_server.join()

    def setUp(self):
        self.to_channel = OneShotTester.to_channel
        self.from_channel = OneShotTester.from_channel
        self.test_msg = OneShotTester.test_msg
        
        self.oneshot_connection_pool = ConnectionPool(connection_class=OneShotConnection)
        self.conn = self.oneshot_connection_pool.get_connection()
        
        self.subscribe_cmd   = self.conn.pack_subscription_command('SUBSCRIBE', self.from_channel)
        self.publish_cmd     = self.conn.pack_publish_command(self.to_channel, self.test_msg)
        self.unsubscribe_cmd = self.conn.pack_subscription_command('UNSUBSCRIBE', self.from_channel)
                
    def tearDown(self):
        self.conn.disconnect()

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testSubscriptions(self):
        
        self.conn.write_socket(self.subscribe_cmd)
        
        # Read and discard the returned status.
        # The method will throw a timeout error if the
        # server does not send the status. The
        # status to a subscribe consists of six lines,
        # such as: 
        #       '*3\r\n$9\r\nsubscribe\r\n$5\r\ntmp.0\r\n:1\r\n'

        self.assertEqual('*3', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('$9', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('subscribe', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('$%d' % len(self.from_channel), self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual(self.from_channel, self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual(':1', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        
        self.conn.write_socket(self.unsubscribe_cmd)
                
        self.assertEqual('*3', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('$11', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('unsubscribe', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual('$%d' % len(self.from_channel), self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual(self.from_channel, self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        self.assertEqual(':0', self.conn.readline(block=True, timeout=Connection.REDIS_RESPONSE_TIMEOUT))
        
        # Should be nothing left in the socket:
        self.assertIsNone(self.conn.readline(block=False))

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')            
    def testReadInt(self):
        
        self.conn.write_socket(self.subscribe_cmd)
        self.assertEqual(3, self.conn.read_int())

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')        
    def testRead(self):
        
        # Reading n bytes...
        
        self.conn.write_socket(self.subscribe_cmd)
        
        # (See comments in testSubscriptions() for details
        #  of what returns from the server):
        
        # Throw away the server return status element '*3':
        self.conn.readline()
        
        # Get length of 'subscribe' element:
        subscribe_len = self.conn.read_int()
        
        
        self.assertEqual('subscribe', self.conn.read(subscribe_len))

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testReadString(self):
        
        self.conn.write_socket(self.subscribe_cmd)
        
        # (See comments in testSubscriptions() for details
        #  of what returns from the server):
        
        # Throw away the server return status element '*3':
        self.conn.readline()
        
        # Next wire protocol line will be length of subscribe,
        # followed by 'subscribe': 
        
        self.assertEqual('subscribe', self.conn.read_string())
        
    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')        
    def testParseResponse(self):

        # Parse a simple response to a subscribe:
        self.conn.write_socket(self.subscribe_cmd)
        parsed_resp = self.conn.parse_response()
        self.assertEqual([u'subscribe', u'tmp.20', 1L], parsed_resp)
        
        # Parse a more complicated incoming msg:
#                 '*3\r\n' +\
# 		    	  '$7\r\n' +\
# 		    	  'PUBLISH\r\n' +\
# 		    	  '$4\r\n' +\
# 		    	  'test\r\n' +\
# 		    	  '$11\r\n{"Hello World"}'
        
        # Make the OnDemandPublisher thread send us this
        # message:
        OneShotTester.answer_server.sendMessage(OneShotTester.test_msg, OneShotTester.from_channel)
        parsed_resp = self.conn.parse_response()
        
        # Get the message that was sent back to us:
        sent_bus_msg = OneShotTester.answer_server.outMsg
        out_id = sent_bus_msg.id
        rx_encoded = []
        for el in codecs.iterencode(parsed_resp, 'UTF-8'):
            rx_encoded.append(el)
        expected = ['message', 
                    OneShotTester.from_channel,
                    '{"content": "Hello world", "id": "%s", ' % out_id
                    ]
        # Time of sent msg will be different each time; cut it out of the 
        # rx-ed string:
        rxed_msg_body = rx_encoded[2]
        rxed_body_chop_pos = rxed_msg_body.index('"time": ')
        chopped_rxed_body = rxed_msg_body[:rxed_body_chop_pos]
        # Replaced msg body part of received by the
        # truncated version that doesn't include the time:
        rx_encoded[2] = chopped_rxed_body 
        
        self.assertEqual(expected, rx_encoded)
        
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSubscribe']
    unittest.main()