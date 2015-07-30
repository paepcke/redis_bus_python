'''
Created on Jul 28, 2015

@author: paepcke
'''
import functools
import re
import sys
import threading
import traceback
import unittest

from redis_bus_python.bus_message import BusMessage
from redis_bus_python.redis_bus import BusAdapter
from redis_bus_python.test.test_harness_server import OnDemandPublisher


TEST_ALL = True

class MsgDeliveryTest(unittest.TestCase):

    def setUp(self):
        self.msg_server = OnDemandPublisher()
        self.msg_server.start()
        self.deliveryDest = functools.partial(self._deliveryDest)
        self.bus = BusAdapter()
        self.delivery_event = threading.Event()
        
        # Content and topic for outgoing msgs sent
        # via the OnDemandPublisher instance:
        
        self.reference_msg_content = 'rTmzntNSQLmXokesBpLmAbPYeysftXnuntfdPKrxMVNUuqVFHFzfcrrSaRssdHuMRhPYjXKYrJjwKcyeYycEzQSkJubTabeSFLRS'
        self.reference_named_topic = 'MyTopic'
        self.reference_pattern_topic = r'MyTopic*'
        self.reference_pattern_obj = re.compile(self.reference_pattern_topic)
        self.reference_pattern_matching_topic = 'MyTopicHurray'
        self.reference_context = 'myContext'
        
        # Msg with a non-pattern, i.e. fixed-name topic:
        self.reference_bus_msg = BusMessage(topicName=self.reference_named_topic, 
                                            content=self.reference_msg_content, 
                                            context=self.reference_context)
        
        # Msg with a topic that will match self.reference_pattern_topic: 
        self.reference_bus_wildcard_msg = BusMessage(topicName=self.reference_pattern_matching_topic, 
                                                     content=self.reference_msg_content,
                                                     context=self.reference_context) 
        
    def tearDown(self):
        self.msg_server.stop()
        self.msg_server.join()
        self.bus.close()
        self.delivery_event.clear()

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testDeliveryNameNotThreaded(self):
        self.bus.subscribeToTopic(self.reference_named_topic, self.deliveryDest, threaded=False, context=self.reference_context)
        
        self.delivery_event.clear()
        self.msg_server.sendMessage(self.reference_bus_msg)
        
        # Wait for message to arrive:
        self.delivery_event.wait()
        # Compare our expected incoming bus message with
        # what we actually got:
        self.assertBusMsgsEqual(self.reference_bus_msg, self.received_bus_msg)

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testDeliveryNameThreaded(self):
        self.bus.subscribeToTopic(self.reference_named_topic, self.deliveryDest, threaded=True, context=self.reference_context)
        self.delivery_event.clear()
        self.msg_server.sendMessage(self.reference_bus_msg)
        
        # Wait for message to arrive:
        self.delivery_event.wait()
        
        # Compare our expected incoming bus message with
        # what we actually got:
        self.assertBusMsgsEqual(self.reference_bus_msg, self.received_bus_msg)

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testDeliveryPatternNotThreaded(self):
        # Subscribe to a pattern topic, using an re.Pattern instance:
        self.bus.subscribeToTopic(self.reference_pattern_obj, self.deliveryDest, threaded=False, context=self.reference_context)        
        
        self.delivery_event.clear()
        # Send to a topic that fits the reference pattern ('MyTopic*'):
        self.msg_server.sendMessage(self.reference_bus_wildcard_msg)

        self.delivery_event.wait()

        self.assertBusMsgsEqual(self.reference_bus_wildcard_msg, self.received_bus_msg)

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testDeliveryPatternThreaded(self):
        # Subscribe to a pattern topic, using an re.Pattern instance:
        self.bus.subscribeToTopic(self.reference_pattern_obj, self.deliveryDest, threaded=True, context=self.reference_context)        
        
        self.delivery_event.clear()
        # Send to a topic that fits the reference pattern ('MyTopic*'):
        self.msg_server.sendMessage(self.reference_bus_wildcard_msg)

        self.delivery_event.wait()

        self.assertBusMsgsEqual(self.reference_bus_wildcard_msg, self.received_bus_msg)

    @unittest.skipIf(not TEST_ALL, 'Temporarily disabled')
    def testSyncPublish(self):
        
        echo_msg = self.reference_bus_msg
        
        # Change target topic to what the test harness echo server
        # listens for:
        echo_msg.topicName = self.msg_server.ECHO_CHANNEL
        
        result = self.bus.publish(echo_msg, sync=True)
        self.assertEqual(self.reference_msg_content, result)

    # -------------------------  Service Methods --------------

    def assertBusMsgsEqual(self, expected_msg, actual_msg):
        self.assertEqual(expected_msg.topicName, actual_msg.topicName)
        self.assertEqual(expected_msg.context, actual_msg.context)
        self.assertEqual(expected_msg.content, actual_msg.content)

    def _deliveryDest(self, bus_msg):
        '''
        Receives incoming messages, and places them into
        instance variable received_bus_msg.
        
        :param bus_msg:
        :type bus_msg:
        '''
        #print('BusMessage: context is %s' % bus_msg.context)
        self.received_bus_msg = bus_msg
        self.delivery_event.set()

def threadStacktraces():
    sys.stderr, "\n*** STACKTRACE - START ***\n"
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename,
                                                        lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    
    for line in code:
        print >> sys.stderr, line
    print >> sys.stderr, "\n*** STACKTRACE - END ***\n"   


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testTopicDelivery']
    unittest.main()