'''
Created on Jul 23, 2015

@author: paepcke
'''
import unittest

from redis_bus_python.redis_lib import fastqueue

TEST_ALL = True

class Test(unittest.TestCase):

    def setUp(self):
        pass

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")
    def testPutGet(self):
        
        q = fastqueue.FastQueue()
        
        self.assertTrue(q.empty())
        
        self.assertRaises(fastqueue.Empty, q.get, timeout=0.2)
            
        q.put('foo\r\n')
        self.assertEqual('foo', q.get())
        self.assertTrue(q.empty())
        
        self.assertRaises(fastqueue.Empty, q.get_nowait)
        
    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")
    def testLimitedSize(self):
        
        q = fastqueue.FastQueue(max_size=3)
        q.put('one')
        q.put('two')
        q.put('three')
        self.assertEqual(3, q.size())
        self.assertRaises(fastqueue.Full, q.put, 'four')

    @unittest.skipIf(not TEST_ALL, "Temporarily disabled")
    def testGarbageCollection(self):
        
        q = fastqueue.FastQueue(max_size=3)
        
        self.assertEqual(q.beyondLastPos, 0)
        self.assertEqual(q.nextPos, 0)
        
        
        item1 = 'one\r\n'
        item2 = 'two\r\n'
        item3 = 'three\r\n'
 
        # Set gc threshold to byte size of these 3 items:
        q.compactingThreshold = 1 + len(item1) + len(item2) + len(item3)

        q.put(item1)
        q.put(item2)
        q.put(item3)
        
        # Should be just before compacting:
        self.assertEqual(q.nextPos, 0)
        self.assertEqual(q.beyondLastPos, len(item1) + len(item2) + len(item3))
        
        # Make one free slot at start of queue:
        one = q.get() #@UnusedVariable
        self.assertEqual(q.nextPos, len(item1))
        self.assertEqual(q.beyondLastPos, len(item1) + len(item2) + len(item3))
        
        # Trigger compacting:
        item4 = 'four\r\n'
        q.put(item4)
        self.assertEqual(q.nextPos, 0)
        self.assertEqual(q.beyondLastPos, len(item2) + len(item3) + len(item4))
        
        self.assertEqual(item2[:-2], q.get())
        self.assertEqual(item3[:-2], q.get())
        self.assertEqual(item4[:-2], q.get())
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPut']
    unittest.main()