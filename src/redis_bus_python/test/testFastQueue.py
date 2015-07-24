'''
Created on Jul 23, 2015

@author: paepcke
'''
import unittest

from redis_bus_python.redis_lib import fastqueue


class Test(unittest.TestCase):

    def setUp(self):
        pass

    def testPutGet(self):
        
        q = fastqueue.FastQueue()
        
        self.assertTrue(q.empty())
        
        self.assertRaises(fastqueue.Empty, q.get, timeout=0.2)
            
        q.put(10)
        self.assertEqual(10, q.get())
        self.assertTrue(q.empty())
        
        self.assertRaises(fastqueue.Empty, q.get_nowait)
        

    def testLimitedSize(self):
        
        q = fastqueue.FastQueue(max_size=3)
        q.put(1)
        q.put(2)
        q.put(3)
        self.assertEqual(3, q.size())
        self.assertRaises(fastqueue.Full, q.put, 4)

    def testGarbageCollection(self):
        
        q = fastqueue.FastQueue(max_size=3)
        
        self.assertEqual(q.beyondLastPos, 0)
        self.assertEqual(q.nextPos, 0)
        
        # Set gc threshold to 3 items:
        q.compactingThreshold = 4
        q.put(1)
        q.put(2)
        q.put(3)
        
        # Should be just before compacting:
        self.assertEqual(q.nextPos, 0)
        self.assertEqual(q.beyondLastPos, 3)
        
        # Make one free slot at start of queue:
        one = q.get() #@UnusedVariable
        self.assertEqual(q.nextPos, 1)
        self.assertEqual(q.beyondLastPos, 3)
        
        # Trigger compacting:
        q.put(4)
        self.assertEqual(q.nextPos, 0)
        self.assertEqual(q.beyondLastPos, 3)
        
        self.assertEqual(2, q.get())
        self.assertEqual(3, q.get())
        self.assertEqual(4, q.get())
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testPut']
    unittest.main()