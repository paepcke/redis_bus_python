'''
Created on Jul 23, 2015

@author: paepcke

TODO:
  o More documentation
  o allow put() with blocking and timeout

'''

import array
import threading


# Faking an enum type:
def enum(**enums):
    return type('Enum', (), enums)

Types = enum(char = 'c',
             signedChar = 'b',
             uChar = 'B',
             unicode = 'u',
             signedShort = 'h',
             uShort = 'H',
             signedInt = 'i',
             uInt = 'I',
             signedLong = 'l',
             uLong = 'L',
             float = 'f',
             double = 'd'
             )

class Full(Exception):
    pass

class Empty(Exception):
    pass

class FastQueue(object):
    '''
    Alternative to queue.Queue, which is oddly slow. This
    implementation uses a simple lock to manage an array.
    '''

    def __init__(self, max_size=0):
        '''
        Constructor
        '''
        
        self.MAX_SIZE = max_size
        self.queueArr          = array.array(Types.char)
        self.nextPos           = 0
        self.beyondLastPos     = 0
        self.numItems          = 0
        
        # Length of queue that triggers garbage collection:
        self.compactingThreshold = 2**13 # 8k
        
        self.queueLock    = threading.Lock()
        self.putCondition = threading.Condition()
        
    def get(self, blocking=True, timeout=None):
        
        is_empty = self.empty()
        if not blocking and is_empty:
            raise Empty('Queue is empty, and non-blocking get() was called.')

        if is_empty:
            self.putCondition.acquire()
            self.putCondition.wait(timeout)
            self.putCondition.release()

        self.queueLock.acquire()
        try:
            val = self.getItem()
            self.numItems -= 1
            return val.tostring()
        finally:
            self.queueLock.release()

    def get_nowait(self):
        return self.get(blocking=False)
    
    def put(self, item):
        
        self.queueLock.acquire()
        try:
            self.putItem(item)
            self.numItems += 1
        finally:
            self.queueLock.release()
            self.putCondition.acquire()
            self.putCondition.notify()
            self.putCondition.release()

    def put_nowait(self, item):
        self.put(item)

    def empty(self):
        return self.beyondLastPos == self.nextPos
    
    def full(self):
        return self.MAX_SIZE > 0 and self.numItems >= self.MAX_SIZE

    def size(self):
        return self.numItems

# ------------------------------ Private Methods -------------------
        
        
    def getItem(self):
        if self.empty():
            raise Empty('No items in queue')
        (terminatorStart, postTerminator) = self.terminatorPos()
        retVal = self.queueArr[self.nextPos:terminatorStart]
        self.nextPos = postTerminator
        if self.nextPos == self.beyondLastPos:
            # Queue empty; take opportunity to 
            # reset to the beginning:
            self.nextPos = 0
            self.beyondLastPos = 0
        return retVal

    def putItem(self, item):

        # Does queue have a max size, which we exceeded?        
        if self.MAX_SIZE > 0 and self.numItems >= self.MAX_SIZE:
            # Reached capacity:
            raise Full('Queue reached max capacity of %d' % self.MAX_SIZE)
        
        #****self.queueArr.append(item)
        self.queueArr.fromstring(item)
        self.beyondLastPos += self.queueArr.itemsize * len(item)
        
        # Time to gc, and do we have room at start of queue,
        # and reached the threshold?
        if self.beyondLastPos >= self.compactingThreshold and self.nextPos > 0:
            self.garbageCollect()
      
    def terminatorPos(self, terminator='\r\n'):
        '''
        Throws value error if terminator not found.
        
        :param terminator:
        :type terminator:
        '''
        try:
            indx = self.nextPos + self.queueArr[self.nextPos:].index(terminator[0])
            for pt in range(len(terminator)):
                if self.queueArr[indx+pt] != terminator[pt]:
                    raise ValueError('Terminator not found in array')
            return (indx, indx+len(terminator))
        except (ValueError, IndexError):
            raise ValueError('Terminator not found in array')
                 
            
            
        
            
    def garbageCollect(self):
        '''
        Move the content of the array left to 
        start at zero, reclaiming the early parts
        of the queue array.
        
        
        '''
        if self.full():
            # Reached capacity:
            raise Full('Queue reached capacity of %d' % self.MAX_SIZE)
        tmpArr = self.queueArr[self.nextPos:self.beyondLastPos]
        self.queueArr = tmpArr
        self.beyondLastPos = self.beyondLastPos - self.nextPos
        self.nextPos = 0
        tmpArr = None
        
