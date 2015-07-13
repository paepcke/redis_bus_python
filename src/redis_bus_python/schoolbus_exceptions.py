'''
Created on May 31, 2015

@author: paepcke
'''

class RedisServerNotFound(Exception):
    '''
    No Redis server was found
    '''
    pass

class InsufficientInformation(Exception):
    '''
    A bus message does not contain all the information
    required by the bus protocol, or a bus module.
    Instance variable 'errorDetail' contains the 
    missing field name.
    '''
    pass


class BadInformation(Exception):
    '''
    A bus message does not contain syntactically
    or semantically incorrect information
    Instance variable 'errorDetail' contains details 
    '''
    pass
    
class SyncCallTimedOut(Exception):
    '''
    A synchronous call to a bus module did not
    return a result within a given timeout.
    '''
    pass

class SyncCallRuntimeError(Exception):
    '''
    Error while processing an incoming result to
    a synchronous call.
    '''
    pass
    
            