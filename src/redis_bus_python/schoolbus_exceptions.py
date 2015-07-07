'''
Created on May 31, 2015

@author: paepcke
'''

class KafkaBusException(Exception):
    '''
    classdocs
    '''

    def __init__(self, msg, errorDetail=None):
        '''
        Constructor
        '''
        super(KafkaBusException, self).__init__(msg)
        self.errorDetail = errorDetail
        
    def __repr__(self):
        '''
        If the errorDetail instance variable of an
        instance of this class contains information,
        return a string <Subclassname>(<msg>: <errorDetail),
        else just return the usual <Subclassname>(<msg>).
        '''
        return('%s(%s%s)' % (self.__class__.__name__,
                             self.message,
                             ': ' + str(self.errorDetail) if self.errorDetail is not None and len(str(self.errorDetail)) > 0 else ''))
        
class KafkaServerNotFound(KafkaBusException):
    '''
    No Kafka server was found
    '''
    pass

class InsufficientInformation(KafkaBusException):
    '''
    A bus message does not contain all the information
    required by the bus protocol, or a bus module.
    Instance variable 'errorDetail' contains the 
    missing field name.
    '''
    pass


class BadInformation(KafkaBusException):
    '''
    A bus message does not contain syntactically
    or semantically incorrect information
    Instance variable 'errorDetail' contains details 
    '''
    pass
    
class SyncCallTimedOut(KafkaBusException):
    '''
    A synchronous call to a bus module did not
    return a result within a given timeout.
    '''
    pass

class SyncCallRuntimeError(KafkaBusException):
    '''
    Error while processing an incoming result to
    a synchronous call.
    '''
    pass
    
            