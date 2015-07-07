'''
Created on May 19, 2015

@author: paepcke
'''

class BusMessage(object):
    '''
    While messages can be passed around within a module as a JSON string,
    this class provides an alternative. Messages can be encapsulated
    in an instance of this class while in use within a bus module.
    The instance ensures that text is in UTF-8. More conveniences
    could/should be added to this class. 
    '''


    def __init__(self, pythonStruct, topicName=None):
        '''
        Create a bus message.
        
        :param pythonStruct: Any Python structure that is to appear
            on the wire in the content field of the bus message.
        :type pythonStruct: <any>
        :param topicName: topic to which the message will ultimately be published.
        :type topicName: String
        '''
        self.setContent(pythonStruct)
        self.topicName = None
        
    def setContent(self, pythonStruct):
        '''
        Change message content.
        
        :param pythonStruct: new message content
        :type pythonStruct: <any>
        '''
        serialStruct = str(pythonStruct)
        self.content = serialStruct.encode('UTF-8', 'ignore')
        # Remember the raw object:
        self.rawContent = pythonStruct
        
    def content(self):
        '''
        Retrieve current message content.
        
        :return: current UTF-8 encoded message content.
        :rtype: String
        '''
        return self.content
    
    def rawContent(self):
        '''
        Return Python data structure that will in flattened form make up the
        message content.
        
        :return: unflattened Python structure
        :rtype: <any>
        
        '''
        return self.rawContent
    
    def topicName(self):
        '''
        Return the topic name associated with this message instance.
        
        :return: topic name
        :rtype: String
        '''
        return self.topicName