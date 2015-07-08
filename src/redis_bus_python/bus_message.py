'''
Created on May 19, 2015

@author: paepcke
'''

class BusMessage(object):
    '''
    Messages can be encapsulated into an instance of this class while 
    in use within a bus module. The instance ensures that text is in UTF-8. 
    More conveniences could/should be added to this class. 
    '''


    def __init__(self, pythonStruct, topicName=None):
        '''
        Create a bus message.
        
        :param pythonStruct: Any Python structure that is to appear
            on the wire in the theContent field of the bus message.
        :type pythonStruct: <any>
        :param topicName: topic to which the message will ultimately be published.
        :type topicName: String
        '''
        self.setContent(pythonStruct)
        self.theTopicName = topicName
        
    def setContent(self, pythonStruct):
        '''
        Change message theContent.
        
        :param pythonStruct: new message theContent
        :type pythonStruct: <any>
        '''
        serialStruct = str(pythonStruct)
        self.theContent = serialStruct.encode('UTF-8', 'ignore')
        # Remember the raw object:
        self.theRawContent = pythonStruct
        
    def content(self):
        '''
        Retrieve current message theContent.
        
        :return: current UTF-8 encoded message theContent.
        :rtype: String
        '''
        return self.theContent
    
    def rawContent(self):
        '''
        Return Python data structure that will in flattened form make up the
        message theContent.
        
        :return: unflattened Python structure
        :rtype: <any>
        
        '''
        return self.theRawContent
    
    def topicName(self):
        '''
        Return the topic name associated with this message instance.
        
        :return: topic name
        :rtype: String
        '''
        return self.theTopicName