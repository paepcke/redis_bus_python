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


    def __init__(self, content=None, topicName=None, moreArgsDict=None, **kwds):
        '''
        Create a bus message. The optional parameter moreArgsDict adds
        the respective key/values as instance variables. So if
        moreArgsDict == {'foo' : 10, 'bar' : 'myString'} then
        a BusMessage myMsg can be used like this: myMsg.foo() --> 10.
        Any additional keyword args will also end up as instance variables.
        
        
        :param pythonStruct: Any Python structure that is to appear
            on the wire in the _content field of the bus message.
        :type pythonStruct: <any>
        :param topicName: topic to which the message will ultimately be published.
        :type topicName: String
        :param moreArgsDict: optional dictionary of additional key/value pairs.
            Instance variables will be created for them.
        :type moreArgsDict: {String : <any>}
        '''
        self.content = content
        self._topicName = topicName

        if moreArgsDict is not None:
            if type(moreArgsDict) != dict:
                raise ValueError("The moreArgsDict parameter of BusMessage must be a dict, None, or left out entirely; was '%s'" % str(moreArgsDict))
            for instVarName,instVarValue in list(moreArgsDict.items()):
                setattr(self, instVarName, instVarValue)
                
        for instVarName,instVarValue in list(kwds.items()):
            setattr(self, instVarName, instVarValue)
                
            

    @property
    def content(self):
        '''
        Retrieve current message _content.
        
        :return: current UTF-8 encoded message _content.
        :rtype: String
        '''
        return self._content

    @content.setter
    def content(self, pythonStruct):
        '''
        Change message _content.
        
        :param pythonStruct: new message _content
        :type pythonStruct: <any>
        '''
        serialStruct = str(pythonStruct)
        self._content = serialStruct.encode('UTF-8', 'ignore')
        # Remember the raw object:
        self._rawContent = pythonStruct
        
    @property
    def rawContent(self):
        '''
        Return Python data structure that will in flattened form make up the
        message _content.
        
        :return: unflattened Python structure
        :rtype: <any>
        
        '''
        return self._rawContent
    
    @property
    def topicName(self):
        '''
        Return the topic name associated with this message instance.
        
        :return: topic name
        :rtype: String
        '''
        return self._topicName

    @topicName.setter
    def topicName(self, newTopicName):
        '''
        Set topicName to a new topic.
        
        :param newTopicName: new name for topic with which this BusMessage is associated
        :type newTopicName: string
        '''
        self._topicName = newTopicName

    
if __name__ == '__main__':
    myMsg = BusMessage('myString', topicName='myTopic', moreArgsDict={'foo' : 10, 'bar' : 'my string'}, kwd1=100, kwd2='foo')
    print('topicName: %s' % myMsg.topicName)
    print('foo: %s' % myMsg.foo)
    print('bar: %s' % myMsg.bar)
    print('kwd1: %s' % myMsg.kwd1)
    print('kwd2: %s' % myMsg.kwd2)
    
    