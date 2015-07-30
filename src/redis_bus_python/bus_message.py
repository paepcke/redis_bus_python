'''
Created on May 19, 2015

@author: paepcke
'''
import datetime
import json
import time
import uuid


class BusMessage(object):
    '''
    Messages can be encapsulated into an instance of this class while 
    in use within a bus module. The instance ensures that text is in UTF-8. 
    More conveniences could/should be added to this class. 
    '''


    def __init__(self, content=None, topicName=None, isJsonContent=None, moreArgsDict=None, **kwargs):
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
        :param isJsonContent: if True, instance creation will consider the content
            to be a JSON compliant dict of the form {"content": "foo", "id": "myId", "time": "12345"}
            In this case the instance's content/id/time properties will be set to the
            respective values in the dict.
        '''
        if isJsonContent:
            contentDict = json.loads(content)
            self.content = contentDict.get('content', None)
            self._id = contentDict.get('id', None)
            if self._id is None:
                self._id = contentDict._createUuid()
            self._time = contentDict.get('time', None)
            if self._time is None:
                contentDict._int(time.time) * 1000
        else:
            self.content    = content
            # If moreArgsDict includes a key 'id' then
            # the following _id value will be overwritten
            # below. That's by design, so that one can
            # feed a json-decoded incoming bus msg in via
            # moreArgsDict, and have this BusMessage instance
            # reflect that incoming json-formatted message.
            # Without moreArgsDict or in the absence of 
            # an 'id' key in moreArgsDict, the following UUID 
            # remains this BusMessage instance's id:
            
            self._id        = self._createUuid()
            
            # Init the time field, though that might be
            # modified by the BusAdapter.publish() method.
            # See comment above for _time being overwritten:
            
            self._time  	= int(time.time()*1000)

        self._topicName = topicName
        
        # A data structure to pass to callable, if this message
        # is passed to one:
        
        self.context = None

        if moreArgsDict is not None:
            if type(moreArgsDict) != dict:
                raise ValueError("The moreArgsDict parameter of BusMessage must be a dict, None, or left out entirely; was '%s'" % str(moreArgsDict))
            for instVarName,instVarValue in list(moreArgsDict.items()):

                # Ensure that the instance variable name is not unicode:
                finalInstVarName = instVarName.encode('UTF-8', 'ignore')
                
                # For values: if incoming dict was created by json.loads()
                # then all values will be unicode. Determine whether
                # the original was likely to have been UTF8 instead,
                # and if so, return value to UTF8:
                try:
                    # Will throw an attribute error if instVarValue
                    # is not a string; we catch that:
                    decodedVal = instVarValue.encode('UTF8')
                    # At least the value could be turned into UTF8;
                    # It could still be true unicode if encode()
                    # was able to *convert* to UTF8, such as
                    # 'Fl\xc3\xbcgel':
                    finalInstVarVal = decodedVal if decodedVal == instVarValue else instVarValue
                except (UnicodeDecodeError, AttributeError):
                    finalInstVarVal = instVarValue
                
                setattr(self, finalInstVarName, finalInstVarVal)
                
        for instVarName,instVarValue in list(kwargs.items()):
            setattr(self, instVarName, instVarValue)
                
    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, msgId):
        self._id = msgId
    
    @property
    def time(self):
        return self._time
    
    @time.setter
    def time(self, msecSinceEpoch):
        self._time = msecSinceEpoch
        
    @property
    def context(self):
        return self._context
    
    @context.setter
    def context(self, new_context):
        self._context = new_context
    
    @property
    def isoTime(self, timeZone='GMT'):
        '''
        Returns the message's time in ISO8601 format.
        Example: '2015-07-05T22:16:18+00:00' for GMT
        '''
        return datetime.datetime.fromtimestamp(self._time).isoformat()
    
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

# --------------------------  Private Methods ---------------------


    def _createUuid(self):
        return str(uuid.uuid4())
            
    
if __name__ == '__main__':
    myMsg = BusMessage('myString', topicName='myTopic', moreArgsDict={'foo' : 10, 'bar' : 'my string'}, kwd1=100, kwd2='foo')
    print('topicName: %s' % myMsg.topicName)
    print('foo: %s' % myMsg.foo)
    print('bar: %s' % myMsg.bar)
    print('kwd1: %s' % myMsg.kwd1)
    print('kwd2: %s' % myMsg.kwd2)
    
    