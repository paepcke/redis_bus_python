function SbTesterControl() {

	/* ------------------------------------ Instance Vars ------------------*/

	var originHost  = '192.168.0.19';
	var originPort  = 8000;
	var REQUEST_STR = '_';
	var originDir   = 'bus/controller';
	var getCmds = {"startServerBtn" : "server",
				   "stopServerBtn"  : "server",
				   "pauseStreamBtn" : "pauseStream", 
				   "standardLen" 	: "strLen",
				   "oneShotTopic"   : "oneShotTopic",
				   "oneShotContent" : "oneShotContent",
				   "streamTopic"	: "streamTopic",
				   "streamContent"	: "streamContent",
				   "syntaxTopic"	: "syntaxTopic",
				   "discardTopics"	: "discardTopics"   
					};
	var reqTemplate = {'server'        : REQUEST_STR,
					   'pauseStream'   : REQUEST_STR,
					   'oneShotTopic'  : REQUEST_STR,
					   'oneShotContent': REQUEST_STR,
					   'streamTopic'   : REQUEST_STR,
					   'streamContent' : REQUEST_STR,
					   'syntaxTopic'   : REQUEST_STR,
					   'discardTopics' : REQUEST_STR
	}
	
	
	/* ------------------------------------ Methods ------------------------*/
	
	this.construct = function() {
		if (window.location.host.length != 0) {
			originHost = window.location.host;
		};
	}();
	
	
	
	this.startServer = function() {
		httpGet({'server' : 'on'});
	}

	this.stopServer = function() {
		httpGet({'server' : 'off'});
	}
	
	this.submit = function() {
		parmsDict = {'strLen' : document.getElementById('standardLen').innerHTML,
					 'oneShotTopic' : document.getElementById('oneShotTopic').innerHTML,
					 'oneShotContent' : document.getElementById('oneShotContent').innerHTML,
					 'streamTopic' : document.getElementById('streamTopic').innerHTML,
					 'streamContent' : document.getElementById('streamContent').innerHTML,
					 'syntaxTopic' : document.getElementById('syntaxTopic').innerHTML,
					 'discardTopics' : document.getElementById('discardTopics').innerHTML,
					 'pauseStream' : ! document.getElementById('stream').checked,
		}
		
	}
	
	var httpGet = function (parmsDict) {
		
		// Names of all the server parameters to *change*:
		reqKeysToChange =  Object.getOwnPropertyNames(parmsDict);
		
		// Make a copy of the just-ask-for-all-values
		// request dict:
		newReqDict = cloneReqTemplate();
		allReqKeys = Object.getOwnPropertyNames(newReqDict);

		// Replace the 'request-cur-parm-val' values 
		// in newReqDict with the desired new values:
		
		for (var i=0; i<reqKeysToChange.length; i++) {
			newReqDict[reqKeysToChange[i]] = parmsDict[reqKeysToChange[i]];
		}

		// Build the GET string:
		getStr = buildGetStr(newReqDict);

		theUrl = 'http://' + originHost + '/' + originDir + getStr;
	    var xmlHttp = new XMLHttpRequest();
	    xmlHttp.open( "GET", theUrl, false );
	    xmlHttp.send( null );
	    //***********
	    console.log('Ret: ' + xmlHttp.responseText);
	    //***********
	    return xmlHttp.responseText;
		
	}
	
	var buildGetStr = function(reqDict) {
		if (Object.keys(reqDict).length == 0) {
			return reqDict;
		}
		getStr = '';
		reqKeys = Object.getOwnPropertyNames(reqDict);
		for (var i=0; i<reqKeys.length; i++) {
			reqKey = reqKeys[i];
			reqVal = reqDict[reqKey];
			if (reqVal == REQUEST_STR) {
				getStr += '&' + reqKey + '=_'
			}
			else {
				getStr += '&' + reqKey + '=' + reqVal
			}
		}
		// Remove the leading '&' and replace it with a '?':
		getStr = '?' + getStr.slice(1);
		return getStr;
	}
	
	var cloneReqTemplate = function() {
		new_template = {}
		propNames = Object.getOwnPropertyNames(reqTemplate);
		for (var i=0; i<propNames.length; i++ ) {
			key = propNames[i];
			if (reqTemplate.hasOwnProperty(key)) {
				new_template[key] = reqTemplate[key];
			}
		}
		return new_template;
	}
}

var sbTesterControl = new SbTesterControl();


document.getElementById('startServerBtn').addEventListener('click', sbTesterControl.startServer);
document.getElementById('stopServerBtn').addEventListener('click', sbTesterControl.stopServer);
document.getElementById('submitBtn').addEventListener('click', sbTesterControl.submit);

