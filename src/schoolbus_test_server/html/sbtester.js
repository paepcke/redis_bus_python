
var sbTesterControl;
function SbTesterControl() {

	/* ------------------------------------ Constants ------------------*/

	// Websocket state 'ready for action'
	// (Note: for other types of sockets the
	// ready state is 1):
	var READY_STATE = 1;
	var MAX_CONNECT_WAIT_TIME = 2000 // 2 seconds

	var originHost  = 'localhost';
	var originPort  = 8000;
	// URL part after the domain and port.
	// Server expects websocket connections there:
	var originDir   = '/controller';
	
	var keepAliveInterval = 15000; /* 15 sec*/
	
	/* ------------------------------------ Instance Vars ------------------*/

	var keepAliveTimer    = null;

	// Will be filled by constructor with 
	// all UI elements as keys, and empty strings
	// as values. When a request for a param change
	// on the server is made, just that parameter
	// value is modified in reqTemplate:
	var reqTemplate = {};
	
	// Dict mapping UUIDs from test server to
	// server-selection radio button widgets:
	var testServers = {}
	
	var connectAttemptTime = null;
	
	/* ------------------------------------ Methods ------------------------*/
	
	this.construct = function() {
		// Note URL of the host that pulled this JS file
		// to its browser:
		originHost
		if (window.location.host.length != 0) {
			originHost = window.location.host;
		};

		connectAttemptTime = new Date();
		ws = new WebSocket("ws://" + originHost + originDir);
		
		ws.onopen = function() {
		    keepAliveTimer = window.setInterval(function() {sendKeepAlive()}, keepAliveInterval);
		};
	
		ws.onclose = function() {
			if (keepAliveTimer !== null) {
		    	clearInterval(keepAliveTimer);
			}
		    alert("The browser or server closed the connection, or network trouble; please reload the page to resume.");
		}
	
		ws.onerror = function(evt) {
			if (keepAliveTimer !== null) {
		    	clearInterval(keepAliveTimer);
			}
		    alert("The browser has detected an error while communicating with the data server: " + evt.data);
		}

	    var sendKeepAlive = function() {
			//var req = buildRequest("keepAlive", "");
	    	var req = "keepAlive";
			ws.send(req);
	    }
		
		ws.onmessage = function(evt) {
		    // Internalize the JSON
		    // e.g. "{resp : "courseList", "args" : ['course1','course2']"
		    try {
			//var oneLineData = evt.data.replace(/(\r\n|\n|\r)/gm," ");
			argsObj = JSON.parse(evt.data);
		    } catch(err) {
		    	alert('Error report from server (' + evt.data + '): ' + err );
			return
		    }
		    processServerResponse(argsObj);
		}
		
		
	}();
	
	this.submit = function() {
		var parmsDict = {'strLen' : document.getElementById('strLen').value,
					     'oneShotTopic' : document.getElementById('oneShotTopic').value,
					     'oneShotContent' : document.getElementById('oneShotContent').value,
					     'streamTopic' : document.getElementById('streamTopic').value,
					     'streamContent' : document.getElementById('streamContent').value,
					     'streamInterval' : document.getElementById('streamInterval').value,
					     'syntaxTopic' : document.getElementById('syntaxTopic').value,
					     'discardTopics' : document.getElementById('discardTopics').value,
					     'streaming' : document.getElementById('streaming').checked ? 'True' : 'False',
					     'echo' : document.getElementById('echo').checked ? 'True' : 'False',
					     'chkSyntax' : document.getElementById('chkSyntax').checked ? 'True' : 'False',
		}
		sendReq(parmsDict);
	}
	
	this.wsReady = function() {
		return ws.readyState == READY_STATE;
	}
	
	this.getWs = function() {
		return ws;
	}

	var send = function(msg) {
		if (ws.readyState != READY_STATE) {
			ws.onreadystatechange = function(msg) {
				if (ws.readyState == READY_STATE) {
					ws.send(msg);
				} else {
					alert('Could not connect to server; timed out.');
				}
			};
			return;
		} else {
			ws.send(msg);
		}
	}

	this.startServer = function() {
		// Simply add a radio button to the bank of
		// available-servers bank:
		var serverRadios = document.getElementsByName('serverRadios')
		var radioInput = document.createElement('input');
		radioInput.setAttribute('type', 'radio');
		radioInput.setAttribute('name', 'serverRadios');
		// Indicate that we don't yet have a server ID
		// for this radio button. The next request will
		// update the id:
		radioInput.id = '';
		radioInput.checked = true;
		// Add the new radio button just before the
		// 'Additional Server' button:
		var additionalServerBtn = document.getElementById('startServerBtn');
		document.getElementById('servers').insertBefore(radioInput, additionalServerBtn);
		
		// Trigger a request to the server with all-empty parameter fields:
		sendReq(getEmptyServerParmForm());
	}

	this.sendOneShot = function() {
		// Ask server to send a one-shot bus message:
		sendReq({'oneShot' : 'True'})
	}

	this.streamingOnOff = function() {
		sendReq({'streaming' : document.getElementById('streaming').checked ? 'True' : 'False'});
	}
	
	this.echoOnOff = function() {
		sendReq({'echo' : document.getElementById('echo').checked ? 'True' : 'False'});
	}

	this.chkSyntaxOnOff = function() {
		sendReq({'chkSyntax' : document.getElementById('chkSyntax').checked ? 'True' : 'False'});
	}
	
	var sendReq = function (parmsDict) {
		// Names of all the server parameters to *change*:
		var reqKeysToChange =  Object.getOwnPropertyNames(parmsDict);
		
		// Make a copy of the just-ask-for-all-values
		// request dict:
		var newReqDict = getEmptyServerParmForm();
		var allReqKeys = Object.getOwnPropertyNames(newReqDict);

		// Replace the 'request-cur-parm-val' values 
		// in newReqDict with the desired new values
		// as specified in the parmsDict:
		
		for (var i=0; i<reqKeysToChange.length; i++) {
			newReqDict[reqKeysToChange[i]] = parmsDict[reqKeysToChange[i]];
		}

		// Add the server UUID so that the test server can
		// find the already existing SchoolBus server:
		var uuid = getCheckedServerId();
		
		// If uuid is the placeholder '_' then 
		// set the server id in the request dict 
		// to the empty string; else to the uuid:
		
		newReqDict['server_id'] = uuid;
		
	    send( JSON.stringify( newReqDict ) );
	}
	
	var processServerResponse = function(respDict) {
		/**
		 * For each key/value pair in respDict, looks up the
		 * UI widget ID that holds the respective parameter
		 * value (i.e. text field, checkbox...). Modifies 
		 * those values to match the received respDict.
		 * 
		 * :param respDict: keys are names of server parameters.
		 * :type respDict: {str : str}
		 */
		
		
		var serverParmNames = Object.getOwnPropertyNames(respDict);
		if (serverParmNames.indexOf('error') != -1) {
			alert('Test server error: ' + respDict['error'])
			return
		}
		
		if (serverParmNames.indexOf('success') != -1) {
			// Our request to the server was to do something
			// without a return, e.g. firing a one-shot message:
			return
		}
		
		// Grab the server_id from the return:
		var serverId = respDict['server_id'];
		var serverRadioBtn = getCheckedServerRadioBtn();
		testServers[serverId] = serverRadioBtn;
		serverRadioBtn.id = serverId;
		
		var parmName;
		var newVal;
		var widget;
		for (var i=0; i<serverParmNames.length; i++) {
			parmName = serverParmNames[i];
			newVal = respDict[parmName];
			
			// See which widget in the HTML page corresponds
			// to the server parameter parmName:
			widget = document.getElementById(parmName);
			if (widget == null) {
				// None (e.g. server_id):
				continue;
			}
			
			if (widget.type == 'checkbox') {
				// It's a checkbox; values will be 'True' or 'False':
				document.getElementById(parmName).checked = newVal === true;
				
		
			} else if ((widget.type == 'text') ||
					   (widget.type == 'textarea')) {
				// 
				document.getElementById(parmName).value = txtArrayToStr(newVal);
			} else {
				// Some other widget that doesn't correspond to a server parameter:
				continue
			}
			
		}
	}
	
	var getEmptyServerParmForm = function() {
		var serverParmForm = document.forms['serverParms'];
		var resDict = {};
		var widget = undefined;
		for (var i=0; i < serverParmForm.length; i++) {
			widget = serverParmForm[i];
			if ((widget.type == 'text' ||
					 widget.type == 'textarea' ||
					 widget.type == 'checkbox' ||
					 widget.type == 'radio' ||
					 widget.type == 'number') && 
				 widget.id.length > 0){
				resDict[widget.id] = '';
			}
		}
		return resDict;
	}

	
	var cloneReqTemplate = function() {
		var newTemplate = {}
		var propNames = Object.getOwnPropertyNames(reqTemplate);
		for (var i=0; i<propNames.length; i++ ) {
			key = propNames[i];
			if (reqTemplate.hasOwnProperty(key)) {
				newTemplate[key] = reqTemplate[key];
			}
		}
		return newTemplate;
	}
	
	var getCheckedServerId = function() {
		/**
		 * Returns the UUID of the test server whose
		 * corresponding radio button is checked.
		 */
		
		var serverRadios = document.getElementsByName('serverRadios')
		for (var i=0; i<serverRadios.length; i++) {
			if (serverRadios[i].checked == true) {
				return serverRadios[i].id
			}
		}
	}
	
	var getCheckedServerRadioBtn = function() {
		var serverRadios = document.getElementsByName('serverRadios')
		for (var i=0; i<serverRadios.length; i++) {
			if (serverRadios[i].checked == true) {
				return serverRadios[i]
			}
		}
		
	}
	
	var isArray = function(obj) {
		return Object.prototype.toString.call( obj ) === '[object Array]';
	}
	
	var txtArrayToStr = function(obj) {
		if (! isArray(obj)) {
			return obj
		} 
		// It's an array
		var res = '';
		var element;
		for (var i=0; i<obj.length; i++) {
			element = obj[i];
			if (res.length != 0) {
				res += ', ';
			} 
			if (Object.prototype.toString.call(element) !== '[object String]') {
				// Against promise: an element isn't a string:
				res += Object.prototype.toString.call(element);
			} else {
				res += element
			}
		}
		return res;
	}
}

document.getElementById('sendOneShotBtn').addEventListener('click', SbTesterControl.sendOneShot);

// document.getElementById('streaming').addEventListener('input', sbTesterControl.streamingOnOff);
// document.getElementById('echo').addEventListener('input', sbTesterControl.echoOnOff);
// document.getElementById('chkSyntax').addEventListener('input', sbTesterControl.chkSyntaxOnOff);

document.getElementById('submitBtn').addEventListener('click', SbTesterControl.submit);

document.getElementById('startServerBtn').addEventListener('click', SbTesterControl.startServer);



/*if (typeof sbTesterControl === "undefined") {
	sbTesterControl = new SbTesterControl();

	// Fill in the fields with actual server parm values:
	// (Race condition with Websocket connection process
	// when done here)
	//******sbTesterControl.submit();
	
		
	document.getElementById('sendOneShotBtn').addEventListener('click', sbTesterControl.sendOneShot);
	
	// document.getElementById('streaming').addEventListener('input', sbTesterControl.streamingOnOff);
	// document.getElementById('echo').addEventListener('input', sbTesterControl.echoOnOff);
	// document.getElementById('chkSyntax').addEventListener('input', sbTesterControl.chkSyntaxOnOff);
	
	document.getElementById('submitBtn').addEventListener('click', sbTesterControl.submit);
	
	document.getElementById('startServerBtn').addEventListener('click', sbTesterControl.startServer);
}
*//*window.onload = function() {
	if (! sbTesterControl.wsReady()) {
		sbTesterControl.getWs().onreadystatechange = function() {
			if (sbTesterControl.wsReady()) {
				sbTesterControl.submit();
			}
		}
	} else {
		sbTesterControl.submit();
	}
};
*/
