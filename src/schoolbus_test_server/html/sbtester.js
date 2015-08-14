
var sbTesterControl;
function SbTesterControl() {

	/* ------------------------------------ Constants ------------------*/

	// Websocket state 'ready for action'
	// (Note: for other types of sockets the
	// ready state is 1):
	var WEBSOCKET_CONNECTING_STATE = 0;
	var WEBSOCKET_READY_STATE = 1;
	var WEBSOCKET_CLOSING_STATE = 2;
	var WEBSOCKET_CLOSED_STATE = 3;
	
	var MAX_CONNECT_WAIT_TIME = 2000 // 2 seconds

	var originHost  = 'localhost';
	var controllerWebsocketPort  = 8001;
	// URL part after the domain and port.
	// Server expects websocket connections there:
	var originDir   = '/controller';
	
	var keepAliveInterval = 15000; /* 15 sec*/
	
	/* ------------------------------------ Instance Vars ------------------*/

	var keepAliveTimer    = null;

	// Dict mapping UUIDs from test server to
	// server-selection radio button widgets:
	var testServers = {}
	
	var connectAttemptTime = null;
	
	/* ------------------------------------ Methods ------------------------*/
	
	this.construct = function() {
		// Note URL of the host that pulled this JS file
		// to its browser:
		if (window.location.host.length != 0) {
			originHost = window.location.host;
			originHostAndPort = originHost.split(':');
			originHost = originHostAndPort[0];
			if (originHostAndPort.length > 1) {
				originPort = originHostAndPort[1];
			} else {
				originPort = undefined;
			}
		};
		

		connectAttemptTime = new Date();
		//*********
		//ws = new WebSocket("wss://" + originHost + originDir);
		ws = new WebSocket("wss://" + originHost + ':' + controllerWebsocketPort + originDir);
		//*********
		
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
			// We only get an event that says 'error'; make a guess
			// at what's wrong:
			try {
				if (Object.prototype.toString.call(evt.currentTarget) === "[object WebSocket]" &&
						evt.currentTarget.readyState === WEBSOCKET_CLOSED_STATE) {
					alert("The SchoolBus test server seems to be unreachable.")
				} else {
					alert("The browser has detected an error while communicating with the data server: " + evt.data);
				}
			
			} catch(err) {
				alert("The browser has detected an error while communicating with the data server: " + evt.data);
			} 
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
					     'topicsToRx' : document.getElementById('topicsToRx').value,
					     'streaming' : document.getElementById('streaming').checked ? 'True' : 'False',
					     'echo' : document.getElementById('echo').checked ? 'True' : 'False',
					     'chkSyntax' : document.getElementById('chkSyntax').checked ? 'True' : 'False',
		}
		sendReq(parmsDict);
	}
	
	this.wsReady = function() {
		return ws.readyState == WEBSOCKET_READY_STATE;
	}
	
	this.getWs = function() {
		return ws;
	}

	var send = function(msg) {
		if (ws.readyState != WEBSOCKET_READY_STATE) {
			ws.onreadystatechange = function(msg) {
				if (ws.readyState == WEBSOCKET_READY_STATE) {
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
		radioInput.setAttribute('class', 'serverSelector');
		// Indicate that we don't yet have a server ID
		// for this radio button. The next request will
		// update the id:
		radioInput.id = '';
		radioInput.checked = true;
		radioInput.addEventListener('change', switchServer);		
		// Add the new radio button just before the
		// 'Additional Server' button:
		var additionalServerBtn = document.getElementById('startServerBtn');
		document.getElementById('servers').insertBefore(radioInput, additionalServerBtn);
		
		// If more than one server exists now, enable the
		// kill server button:
		
		if (document.getElementsByName('serverRadios').length > 1) {
			document.getElementById('stopServerBtn').disabled = false;
		}
		
		// Trigger a request to the server with all-empty parameter fields:
		sendReq(getEmptyServerParmForm());
	}

	this.killServer = function() {
		
		// Can't kill last server:
		if (document.getElementsByName('serverRadios').length === 1) {
			alert("Can't kill last server.");
			return;
		}
		
		// Get server ID associated with currently checked
		// server selection radio button:
		var checkedServerId = getCheckedServerId();
		
		// Get checked radio widget:
		var chkBoxToRemove  = getCheckedServerRadioBtn();
		
		// Via the radio button's parent, remove the radio button:
		chkBoxToRemove.parentNode.removeChild(chkBoxToRemove);
		
		// Disable kill server button if only one checkbox left:
		if (document.getElementsByName('serverRadios').length === 1) {
			document.getElementById('stopServerBtn').disabled = true;
		}
		
		// Tell server that we no longer need the server
		// with the given server ID:
		sendReq({'killServer' : checkedServerId});
		
		// Now select the first radio button (without this
		// no button will be selected:
		document.getElementsByName('serverRadios')[0].checked = true;
		
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
		
		/* ********
		// Make a copy of the just-ask-for-all-values
		// request dict:
		var newReqDict = getEmptyServerParmForm();
		***************/
		// Get a dict of all current field values:
		var currReqDict = getCurrLocalFields();
		
		// Replace the 'request-cur-parm-val' values 
		// in newReqDict with the desired new values
		// as specified in the parmsDict:
		
		for (var i=0; i<reqKeysToChange.length; i++) {
			currReqDict[reqKeysToChange[i]] = parmsDict[reqKeysToChange[i]];
		}

		// Add the server UUID so that the test server can
		// find the already existing SchoolBus server:
		currReqDict['server_id'] = getCheckedServerId();
		
	    send( JSON.stringify( currReqDict ) );
	}
	

	var switchServer = function() {
		// Called when a server radio button goes checked:
		// Get ID of checked server radio button:
		sendReq(getEmptyServerParmForm());
	}
	
	var processServerResponse = function(respDict) {
		/**
		 * The respDict from the server contains either
		 * one of keys:
		 *     * 'error'
		 *     * 'success'
		 *     * 'inmsg'
		 *     * 'instat'
		 *     
		 * or the ID of an HTML widget. Respective actions are:
		 * 
		 *     * 'error': alert dialog with error msg contained in value.
		 *     * 'success': do nothing.
		 *     * 'inmsgs': write the msg in the value to the inmsgs textarea
		 *     * 'instat': write the statistics in the value to stats textarea
		 *     * ID of HTML widget: update value of the widget.
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
		
		if (serverParmNames.indexOf('inmsg') != -1) {
			document.getElementById('inmsgs').value += respDict['inmsg'];
			return;
		}
		
		if (serverParmNames.indexOf('instat') != -1) {
			document.getElementById('instats').value += respDict['instat'];
			return;
		}
		
		// Not a command: ID of a widget:
			
		// Grab the server_id from the return:
		var serverId = respDict['server_id'];
		// Give the currently selected server radio button
		// the server id as an ID, and save the server ID:
		var serverRadioBtn = getCheckedServerRadioBtn();
		testServers[serverId] = serverRadioBtn;
		serverRadioBtn.id = serverId;
		
		// Check whether this server id matches the one
		// session storage; if not, update session storage:
		//   sessionStorage.setItem('serverId', serverId)
		
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
					   (widget.type == 'textarea') ||
					   (widget.type == 'number')) {
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
			// The form has many administrative elements
			// that we don't care about; filter out the ones
			// that truly are visible user input fields/buttons:
			if ((widget.type == 'text' ||
					 widget.type == 'textarea' ||
					 widget.type == 'checkbox' ||
					 widget.type == 'radio' ||
					 widget.type == 'number') && 
				 ((widget.id.length > 0) &&
					 (widget.className !== 'serverSelector'))
			) {
				resDict[widget.id] = '';
			}
		}
		return resDict;
	}
	
	var getCurrLocalFields = function() {
		/**
		 * Go through the HTML form's widgets, and construct a dict
		 * mapping the widget to its value. There are many
		 * 'junk' widgets in a form that are not included. Also
		 * not included are elements that are of local concern only,
		 * and shouldn't go to the server. Examples are the inmsg and
		 * instat text areas, which are about SchoolBus messages that
		 * were passed *into* this UI from the server to be displayed
		 * in those text areas:
		 */
		
		var serverParmForm = document.forms['serverParms'];
		var resDict = {};
		var widget = undefined;
		for (var i=0; i < serverParmForm.length; i++) {
			widget = serverParmForm[i];
			
			// Filter out the server selection radio buttons,
			// since they are only of local significance; the
			// server won't know what to do with them:
			if (widget.className === 'serverSelector') {
				continue;
			} else if ((widget.id === 'inmsgs') || (widget.id === 'instats')) {
				continue;
			}
			
			// The form has many administrative elements
			// that we don't care about; filter out the ones
			// that truly are visible user input fields/buttons:
			if ((widget.type == 'text' ||
					widget.type == 'textarea' ||
					widget.type == 'radio' ||
					widget.type == 'number') && 
					((widget.id.length > 0) &&
							(widget.className !== 'serverSelector'))
			) {
				resDict[widget.id] = widget.value;
			} else if (widget.type == 'checkbox' ||
					   	widget.type == 'radio') {
				resDict[widget.id] = widget.checked;
			}
		}
		return resDict;
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

var sbTesterControl;
window.onload = function() {

	sbTesterControl = new SbTesterControl();
	
	document.getElementById('sendOneShotBtn').addEventListener('click', sbTesterControl.sendOneShot);
	
	document.getElementById('streaming').addEventListener('change', sbTesterControl.streamingOnOff);
	document.getElementById('echo').addEventListener('change', sbTesterControl.echoOnOff);
	document.getElementById('chkSyntax').addEventListener('change', sbTesterControl.chkSyntaxOnOff);
	
	document.getElementById('submitBtn').addEventListener('click', sbTesterControl.submit);
	
	document.getElementById('startServerBtn').addEventListener('click', sbTesterControl.startServer);
	document.getElementsByName('serverRadios')[0].addEventListener('change', sbTesterControl.switchServer)
	document.getElementById('stopServerBtn').addEventListener('click', sbTesterControl.killServer);
	document.getElementById('startServerBtn').disabled = true;
	document.getElementById('stopServerBtn').disabled = true;
	
	// Make text areas auto scroll to bottom as text get added.
	document.getElementById("inmsgs").scrollTop = document.getElementById("inmsgs").scrollHeight;
	document.getElementById("instats").scrollTop = document.getElementById("instats").scrollHeight;
	
	// Bind clear-inmsgs and stats button:
	document.getElementById("inmsgsClear").addEventListener('click', function() {
		document.getElementById("inmsgs").value = '';
		document.getElementById("instats").value = '';
		});
	
	// Have the form fields filled in with current server parameter values: 
	//*****sbTesterControl.submit();
}
