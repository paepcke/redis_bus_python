/*
 * Generic javascript access to SchoolBus bridge.
 * Communicates via websocket to a server that
 * forwards published messages to the bus, and
 * delivers incoming messages for subscribed topics
 * to the Web browser where this BusInteractor runs.
 * Such a generic server is js_bus_bridge.py. Its
 * protocol expectations are matched with this interactor.
 * 
 * Client creates an instance of BusInteractor,
 * passing a function to call with incoming bus 
 * messages, a function to call when errors occur,
 * and an optional third argument with the bus bridge
 * server's fully qualified domain name. It defaults
 * to localhost. 
 */

function BusInteractor(msgCallback, errCallback, busBridgeHost) {

	/* ------------------------------------ Constants ------------------*/

	var msgCallback = msgCallback;
	var errCallback = errCallback;
	if (typeof busBridgeHost === 'undefined') {
		originHost = 'localhost';
	} else {
		originHost = busBridgeHost;
	}

	var controllerWebsocketPort  = 4363;
	// URL part after the domain and port.
	// Server expects websocket connections there:
	var originDir   = '/jsbusbridge';

	// Websocket state 'ready for action'
	// (Note: for other types of sockets the
	// ready state is 1):
	var WEBSOCKET_CONNECTING_STATE = 0;
	var WEBSOCKET_READY_STATE = 1;
	var WEBSOCKET_CLOSING_STATE = 2;
	var WEBSOCKET_CLOSED_STATE = 3;
	
	var MAX_CONNECT_WAIT_TIME = 2000 // 2 seconds
	
	var keepAliveInterval = 15000; /* 15 sec*/
	
	/* ------------------------------------ Instance Vars ------------------*/

	var keepAliveTimer    = null;

	// Dict mapping UUIDs from test server to
	// server-selection radio button widgets:
	var testServers = {}
	
	var connectAttemptTime = null;
	
	var ws = null;
	
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
		
	}();

	var sendKeepAlive = function() {
		//var req = buildRequest("keepAlive", "");
		var req = "keepAlive";
		if (ws === null) {
			initWebsocket();
		}
		ws.send(req);
	}

	this.initWebsocket = function() {
		//*********
		//ws = new WebSocket("ws://" + originHost + ':' + controllerWebsocketPort + originDir);
		ws = new WebSocket("wss://" + originHost + ':' + controllerWebsocketPort + originDir);

		//*********
		
		ws.onopen = function() {
		    keepAliveTimer = window.setInterval(function() {sendKeepAlive()}, keepAliveInterval);
		};
	
		ws.onclose = function() {
			if (keepAliveTimer !== null) {
		    	clearInterval(keepAliveTimer);
			}
		    err_callback("The browser or server closed the connection, or network trouble; please reload the page to resume.");
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
					err_callback("The SchoolBus test server seems to be unreachable.")
				} else {
					err_callback("The browser has detected an error while communicating with the data server: " + evt.data);
				}
			
			} catch(err) {
				err_callback("The browser has detected an error while communicating with the data server: " + evt.data);
			} 
		}

		ws.onmessage = function(evt) {
		    // Internalize the JSON
		    // e.g. "{resp : "courseList", "args" : ['course1','course2']"
		    try {
			//var oneLineData = evt.data.replace(/(\r\n|\n|\r)/gm," ");
			argsObj = JSON.parse(evt.data);
		    } catch(err) {
		    	err_callback('Error report from server (' + evt.data + '): ' + err );
			return
		    }
		    processServerResponse(argsObj);
		}
		
	}
	
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
		if (ws === null) {
			initWebsocket();
		}
		return ws.readyState == WEBSOCKET_READY_STATE;
	}
	
	this.getWs = function() {
		return ws;
	}

	var send = function(msg) {
		if (ws === null) {
			initWebsocket();
		}
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
	
	this.subscribeToTopic = function(topic) {
		subscribedTopics = txtArrayToStr(document.getElementById('topicsToRx').innerHTML);
		topicArr = subscribedTopics.split(/,[ ]*/);
		if (topicArr.indexOf(topic) === -1) {
			// Weren't already subscribed to:
			topicArr.push(topic);
			topicStr = txtArrayToStr(topicArr);
			document.getElementById('topicsToRx').innerHTML = topicStr;
			sendReq({'topicsToRx' : topicStr});
		}
	}
	
	this.unsubscribeFromTopic = function(topic) {
		subscribedTopics = txtArrayToStr(document.getElementById('topicsToRx').innerHTML);
		topicArr = subscribedTopics.split(/,[ ]*/);
		topicPos = topicArr.indexOf(topic);
		// Was topic subscribed to?
		if (topicPos > -1) {
			// Remove one element from arr at pos topicPos:
			topicArr.splice(topicPos, 1);
			topicStr = txtArrayToStr(topicArr);
			document.getElementById('topicsToRx').innerHTML = topicStr;
			// Update the server:
			sendReq({'topicsToRx' : topicStr});
		}
	}
	
	this.subscribedTo = function(topic) {
		// With arg, return whether we are
		// subscribed to that topic; without topic arg,
		// return all topic we are subscribed to.
		
		subscribedTopics = txtArrayToStr(document.getElementById('topicsToRx').innerHTML);
		topicArr = subscribedTopics.split(/,[ ]*/);
		
		if (topic === undefined) {
			// Return all topics we are subscribed to:
			return topicArr;
		}
		return (topicArr.indexOf(topic) > -1);
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
