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

	var that = this
	
	var msgCallback = msgCallback;
	var errCallback = errCallback;
	if (typeof busBridgeHost === 'undefined') {
		originHost = 'localhost';
	} else {
		originHost = busBridgeHost;
	}

	var USE_SSL = false;
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
	var connectAttemptTime = null;
	
	that.ws = null;
	
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

	var initWebsocket = function() {
		if (USE_SSL) {
			ws = new WebSocket("wss://" + originHost + ':' + controllerWebsocketPort + originDir);
		} else {
			ws = new WebSocket("ws://" + originHost + ':' + controllerWebsocketPort + originDir);
		}
		
		ws.onopen = function() {
		    keepAliveTimer = window.setInterval(function() {sendKeepAlive()}, keepAliveInterval);
		};
	
		ws.onclose = function() {
			if (keepAliveTimer !== null) {
		    	clearInterval(keepAliveTimer);
			}
		    errCallback("The browser or server closed the connection, or network trouble; please reload the page to resume.");
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
					errCallback("The SchoolBus test server seems to be unreachable.")
				} else {
					errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
				}
			
			} catch(err) {
				errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
			} 
		}

		ws.onmessage = function(evt) {
		    // Internalize the JSON
		    // e.g. "{resp : "courseList", "args" : ['course1','course2']"
		    try {
			//var oneLineData = evt.data.replace(/(\r\n|\n|\r)/gm," ");
			argsObj = JSON.parse(evt.data);
		    } catch(err) {
		    	errCallback('Error report from server (' + evt.data + '): ' + err );
			return
		    }
		    processServerResponse(argsObj);
		}
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

	var sendReq = function(msgDict) {
		if (ws === null) {
			initWebsocket();
		}
		if (ws.readyState != WEBSOCKET_READY_STATE) {
			ws.onreadystatechange = function(msg) {
				if (ws.readyState == WEBSOCKET_READY_STATE) {
					ws.send(JSON.stringify(msgDict));
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
		sendReq({"cmd" : "subscribe", "topic" : topic});
	}
	
	this.unsubscribeFromTopic = function(topic) {
		sendReq({"cmd" : "unsubscribe", 'topic' : topic});
	}
	
	this.subscribedTo = function() {
		// Request list of all topics we are subscribed to.
		// Result will arrive asynchronously.
		sendReq({"cmd" : "subscribed_to"})
	}
	
	this.publish = function(str, topic) {
		sendReq({"cmd" : "publish", "topic" : topic});
	}
	
	var processServerResponse = function(argsObj) {
		/*
		 * Called when a bus message arrives from the bridge,
		 * or when the bridge responds to an earlier request
		 * for the subscribed-to topics. The difference is
		 * detected by the value of the "resp" key. If it is
		 * an array, it's a list of subscribed-to topics.
		 * Else it's a msg or error string:
		 * 
		 *      {"resp": ["topic1", "topic2", ...]}
		 * vs:
		 *      {"resp" : "theMessageContent", 
         *                "topic" : "theTopic",
         *                "time" : "isoSendTimeStr"}
         *                
         * vs:  {"error" : "errMsg"}
		 */
		var resp = argsObj.resp
		if (isArray(resp)) {
			var topics = txtArrayToStr(resp);
			document.getElementById('topics').innerHtml = topics;
			return;
		}
		// Error msg?:
		if (typeof argsObj.resp !== 'undefined') {
			// Regular message arrived:
			str = argsObj.time + ' (' + argsObj.topic + "): " + argsObj.resp + '\r\n';
			msgCallback(str);
			return;
		}
		if (typeof argsObj.error !== 'undefined') {
			// Error message arrived:
			str = "Error: " + argsObj.error;
			errCallback(str);
			return;
		}
		// If we get here the server sent an unknown response
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
