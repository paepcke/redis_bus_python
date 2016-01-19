/*
 * Generic javascript access to SchoolBus bridge.
 * Communicates via websocket to a server that
 * forwards published messages to the bus, and
 * delivers incoming messages for subscribed topics
 * to the Web browser where this busInteractor runs.
 * The generic server to match this client is 
 * js_bus_bridge.py. It must be running, and its
 * protocol expectations are matched with this
 * client.
 * 
 * This client has two options, both callback functions:
 * msgCallback() is called when incoming messages 
 * arrive from the SchoolBus. Function errCallback()
 * is called when errors are encountered. Defaults
 * for these callbacks is the built-in alert() function.
 * 
 *  This client is a singleton. Get that one instance
 *  via:
 *       busInteractor.getInstance([specs])

 *  where the optional 'specs' is an object:
 *  
 *     {"msgCallback" : msg_callback_func(msg),
 *      "errCallback" : err_callback_func(errMsg)
 *     }
 *  Either of these keys may be omitted. Either
 *  value can be changed at runtime via:
 *  
 *     setMsgCallback() and
 *     setErrCallback().
 *     
 *  the msg parameter is a JS object with keys
 *  'time', 'topic', and 'content'. The errMsg
 *  parameter is an object with keys 'time', and
 *  'content'. Time will be in ISO string format. 
 *     
 *  API:
 *  	subscribeToTopic(topic)     : subscribe to topic       
 * 		unsubscribeFromTopic(topic) : unsubscribe from topic
 *		subscribedTo()              : get list of subscriptions -> stringified array of str
 *		publish(msg,topic)          : publish text msg to topic
 *		setMsgCallback(callback)    : change msgCallback
 *		setErrCallback(callback)	: change errCallback
 *		getMsgCallback()		    : get current msgCallback
 *		getErrCallback()			: get current errCallback
 * 
 */

function busInteractor() {

	/* ------------------------------------ Constants ------------------*/

	// Enforce singleton: official way to get the
	// singleton instance of this class is to call
	//   busInteractor.getInstance([Callbackfuncsobj])
	// but if this func/calls is simply called:
	//     busInteractor()
	// then make sure we don't run through the func
	// def more than once (we do need to run through
	// it once):
	if (typeof my !== 'undefined') {
		throw "Please obtain the busInteractor instance via busInteractor.getInstance([Callbackfuncsobj])." 
	}
	
	// Make a private object in which we'll 
	// stick instance vars and private methods:
	var my = {};

	my.instance = null;
	my.instancePromise = null; //*****
	
	// Default message and error funcs;
	// They are typically replaced when
	// a client calls busInteractor.getInstance():
	my.msgCallback = function(msg) { alert(msg); };
	my.errCallback = function(msg) { alert(msg); };

	my.USE_SSL = false;
	my.controllerWebsocketPort  = 4363;
	// URL part after the domain and port.
	// Server expects websocket connections there:
	my.originDir   = '/jsbusbridge';

	// Websocket state 'ready for action'
	// (Note: for other types of sockets the
	// ready state is 1):
	my.WEBSOCKET_CONNECTING_STATE = 0;
	my.WEBSOCKET_READY_STATE = 1;
	my.WEBSOCKET_CLOSING_STATE = 2;
	my.WEBSOCKET_CLOSED_STATE = 3;
	
	// Total time to wait for the websocket
	// to the server to complete:
	my.MAX_CONNECT_WAIT_TIME = 2000 // 2 seconds
	// How often to check whether the initial
	// websocket connection has completed:
	my.INTER_CHECK_CONNECT_TIME = 50 //
	my.connectAttemptStartTime = 
	
	my.keepAliveInterval = 15000; /* 15 sec*/
	
	/* ------------------------------------ Instance Vars ------------------*/

	my.keepAliveTimer    = null;
	my.connectAttemptTime = null;
	
	my.ws = null;
	
	/* ------------------------------------ Methods ------------------------*/

	my.getInstance = function(callbackSpecs) {
		/**
		 * Enable getting an instance either
		 * via busInteractor.getInstance()
		 * or  <existingInstance>.getInstance()
		 */
		
		if (typeof callbackSpecs !== 'undefined') {
			if (typeof callbackSpecs.msgCallback !== 'undefined') {
				my.setMsgCallback(callbackSpecs.msgCallback);
			}
			if (typeof callbackSpecs.errCallback !== 'undefined') {
				my.setErrCallback(callbackSpecs.errCallback);
			}
		}
		my.instancePromise = new Promise(function(resolve, reject) {
			if (my.wsReady()) {
				resolve(my.instance);
				return my.instance;
			}
			// Schedule another check for ws-ready,
			// unless total allowed wait time has already
			// been exceeded:
			if (new Date() - my.connectAttemptTime < my.MAX_CONNECT_WAIT_TIME) {
				setTimeout(function() {
					if (my.wsReady()) {
						resolve(my.instance);
						return my.instance;
						}
				}, my.INTER_CHECK_CONNECT_TIME);
			}
			else {
				reject("Timeout while waiting for WebSocket connection to " +
						my.originHost + ':' + my.controllerWebsocketPort + my.originDir);
				return;
			}
				
			}) // end new Promise
		return my.instancePromise;
	}
	
	my.initialize= function() {
		// Note URL of the host that pulled this JS file
		// to its browser; that will be the host that also
		// listens for websocket connections:
		if (window.location.host.length != 0) {
			my.originHost = window.location.host;
			my.originHostAndPort = my.originHost.split(':');
			my.originHost = my.originHostAndPort[0];
			if (my.originHostAndPort.length > 1) {
				my.originPort = my.originHostAndPort[1];
			} else {
				my.originPort = undefined;
			}
		};
	};
	
	my.setMsgCallback = function(newMsgCallback) {
		if (typeof newMsgCallback !== "function") {
			throw "Call to setMsgCallback takes a function as argument; was passed an '" +
				   typeof newMsgCallback + "'."
		}
		my.msgCallback = newMsgCallback;
	}
	
	my.setErrCallback = function(newErrCallback) {
		if (typeof newErrCallback !== "function") {
			throw "Call to setErrCallback takes a function as argument; was passed an '" +
				   typeof newErrCallback + "'."
		}
		my.errCallback = newErrCallback;
	}
	
	my.getMsgCallback = function() {
		return my.msgCallback;
	}
	
	my.getErrCallback = function() {
		return my.errCallback;
	}
	
	my.sendKeepAlive = function() {
		//var req = buildRequest("keepAlive", "");
		var req = "keepAlive";
		if (my.ws === null) {
			my.initWebsocket();
		}
		my.ws.send(req);
	}

	my.initWebsocket = function() {
		
		if (my.USE_SSL) {
				my.ws = new WebSocket("wss://" + my.originHost + ':' + my.controllerWebsocketPort + my.originDir);
			} else {
				my.ws = new WebSocket("ws://" + my.originHost + ':' + my.controllerWebsocketPort + my.originDir);
		}
		// Note when we started the connection process:
		my.connectAttemptTime = new Date();
		
		my.ws.onopen = function() {
			/**
			 * Called when websocket successfully opened.
			 * If a client called getInstance() before the
			 * connection is complete, then a promise will
			 * be sitting unresolved in my.instancePromise.
			 * Since the connection succeeded, we fulfill the
			 * promise now: 
			 */
			//******
/*			if (my.instancePromise !== null) {
				my.instancePromise.resolve(my.instance);
			}
*/			//******
		    my.keepAliveTimer = window.setInterval(function() { my.sendKeepAlive(); }, 
		    									   my.keepAliveInterval);
		};
	
		my.ws.onclose = function() {
			if (my.keepAliveTimer !== null) {
		    	clearInterval(my.keepAliveTimer);
			}
		    my.errCallback("The browser or server closed the connection, or network trouble; please reload the page to resume.");
		}
	
		my.ws.onerror = function(evt) {
			if (my.keepAliveTimer !== null) {
		    	clearInterval(my.keepAliveTimer);
			}
			// We only get an event that says 'error'; make a guess
			// at what's wrong:
			try {
				if (Object.prototype.toString.call(evt.currentTarget) === "[object WebSocket]" &&
						evt.currentTarget.readyState === my.WEBSOCKET_CLOSED_STATE) {
					my.errCallback("The SchoolBus test server seems to be unreachable.")
				} else {
					my.errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
				}
			
			} catch(err) {
				my.errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
			} 
		}

		my.ws.onmessage = function(evt) {
		    // Internalize the JSON
		    // e.g. "{resp : "courseList", "args" : ['course1','course2']"
		    try {
			//var oneLineData = evt.data.replace(/(\r\n|\n|\r)/gm," ");
			argsObj = JSON.parse(evt.data);
		    } catch(err) {
		    	my.errCallback('Error report from server (' + evt.data + '): ' + err );
			return
		    }
		    my.processServerResponse(argsObj);
		}
	}
	
	my.wsReady = function() {
		if (my.ws === null) {
			my.initWebsocket();
		}
		return my.ws.readyState == my.WEBSOCKET_READY_STATE;
	}
	
	my.getWs = function() {
		return my.ws;
	}

	my.sendReq = function(msgDict) {
		if (my.ws === null) {
			my.initWebsocket();
		}
		if (my.ws.readyState != my.WEBSOCKET_READY_STATE) {
			my.onreadystatechange = function(msg) {
				if (my.ws.readyState == my.WEBSOCKET_READY_STATE) {
					my.ws.send(JSON.stringify(msgDict));
				} else {
					alert('Could not connect to server; timed out.');
				}
			};
			return;
		} else {
			my.ws.send(JSON.stringify(msgDict));
		}
	}
	
	my.subscribeToTopic = function(topic) {
		my.sendReq({"cmd" : "subscribe", "topic" : topic});
	}
	
	my.unsubscribeFromTopic = function(topic) {
		my.sendReq({"cmd" : "unsubscribe", 'topic' : topic});
	}
	
	my.subscribedTo = function(respCallback) {
		/*
		 * Request list of all topics we are subscribed to.
		 * Result will arrive asynchronously, and will
		 * be delivered to the provided callback function.
		 */
		// Callback will be invoked by processServerResponse().
		my.respCallback = respCallback;
		my.sendReq({"cmd" : "subscribed_to"})
	}
	
	my.publish = function(str, topic) {
		my.sendReq({"cmd" : "publish", "msg" : str, "topic" : topic});
	}
	
	my.processServerResponse = function(argsObj) {
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

		if (typeof argsObj != "object") {
			my.errCallback("JS->Bus bridge sent an empty response.");
			return;
		}
		
		var resp = argsObj.resp	
		if (typeof resp != "string") {
			my.errCallback("JS->Bus bridge sent non-string response: " + String(resp));
			return;
		}

		// Regular msg? (most common case:):
		if (resp == 'msg') {
			// Regular message arrived:
			//str = String(argsObj.time) + ' (' + String(argsObj.topic) + "): " + String(argsObj.content) + '\r\n';
			//my.msgCallback(str);

			// Callback with time, topic, and returned content:
			my.msgCallback(argsObj);
			return;
		}
		
		if (resp == "topics") {
			//var contents = my.txtArrayToStr(resp);
			content = argsObj["content"];
			if (typeof content === 'undefined' ||
				content.length == 0) {
				content = "no subscriptions";
			}
			my.msgCallback(String(content));
			return;
		}

		if (resp == 'error') {
			// Error message arrived:
			//str = "Error: " + String(argsObj.content);
			//my.errCallback(str);
			
			// Callback with time and error string:
			my.errCallback(argsObj);
			return;
		}
		// If we get here the server sent an unknown response
		my.errCallback("JS->Bus bridge sent unknown response: " + String(resp));
		return;
	}	
	
	my.isArray = function(obj) {
		return Object.prototype.toString.call( obj ) === '[object Array]';
	}
	
	my.txtArrayToStr = function(obj) {
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
	
	// Make the object we would actually return
	// if this wasn't a singlton:
	that = {}
	// Add a reference to the public ones of the above methods:
	that.subscribeToTopic = my.subscribeToTopic;
	that.unsubscribeFromTopic = my.unsubscribeFromTopic;
	that.subscribedTo = my.subscribedTo;
	that.publish = my.publish;
	that.setMsgCallback = my.setMsgCallback;
	that.setErrCallback = my.setErrCallback;
	that.getMsgCallback = my.getMsgCallback;
	that.getErrCallback = my.getErrCallback;
	that.getInstance = my.getInstance;
	
	my.initialize();
	my.instance = that;
	my.initWebsocket();
	
	busInteractor.getInstance = my.getInstance;
	
	return null;
}
// The above func adds the getInstance() function to
// the top level function busInteractor(). This happens
// way at the func's end. We can therefore determine
// whether busInteractor() ever ran by checking whether
// the function has the attr getInstance(). If not,
// the function is run, and the singleton instance is
// created:
if (typeof busInteractor.getInstance === 'undefined') {
	busInteractor(alert,alert);
	// Now the top level function ran, and Web
	// socket connecting is under way, but not done!
	// Clients do need to use promises.
}
