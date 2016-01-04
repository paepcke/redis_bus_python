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

function busInteractor() {

	/* ------------------------------------ Constants ------------------*/

	// Enforce singleton: official way to get the
	// singleton instance of this class is to call
	//   busInteractor.getInstance([callbackFuncsObj])
	// but if this func/calls is simply called:
	//     busInteractor()
	// then make sure we don't run through the func
	// def again:
	if (typeof my !== 'undefined') {
		return  my.instance;
	}
	
	// Make a private object in which we'll 
	// stick instance vars and private methods:
	var my = {};

	my.instance = null;

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
	
	my.MAX_CONNECT_WAIT_TIME = 2000 // 2 seconds
	
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
			if (typeof callbackSpecs.bridgeHost !== 'undefined') {
				my.setBridgeHost(callbackSpecs.bridgeHost);
			}
		}
		return my.instance;
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
		
		my.connectAttemptTime = new Date();
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
		
		my.ws.onopen = function() {
		    my.keepAliveTimer = window.setInterval(function() { my.sendKeepAlive(); }, 
		    									   my.keepAliveInterval);
		};
	
		my.ws.onclose = function() {
			if (my.keepAliveTimer !== null) {
		    	clearInterval(my.keepAliveTimer);
			}
		    errCallback("The browser or server closed the connection, or network trouble; please reload the page to resume.");
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
					errCallback("The SchoolBus test server seems to be unreachable.")
				} else {
					errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
				}
			
			} catch(err) {
				errCallback("The browser has detected an error while communicating with the data server: " + evt.data);
			} 
		}

		my.ws.onmessage = function(evt) {
		    // Internalize the JSON
		    // e.g. "{resp : "courseList", "args" : ['course1','course2']"
		    try {
			//var oneLineData = evt.data.replace(/(\r\n|\n|\r)/gm," ");
			argsObj = JSON.parse(evt.data);
		    } catch(err) {
		    	errCallback('Error report from server (' + evt.data + '): ' + err );
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
			errCallback("JS->Bus bridge sent an empty response.");
			return;
		}
		
		var resp = argsObj.resp	
		if (typeof resp != "string") {
			errCallback("JS->Bus bridge sent non-string response: " + String(resp));
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
		// Regular msg?:
		if (resp == 'msg') {
			// Regular message arrived:
			str = String(argsObj.time) + ' (' + String(argsObj.topic) + "): " + String(argsObj.content) + '\r\n';
			msgCallback(str);
			return;
		}
		if (resp == 'error') {
			// Error message arrived:
			str = "Error: " + String(argsObj.content);
			errCallback(str);
			return;
		}
		// If we get here the server sent an unknown response
		errCallback("JS->Bus bridge sent unknown response: " + String(resp));
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
	
	// Make the object we'll actually return:
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
	my.initWebsocket();
	my.instance = that;
	return that;
}
//if (typeof busInteractor.prototype.instance === 'undefined') {
if (typeof document.__busInteractor_instance === 'undefined') {
	 document.__busInteractor_instance = busInteractor(alert, alert);
	busInteractor.getInstance = document.__busInteractor_instance.getInstance;
}
