function SbTesterControl() {

	/* ------------------------------------ Instance Vars ------------------*/

	var originHost  = 'localhost';
	var originPort  = 8000;
	// String that shows up in 
	var originDir   = 'bus/controller';
	
	var chkBoxes = ['streaming', 'echo', 'chkSyntax'];

	// Will be filled by constructor with 
	// all UI elements as keys, and empty strings
	// as values. When a request for a param change
	// on the server is made, just that parameter
	// value is modified in reqTemplate:
	var reqTemplate = {};
	
	// Dict mapping UUIDs from test server to
	// server-selection radio button widgets:
	var testServers = {}
	
	/* ------------------------------------ Methods ------------------------*/
	
	this.construct = function() {
		// Note URL of the host that pulled this JS file
		// to its browser:
		if (window.location.host.length != 0) {
			originHost = window.location.host;
		};
		
		var serverParmForm = document.forms['serverParms'];
		for (var i=0; i < serverParmForm.length; i++) {
			widget = serverParmForm[i];
			if ((widget.type == 'text' ||
				 widget.type == 'checkbox' ||
				 widget.type == 'radio') && 
				widget.id.length > 0){
			reqTemplate[widget.id] = '';
			}
		}
		
	}();
	
	this.startServer = function() {
		sendReq({'server' : 'on'});
	}

	this.submit = function() {
		parmsDict = {'strLen' : document.getElementById('strLen').value,
					 'oneShotTopic' : document.getElementById('oneShotTopic').value,
					 'oneShotContent' : document.getElementById('oneShotContent').value,
					 'streamTopic' : document.getElementById('streamTopic').value,
					 'streamContent' : document.getElementById('streamContent').value,
					 'syntaxTopic' : document.getElementById('syntaxTopic').value,
					 'discardTopics' : document.getElementById('discardTopics').value,
					 
					 'streaming' : document.getElementById('streaming').checked ? 'True' : 'False',
					 'echo' : document.getElementById('echo').checked ? 'True' : 'False',
					 'chkSyntax' : document.getElementById('chkSyntax').checked ? 'True' : 'False',
		}
		sendReq(parmsDict);
	}
	
	var sendReq = function (parmsDict) {
		if (sendReq.locked) {
			return undefined
		} else {
			sendReq.locked = true;
		}
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

		// Add the server UUID so that the test server can
		// find the already existing SchoolBus server:
		var uuid = getCheckedServerId();
		
		// If uuid is the placeholder '_' then 
		// set the server id in the request dict 
		// to the empty string; else to the uuid:
		
		newReqDict['server_id'] = uuid == '_' ? '' : uuid; 
		
		theUrl = 'http://' + originHost + '/' + originDir;

	    var xmlHttp = new XMLHttpRequest();
	    xmlHttp.onreadystatechange = function() {
	    	if (xmlHttp.readyState == 4) {
	    		if (xmlHttp.status != 200) {
	    			alert('Server error ' + xmlHttp.status)
	    		}
	    		try {
				    //***********
				    console.log('Ret: ' + xmlHttp.responseText);
				    //***********
				    respDict = JSON.parse(xmlHttp.responseText);
				    processServerResponse(respDict);
	    		} finally {
					sendReq.locked = false;
	    		}
	    	}
	    }
	    
	    // The 'false' means: call synchronously:
	    xmlHttp.open( "POST", theUrl, true );
	    xmlHttp.send( JSON.stringify( newReqDict ) );
	}
	sendReq.locked = false;

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
		
		serverParmNames = Object.getOwnPropertyNames(respDict);
		if (serverParmNames.indexOf('error') != -1) {
			alert('Test server error: ' + respDict['error'])
			return
		}
		
		// Grab the server_id from the return:
		serverId = respDict['server_id'];
		serverRadioBtn = getCheckedServerRadioBtn();
		testServers[serverId] = serverRadioBtn;
		serverRadioBtn.id = serverId;
		
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
	
	var cloneReqTemplate = function() {
		var newTemplate = {}
		propNames = Object.getOwnPropertyNames(reqTemplate);
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
		res = ''
		for (var i=0; i<obj.length; i++) {
			element = obj[i];
			if (res.length != 0) {
				res += ', ';
			} 
			if (Object.prototype.toString.call(element) !== '[Object String]') {
				// Against promise: an element isn't a string:
				res += Object.prototype.toString.call(element);
			} else {
				res += element
			}
		}
		return res;
	}
}

var sbTesterControl = new SbTesterControl();

// Fill in the fields with actual server parm values:
sbTesterControl.submit();

document.getElementById('startServerBtn').addEventListener('click', sbTesterControl.startServer);
document.getElementById('submitBtn').addEventListener('click', sbTesterControl.submit);

