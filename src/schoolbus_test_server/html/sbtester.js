function SbTesterControl() {

	/* ------------------------------------ Instance Vars ------------------*/

	var originHost  = 'localhost';
	var originPort  = 8000;
	// String that shows up in 
	var originDir   = 'bus/controller';
	
	var chkBoxes = ['stream', 'echo', 'chkSyntax'];

	// Will be filled by constructor with 
	// all UI elements as keys, and empty strings
	// as values. When a request for a param change
	// on the server is made, just that parameter
	// value is modified in reqTemplate:
	var reqTemplate = {};
	
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
			if (widget.type == 'text' ||
				widget.type == 'checkbox' ||
				widget.type == 'radio') {
			reqTemplate[widget.id] = '';
			}
		}
	}();
	
	this.startServer = function() {
		sendReq({'server' : 'on'});
	}

	this.stopServer = function() {
		sendReq({'server' : 'off'});
	}
	
	this.submit = function() {
		parmsDict = {'strLen' : document.getElementById('strLen').innerHTML,
					 'oneShotTopic' : document.getElementById('oneShotTopic').innerHTML,
					 'oneShotContent' : document.getElementById('oneShotContent').innerHTML,
					 'streamTopic' : document.getElementById('streamTopic').innerHTML,
					 'streamContent' : document.getElementById('streamContent').innerHTML,
					 'syntaxTopic' : document.getElementById('syntaxTopic').innerHTML,
					 'discardTopics' : document.getElementById('discardTopics').innerHTML,
					 
					 'stream' : document.getElementById('stream').checked ? 'True' : 'False',
					 'echo' : document.getElementById('echo').checked ? 'True' : 'False',
					 'chkSyntax' : document.getElementById('chkSyntax').checked ? 'True' : 'False',
		}
		sendReq(parmsDict);
	}
	
	var sendReq = function (parmsDict) {
		
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

		theUrl = 'http://' + originHost + '/' + originDir;

	    var xmlHttp = new XMLHttpRequest();
	    xmlHttp.open( "POST", theUrl, false );
	    xmlHttp.send( JSON.stringify( newReqDict ) );
	    
	    //***********
	    console.log('Ret: ' + xmlHttp.responseText);
	    //***********
	    return xmlHttp.responseText;
		
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
		
		serverParmNames = Object.getOwnPropertyNames(respDict);
		for (var i=0; i<serverParmNames.length; i++) {
			parmName = serverParmNames[i];
			newVal = respDict[parmName];
			
			// If value is for a checkbox, turn the server-returned
			// 'True', 'False' into lower case, and use the proper
			// checkbox setting syntax:
			if (chkBoxes.indexOf(parmName) != -1) {
				// It's a checkbox:
				document.getElementById(parmName).checked = newVal.toLowerCase();
			} else {
				document.getElementById(parmName).innerHTML = newVal;
			}
		}
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

