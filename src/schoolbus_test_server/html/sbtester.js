function SbTesterControl() {

	/* ------------------------------------ Instance Vars ------------------*/

	var originHost = null;
	var originPort = 8000;
	var originDir  = 'bus';
	var getCmds = {"startServerBtn" : "startServer",
				   "stopServerBtn"  : "stopServer",
				   "standardLen" 	: "strLen",
				   "oneShotTopic"   : "oneShotTopic",
				   "oneShotContent" : "oneShotContent",
				   "streamTopic"	: "streamTopic",
				   "streamContent"	: "streamContent",
				   "syntaxTopic"	: "syntaxTopic",
				   "discardTopics"	: "discardTopics"
					};
	
	
	/* ------------------------------------ Methods ------------------------*/
	
	this.construct = function() {
		originHost = window.location.host;
	}();
	
	var startServer = function() {
		httpGet({'server' : 'on'});
	}

	var startServer = function() {
		httpGet({'server' : 'off'});
	}
	
	var httpGet = function (parmsDict) {
		paramStr = '';
		for (var prop in Object.getOwnPropertyNames(parmsDict)) {
			if (paramDict[prop] === undefined) {
				// Make HTML parm val an underscore to coax
				// tornado at the server to include the prop
				// name in its dict:
				paramStr += '&' + prop + '=_'
				}
			else {
				paramStr += '&' + prop + '=' + paramDict[prop];
			}
		}
		// Remove the leading '&' from the GET param str:
		if (paramStr[0] === '&') {
			paramStr = paramStr[1];
		}
		
		theUrl = 'http://' + originHost + ':' + originPort + '/' + originDir + '?' + paramStr
	    var xmlHttp = new XMLHttpRequest();
	    xmlHttp.open( "GET", theUrl, false );
	    xmlHttp.send( null );
	    //***********
	    console.log('Ret: ' + responseText);
	    //***********
	    return xmlHttp.responseText;
	}	
}

document.getElementById('startServerBtn').addEventListener('click', SbTesterControl.startServer);
document.getElementById('stopServerBtn').addEventListener('click', SbTesterControl.stopServer);
//document.getElementById('submitBtn').addEventListener('click', SbTesterControl.stopServer);

new SbTesterControl();
