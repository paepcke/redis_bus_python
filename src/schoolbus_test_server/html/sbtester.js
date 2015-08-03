function SbTesterControl() {

	/* ------------------------------------ Instance Vars ------------------*/

	var originHost = '192.168.0.19';
	var originPort = 8000;
	var originDir  = 'bus/controller';
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
	
	var httpGet = function (parmsDict) {
		parmStr = '';
		parmNames =  Object.getOwnPropertyNames(parmsDict);
		for (var indx=0; indx < parmNames.length; indx++) {
			propName = parmNames[indx]
			if (parmsDict[propName] === undefined) {
				// Make HTML parm val an underscore to coax
				// tornado at the server to include the prop
				// name in its dict:
				parmStr += '&' + propName + '=_'
				}
			else {
				parmStr += '&' + propName + '=' + parmsDict[propName];
			}
		}
		// Remove the leading '&' from the GET param str:
		if (parmStr[0] === '&') {
			parmStr = parmStr.slice(1);
		}
		
		theUrl = 'http://' + originHost + '/' + originDir + '?' + parmStr
	    var xmlHttp = new XMLHttpRequest();
	    xmlHttp.open( "GET", theUrl, false );
	    xmlHttp.send( null );
	    //***********
	    console.log('Ret: ' + xmlHttp.responseText);
	    //***********
	    return xmlHttp.responseText;
	}	
}

var sbTesterControl = new SbTesterControl();


document.getElementById('startServerBtn').addEventListener('click', sbTesterControl.startServer);
document.getElementById('stopServerBtn').addEventListener('click', sbTesterControl.stopServer);
//document.getElementById('submitBtn').addEventListener('click', sbTesterControl.stopServer);

