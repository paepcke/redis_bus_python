function installHandlers() {
	document.getElementById('startServerBtn').addEventListener('click', SbTesterControl.startServer);
	document.getElementById('stopServerBtn').addEventListener('click', SbTesterControl.stopServer);
	
}