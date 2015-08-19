/**
 * Bindings of car engine image areas to requests for Wikipedia 
 */
var wireImageRegions = function() {
    carMap = document.getElementById('carEngine');
    areas  = carMap.areas;
    for (var i=0; i<areas.length; i++) {
	//areas[i].addEventListener("click", areaClickHandler);
	areas[i].addEventListener('click', function(evt) {
		// Don't follow the (random) URL
		// associated with the area's src;
		// We make a bus request instead:
		evt.preventDefault();
		
		// If not subscribed to 'tmp.*' then
		// subscribe temporarily:
		wasTmpSubscribed = parent.window.sbTesterControl.subscribedTo('tmp.*');
		if (! wasTmpSubscribed) {
			parent.window.sbTesterControl.subscribeToTopic('tmp.*');
		}
		
		// Area object user clicked on:
		clickedImgArea = evt.currentTarget;
		partName = clickedImgArea.alt;
		
		parent.window.sbTesterControl.sendReq(
		    {"oneShot" : {"topic" : partName, "summary" : "1"},
		     "oneShotTopic" : 'wikipedia'
		    }
		);
		return false;
	    }, false);
    }
}
wireImageRegions();
