// console.log(cointypes['btc'].length);
// console.log(cointypes.ltc.length);
// console.log(cointypes['bch'].length);
// console.log(cointypes['eth'].length);
// console.log(cointypes['etc'].length);

var d = new Date();
// console.log(d.toDateString())

var localDate = d.toLocaleString('en-US', {
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });

var DateStr = localDate.substring(6, 10) + "-" + localDate.substring(0, 2) + "-" + localDate.substring(3, 5);
console.log(DateStr);

var assetSelect = document.getElementById('assetslt');
var typeSelect = document.getElementById('typeslt');
var startDate = document.getElementById('startDate');
var endDate = document.getElementById('endDate');

startDate.max = DateStr;
endDate.max = DateStr;


var submitButton = document.getElementById('submit');

assetSelect.onchange = function(){updataTypeSelector();};
startDate.onchange = function(){updateEndDateSelector();};

function updataTypeSelector(){
	var asset = assetSelect.options[assetSelect.selectedIndex].text;
	for (var i = 0; i < cointypes[asset].length; i++) {
		var str = cointypes[asset][i];
		typeSelect.options[i] = new Option(str, str);
	}
	while(typeSelect.options.length > cointypes[asset].length){
		typeSelect.options[typeSelect.options.length-1] = null;
	}
}

function updateEndDateSelector(){
	var start = String(startDate.value);
	endDate.min = start;
}

function load(){
	assetSelect.selectedIndex = -1;
	typeSelect.selectedIndex = -1;
	createChart();
}

function search(){
	if(assetSelect.selectedIndex == -1){
		alert("Please select asset!");
		return;
	}else if(typeSelect.selectedIndex == -1){
		alert("Please select dataType!");
		return;
	}else if(startDate.value == ""){
		alert("Please select start date!");
		return;
	}else if(endDate.value == ""){
		alert("Please select end date!");
		return;
	}
	console.log(endDate.value);
	console.log(startDate.value);
	var list = [];
	var asset = assetSelect.options[assetSelect.selectedIndex].text;
	var dataType = typeSelect.options[typeSelect.selectedIndex].text;
	var start = String(startDate.value);
	var end = String(endDate.value);


	// if(start.replace("-", "") )

	var post_data = {
		"asset": asset,
		"dataType": dataType,
		"start": start,
		"end": end,
	};
	console.log(post_data);
	$.ajax({
	    url: updateurl,
		    type: "POST",
	        data: post_data,
	        success: function (data) {
		        data = JSON.parse(data);
		        if (data["status"] == 0){
		        	alert(data["message"]);
		        }else{
		        	createChart(String(asset), String(dataType), data);  
		        }
		          
			}
    });

		
}


function createChart(asset="asset", dataType="dataType", data=[]){
	var datapoints = [];
	for(var i = 0; i < data.length; i++){
		var date = data[i][0];
		var value = data[i][1];
		var testDate = new Date(date.substring(0, 4), Number(date.substring(5, 7))-1, date.substring(8, 10));
		datapoint = {x: testDate, y: Number(value)};
		datapoints.push(datapoint);
		console.log(value);
	}

	var titleText = String(asset)+"-"+String(dataType)+" line chart";
	var chart = new CanvasJS.Chart("chartContainer", {
		animationEnabled: true,
		theme: "light2",
		title:{
			text: titleText
		},
		axisX:{
			title: "Date",
			valueFormatString: "MM DD YYYY"
		},
		axisY: {
			title: String(dataType),
			includeZero: false,
			scaleBreaks: {
				autoCalculate: true
			}
		},

		data: [{        
			type: "line",  
			xValueFormatString: "MM DD YYYY",
			color: "#F08080",     
			dataPoints: datapoints
		}]
	});
	chart.render();
}
