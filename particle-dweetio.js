// Require required stuff
var spark = require('spark'),
    https = require('https');

// Required variables
var ACCESS_TOKEN = (process.env.ACCESS_TOKEN ? process.env.ACCESS_TOKEN : '').trim();


// Optional variables
var EVENT_NAME = (process.env.EVENT_NAME ? process.env.EVENT_NAME : 'statsd');
var FORWARD_SPARK = (process.env.FORWARD_SPARK ? process.env.FORWARD_SPARK : 0);
var SPARK_PATH = (process.env.SPARK_PATH ? process.env.SPARK_PATH : 'spark');
var DWEETIO_PREFIX = (process.env.DWEETIO_PREFIX ? process.env.DWEETIO_PREFIX : '');

// I said it was required!
if(ACCESS_TOKEN.length==0) {
	console.error('You MUST provide a Particle access token');
	process.exit(1);
}


// Login to the cloud
spark.login({accessToken: ACCESS_TOKEN}, function() {
	subscribe_event();
	subscribe_spark();
});


// A queue to hold all the Dweets to go out
var dweet_queue = [];
process_queue();


// Subscription handler
function subscribe_event() {
	console.time('subscribe');
	_log('subscription started');

	// Subscribe
	var req = spark.getEventStream(EVENT_NAME, 'mine', function(data) {
		stats_parse(data);
	});

	// Re-subscribe
	req.on('end', function() {
		_log('subscription ended');
		console.time('subscribe');

		// Re-subscribe in 1 second
		setTimeout(subscribe_event, 1000);
	});
}


function subscribe_spark() {
	// Exit early if disabled
	if(FORWARD_SPARK!=1) return;

	console.time('spark_subscribe');
	_log('spark subscription started');

	// Subscribe
	var req = spark.getEventStream('spark', 'mine', function(data) {
		spark_parse(data);
	});

	// Re-subscribe
	req.on('end', function() {
		_log('spark subscription ended');
		console.time('spark_subscribe');

		// Re-subscribe in 1 second
		setTimeout(subscribe_spark, 1000);
	});
}


// Parse the data from the event
function stats_parse(data) {
	var msg, device_name;
	
	_log('>>>', data);

	// If a semi-colon exists, treat the string before it as the device name	
	if(data.data.indexOf(';')>0) {
		var semicolon_split = data.data.split(';');
		device_name = semicolon_split[0];
		data.data = semicolon_split[1];

	// If no custom device name, use the coreid instead
	} else
		device_name = data.coreid;
	
	// Check for the presence of a comma to see if we have multiple metrics in this payload
	// No comma = 1 metric
	if(data.data.indexOf(',')<0) {
		stats_send(device_name+'.'+data.data);

	// Commas = multiple metrics
	} else {
		// Split metrics into an array
		var data_arr = data.data.split(',');

		// Loop through the array and send the metrics		
		for(var i=0; i<data_arr.length; i++)
			stats_send(device_name+'.'+data_arr[i]);
	}
}


// Parse spark event data
function spark_parse(data) {
	_log('>>>', data);
	var path = data.name.replace('\/', '.');

	stats_send(SPARK_PATH+'.'+data.coreid+'.'+path+'.'+data.data+':1|c');
}


// Send the parsed metrics to StatsD
function stats_send(msg) {
	// Splits "overwinter1.h:34.37|g" into "overwinter1.h", "34.37|g"
	var a = msg.split(':');

	// Splits "34.37|g" into "34.37", "g"
	var b = a[1].split('|');

	// Friendly variable names
	var c = a[0].split('.');
	var thing = c[0];
	var key = c[1];
	var val = b[0];


	// If a prefix was given, try to "dweet_for"
	if(DWEETIO_PREFIX.length>0) {
		var dweetio_path = '/for/'+DWEETIO_PREFIX+thing+'?'+key+'='+val;
	// Otherwise, just accept what we'll be called
	} else {
		var dweetio_path = '/?name='+thing+'&'+key+'='+val;
	}

	dweet_queue.push(dweetio_path);
}


function process_queue() {
	if(dweet_queue.length==0) {
		setTimeout(process_queue, 1000);
		return;
	}

	var dweetio_path = dweet_queue.shift();

	_log('(INFO)', 'Queue remaining:', dweet_queue.length);

	https.get('https://dweet.io/dweet'+dweetio_path, function(res) {
		res.on('data', function(data) {
			var d = JSON.parse(data.toString());

			// '{"this":"succeeded","by":"dweeting","the":"dweet","with":{"thing":"wgb_lola","created":"2016-02-15T05:26:11.869Z","content":{"h":18.5},"transaction":"3429f845-21fa-4adf-be77-ce17eae18003"}}' ]
			_log('<<<', d.with.thing, d.this, JSON.stringify(d.with.content));

			setTimeout(process_queue, 1000);
		});
	}).on('error', function(err) {
		_log('<<< !!!', dweetio_path, err);
		setTimeout(process_queue, 1000);
	});
}


// Semi-fancy logging with timestamps
function _log() {
	var d = new Date();

	// Year
	d_str = d.getFullYear();
	d_str += '-';

	// Month
	if(d.getMonth()+1<10) d_str += '0';
	d_str += (d.getMonth()+1);
	d_str += '-';

	// Day
	if(d.getDate()<10) d_str += '0';
	d_str += d.getDate();
	d_str += ' ';

	// Hour
	if(d.getHours()<10) d_str += '0';
	d_str += d.getHours();
	d_str += ':';

	// Minute
	if(d.getMinutes()<10) d_str += '0';
	d_str += d.getMinutes();
	d_str += ':';

	// Second
	if(d.getSeconds()<10) d_str += '0';
	d_str += d.getSeconds();
	d_str += '.';

	// Milliseconds
	d_str += d.getMilliseconds();


	if(arguments.length==1)
		var l = arguments[0];
	else {
		var l = [];
		for(var i=0; i<arguments.length; i++)
			l.push(arguments[i]);
	}
			

	console.log('['+d_str+']', l);
}
