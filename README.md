particle-dweetio
===================

A simple Node.JS daemon that listens for published events on your Particle event stream, parses them, and pushes them to [dweet.io](http://dweet.io).


Installation
------------

1. Clone this repository
2. Change to repo directory (`cd particle-dweetio`)
3. Run `npm install` to install dependencies
4. Run using `node particle-dweetio.js` or use any process manager (nodemon, foreverjs, pm2)


Options
-------
Options are now set via environment variables.  Available options are:

 - `ACCESS_TOKEN` - (Required) Your Particle cloud access token
 - `DWEETIO_PREFIX` - Prefix to prepend to the names of your things.  For example, `my_unique_prefix_` would become `my_unique_prefix_mydevice` when pushed to Dweet.io.
 - `EVENT_NAME` - The name of the event to listen for - default: `statsd`
 - `FORWARD_SPARK` - Parse `spark/*` events. `0` = Disable, Any other value = Enable - default: `1`
 - `SPARK_PATH` - The metric path prefix for `spark/*` events - default: `spark`

Data format
-----------
Data format is: `[device name]`;`[metric name]`:`[metric value]`|`[metric type]`,`[metric name]`:`[metric value]`|`[metric type]`

 - `device name` - (Optional)  If not specified with a name (followed by a semi-colon), the Particle device ID will be used instead.
 - `metric name` - The name of the metric you wish to record.  Keep it short so you can fit more data in a single publish.
 - `metric value` - The value of the metric you wish to record.
 - `metric type` - The StatsD metric type to use.

Multiple metrics can be passed as long as each metric set (`metric name`, `metric value`, and `metric type`) is separated by commas.

In it's current state, this does _NOT_ "remember" names given to devices if you do not specify a `DWEETIO_PREFIX` environment variable.  In other words, it's not technically required, but you should treat it as such.

_Note that this keeps the same format as the [particle-statsd](https://github.com/wgbartley/particle-statsd) repo so it's easier to switch amongst the two (or run them simultaneously)._
