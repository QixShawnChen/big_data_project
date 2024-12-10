'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')

const url = new URL(process.argv[3]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 80 : 443, // http or https defaults
	protocol: url.protocol.slice(0, -1), 
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

hclient.table('qixshawnchen_final_project_hbase').row('USA_2017').get((error, value) => {
	console.info(value)
})

function counterToNumber(c) {
	const buffer = Buffer.from(c, 'latin1');
	if (buffer.length < 8) {
		throw new Error("Buffer is too small");
	}
	return Number(buffer.readBigInt64BE());
}

function rowToMap(row) {
	// Check if the row is null or undefined
	if (!row || row.length === 0) {
		// Return a default object with all fields set to 'N/A'
		return {
			"info:country_id": "N/A",
			"info:country_name": "N/A",
			"info:year": "N/A",
			"info:NGDP_RPCH": "N/A",
			"info:PCPIPCH": "N/A",
			"info:PPPPC": "N/A",
			"info:PPPGDP": "N/A",
			"info:LP": "N/A",
			"info:BCA": "N/A",
			"info:LUR": "N/A",
			"info:rev": "N/A",
			"info:GGXCNL_NGDP": "N/A",
			"info:NGS_GDP": "N/A",
			"info:GGXCNL_GDP": "N/A",
			"info:NI_GDP": "N/A"
		};
	}

	// Process the row normally if it exists
	const stats = {};
	row.forEach(function (item) {
		stats[item['column']] = item['$'];
	});
	return stats;
}


/*
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = item['$'];
	});
	return stats;
}

hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

*/
app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
	const year = parseInt(req.query['year']);
	const route=req.query['origin'] + '_' + req.query['year'];
	console.log(route);
	const tableName = year > 2023 ? 'qixshawnchen_final_project_speed_hbase' : 'qixshawnchen_final_project_hbase';
	hclient.table(tableName).row(route).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_delay(key) {
			var delayValue = weatherInfo["info:" + key];
			var delays = parseFloat(delayValue);
			return delays.toFixed(1);
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			dest : req.query['year'],
			country_name: weatherInfo['info:country_name'] || 'N/A',
			NGDP_RPCH: weather_delay('NGDP_RPCH') || 'N/A',
			PCPIPCH: parseFloat(weatherInfo['info:PCPIPCH']) || 'N/A',
			PPPPC: parseFloat(weatherInfo['info:PPPPC']) || 'N/A',
			PPPGDP: parseFloat(weatherInfo['info:PPPGDP']) || 'N/A',
			LP: parseFloat(weatherInfo['info:LP']) || 'N/A',
			BCA: parseFloat(weatherInfo['info:BCA']) || 'N/A',
			LUR: parseFloat(weatherInfo['info:LUR']) || 'N/A',
			rev: parseFloat(weatherInfo['info:rev']) || 'N/A',
			GGXCNL_NGDP: parseFloat(weatherInfo['info:GGXCNL_NGDP']) || 'N/A',
			NGS_GDP: parseFloat(weatherInfo['info:NGS_GDP']) || 'N/A',
			GGXCNL_GDP: parseFloat(weatherInfo['info:GGXCNL_GDP']) || 'N/A',
			NI_GDP: parseFloat(weatherInfo['info:NI_GDP']) || 'N/A',
		});
		res.send(html);
	});
});


app.listen(port);
