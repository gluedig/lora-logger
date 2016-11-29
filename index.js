//'use strict'

var config = require('./config');
const EventEmitter = require('events');
const pino = require('pino');

var pretty = pino.pretty();
pretty.pipe(process.stdout);
global.log = pino({level: "info"}, pretty);

var mongoose = require('mongoose');
var Promise = require('bluebird');
mongoose.Promise = Promise;

var LoraSchema = require('lora-mongoose-schema');
var LoraMessage, LoraMessageLog, LoraGateway;

class LoraDB {
    fill_gateway(eui) {
	var find = LoraGateway.findOne({gateway_eui: eui});
	find.exec().bind(this).then( function(gw) {
	    var this_meta;
	    this.metadata.forEach( (meta) => {
		if (meta.gateway_eui == eui) {
		    this_meta = meta;
		}
	    });
	    if (gw) {
		this_meta.gateway = gw;
		this.save();
		gw.last_message = this_meta.server_time;
		gw.number_of_messages++;
		if (this_meta.altitude != gw.altitude ||
		    this_meta.latitude != gw.latitude ||
		    this_meta.longitude != gw.longitude ) {
		    	log.info("GW eui: "+eui+" changed position");
			gw.altitude = this_meta.altitude;
			gw.latitude = this_meta.latitude;
			gw.longitude = this_meta.longitude;
		}
		gw.save();
	    } else {
		log.info("No GW with eui: "+eui);
		var new_gw = new LoraGateway({
				    gateway_eui: eui,
				    last_message: this_meta.server_time,
				    number_of_messages: 1,
				    altitude: this_meta.altitude,
				    latitude: this_meta.latitude,
				    longitude: this_meta.longitude
				});
		new_gw.save().bind(this).then( function(gw) {
		    log.info("Created new GW eui: "+eui);
		    this_meta.gateway = gw;
		    this.save();
		});
	    }
	});
    };
    on_message(msg) {
//	log.info(''+JSON.stringify(msg));
//	log.info(""+JSON.stringify(msg['metadata']));
	var new_msg = new LoraMessage(msg);
	new_msg.save().bind(this).then( function(doc) {
	    doc.metadata.forEach( (meta) => {
		this.fill_gateway.bind(doc)(meta.gateway_eui);
	    });
	    var new_log = new LoraMessageLog({message: doc});
	    new_log.save();
	});
    };
    constructor(mqtt, host, db) {
	this.mqtt = mqtt;
	log.info("mongodb connection", config.db);
	this.dbcon = mongoose.createConnection(config.db);
	LoraMessage = this.dbcon.model('LoraMessage', LoraSchema.Message);
	LoraMessageLog = this.dbcon.model('LoraMessageLog', LoraSchema.MessageLog);
	LoraGateway = this.dbcon.model("LoraGateway", LoraSchema.Gateway);

	this.dbcon.on('error', log.error.bind(log, 'connection error:'));
        this.dbcon.on('open', () => {
	    log.info('mongodb connected');
	    this.mqtt.on('message', (message) => {
		this.on_message(message);
	    });
	});
	this.dbcon.on('close', () => {
	    log.error("mongoddb disconnected");
	    this.mqtt.removeListener('message', this.on_message);
	});
    };
};

class TTNClient extends EventEmitter {
    constructor() {
	super();
	var mqtt = require('mqtt');
	log.info("TTN connectig to: "+config.ttn.host+" username: "+config.ttn.username);
	this.mqttclient = mqtt.connect(
		config.ttn.host,
	    	{   username: config.ttn.username,
		    password: config.ttn.password
		});

	this.mqttclient.on('connect', () => {
	    log.info("TTN mqtt connected");
	    this.mqttclient.subscribe('#');
	});

	this.mqttclient.on('message', (topic, message) => {
	    log.debug('mqtt message topic %s', topic)
	    log.debug('mqtt message body %s', message)
	    try {
		var msgJson = JSON.parse(message);
		this.emit('message', msgJson);
	    } catch(e) {
		log.error(e.message);
	    }
	});
    }
}

var ttnclient = new TTNClient();
var db = new LoraDB(ttnclient, 'localhost', 'lora-test');
