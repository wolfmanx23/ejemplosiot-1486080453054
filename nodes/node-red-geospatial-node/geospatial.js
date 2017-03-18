/**
 * Copyright 2014 IBM Corp. and Chris Howard @kitard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
    "use strict";
	var connectionPool 	= require("./lib/mqttConnectionPool");
	var http 			= require("follow-redirects").http;
    var https 			= require("follow-redirects").https;
	var url 			= require("url");
	
	var vcap 			= JSON.parse(process.env.VCAP_SERVICES || "{}");
    var services 		= vcap["Geospatial Analytics"]||[];
    var serviceList 	= services.map(function(s) { return { name: s.name, label: s.label }; });
		
    // HTTP endpoints that will be accessed from the HTML file
    RED.httpAdmin.get('/geospatial/vcap', function(req,res) {
        res.send(JSON.stringify(serviceList));
    });
		
    function GeospatialNode(n) {
        RED.nodes.createNode(this,n);
		
		var node = this;
		node.notifytopic = n.notifytopic;
		node.inputtopic = n.inputtopic;
		node.qos = n.qos || null;
        node.retain = n.retain;
		node.broker = n.broker;
		node.deviceid = n.deviceid;
        node.latitude = n.latitude;
        node.longitude = n.longitude;
		node.refresh = n.refresh;
		node.brokerConfig = RED.nodes.getNode(node.broker);
		node.interval_id = null;
		node.timer = null;
	
		// Retrieve the VCAP_SERVICES detail
		if (services.length == 0) {
 			node.error("No GeoSpatial Analytics service bound to application");
			// Service is expected to run in  on Bluemix and so no GS services is a show stopper
        } else {
			// Get the credentials for the first bound Geospatial Analytics Service
            var cred = services[0].credentials;
			// Server details
			var gs_host = cred.geo_host;
			var gs_port = cred.geo_port;
			// User credential
			var gs_username = cred.userid;
            var gs_password = cred.password;
			var authbuf = 'Basic ' + new Buffer(gs_username + ':' + gs_password).toString('base64');	            
			// Control paths			
			var start_path = cred.start_path;
			var stop_path = cred.stop_path;
			var restart_path = cred.restart_path;
			var status_path = cred.status_path;
			var remove_path = cred.remove_region_path;
			var add_path = cred.add_region_path;
			
			// Check refresh flag and setup interval timer to updated status avery 60 seconds
			/*
			if (node.refresh) {
				this.log("Status refresh started");
				
				//this.interval_id = setInterval( function(){ node.emit("input",{topic:"ping"}); }, 60000);
				node.timer = setTimeout(function(){ node.emit("input",{topic:"ping"}); }, 60000);
				
			}
			*/
			// Get the initial service status at least once (we may not be refreshing every 60 seconds)
			setTimeout(function() { node.emit("input",{topic:"ping"}) }, 5000);
		}
		
        if (this.brokerConfig) {
        	this.status({fill:"red",shape:"ring",text:"Checking..."});
            this.client = connectionPool.get(this.brokerConfig.broker,this.brokerConfig.port,this.brokerConfig.clientid,this.brokerConfig.username,this.brokerConfig.password);
            
			// PROCESS INCOMING GEOFENCE EVENTS
			// Subscribe to the notify topic to receive geofence events from the Geospatial Service (entry / exit)
			this.client.subscribe(this.notifytopic,2,function(topic,payload,qos,retain) {
				var msg = {topic:topic,payload:payload,qos:qos,retain:retain};
                if ((node.brokerConfig.broker === "localhost")||(node.brokerConfig.broker === "127.0.0.1")) {
                    msg._topic = node.notifytopic;
                }
				node.log("Processing new geofence / region event");
				msg.topic = node.notifytopic;
				msg.payload = JSON.parse(msg.payload);
                node.send(msg);
            });
			
			// Either publish to the input topic to send device updates to the Geospatial Service or respond to status requests
			this.on("input",function(msg) {
				var msgTopic = msg.topic;
				
				// If no Geospatial Analytics service then we just have to ignore the requests
				if (services.length != 0) {
				
					switch (msgTopic) {
						case "device":
							// PROCESS INCOMING DEVICE UPDATES - need to publish to MQTT for processing by the Geospatial service
							node.log("Processing device update from upstream node");
							if (msg.qos) {
								msg.qos = parseInt(msg.qos);
								if ((msg.qos !== 0) && (msg.qos !== 1) && (msg.qos !== 2)) {
									msg.qos = null;
								}
							}
					
							msg.qos = Number(node.qos || msg.qos || 0);
							msg.retain = node.retain || msg.retain || false;
							msg.retain = ((msg.retain === true) || (msg.retain === "true")) || false;
							msg.topic = this.inputtopic;
							this.client.publish(msg);  // send the message
							break;
						
						case "add":
							node.log("Processing add region request");
							// PROCESS NEW REGION REQUEST - need to call REST Api for the Geospatial service
							// Parse payload to add new region via the VCAP service URL
							
							var jsonObject = msg.payload;
							
							/*
							{ "regions" : [ { "region_type" : "custom", "name" : "Circular Quay - Wharf 3", "notifyOnExit" : "true", "polygon" : [ {"latitude" : "-33.86128372966939", "longitude" : "151.2117084850667"}, {"latitude" : "-33.85987928244262", "longitude" : "151.211919823112"}, {"latitude" : "-33.85979952669746", "longitude" : "151.2112941501821"}, {"latitude" : "-33.86123241743366", "longitude" : "151.2111575446197"} ] }, { "region_type" : "custom", "name" : "Circular Quay", "notifyOnExit" : "false", "polygon" : [ {"latitude" : "-33.85987935698185", "longitude" : "151.2119179608288"}, {"latitude" : "-33.85612872741613", "longitude" : "151.2144755478575"}, {"latitude" : "-33.85553024178637", "longitude" : "151.2107668322533"}, {"latitude" : "-33.85979918338862", "longitude" : "151.2112823811906"} ] }, { "region_type" : "custom", "name" : "Manly Wharf", "notifyOnExit" : "true", "polygon" : [ {"longitude" : "151.2834204345545", "latitude" : "-33.79939531884709"}, {"longitude" : "151.2829123988959", "latitude" : "-33.80070479647617"}, {"longitude" : "151.2841503476715", "latitude" : "-33.80117700731136"}, {"longitude" : "151.2847030493143", "latitude" : "-33.79998545710612"} ] }, { "region_type" : "custom", "name" : "Manly Cove", "notifyOnExit" : "false", "polygon" : [ {"longitude" : "151.2829123988959", "latitude" : "-33.80070479647617"}, {"longitude" : "151.2757792155393","latitude" : "-33.80804339682668"}, {"longitude" : "151.282173159308","latitude" : "-33.80863059049899"}, {"longitude" : "151.2841545227467","latitude" : "-33.80117416348082"} ] }, { "region_type" : "custom", "name" : "Port Jackson East", "notifyOnExit" : "false", "polygon" : [ {"longitude" : "151.2482400249193","latitude" : "-33.86053854770842"}, {"longitude" : "151.2661259042877","latitude" : "-33.84902581903113"}, {"longitude" : "151.2787662663046","latitude" : "-33.83357632379445"}, {"longitude" : "151.2691548194773","latitude" : "-33.82761625016626"}, {"longitude" : "151.2464483405304","latitude" : "-33.85387990006348"} ] }, { "region_type" : "custom", "name" : "Port Jackson West", "notifyOnExit" : "false", "polygon" : [ {"longitude" : "151.2144857211807","latitude" : "-33.85612519532695"}, {"longitude" : "151.2152869137699","latitude" : "-33.85601514714193"}, {"longitude" : "151.2298442398505","latitude" : "-33.85785734812639"}, {"longitude" : "151.2482513233579","latitude" : "-33.86053134911698"}, {"longitude" : "151.246432854532","latitude" : "-33.85387032496649"}, {"longitude" : "151.2310753735094","latitude" : "-33.85108139034109"}, {"longitude" : "151.2187417166343","latitude" : "-33.85289113905708"}, {"longitude" : "151.2128793548532","latitude" : "-33.85110591923923"}, {"longitude" : "151.210787591753","latitude" : "-33.85553538761177"} ] }, { "region_type" : "custom", "name" : "North Harbour", "notifyOnExit" : "false", "polygon" : [ {"longitude" : "151.2787848684667","latitude" : "-33.83360489915113"}, {"longitude" : "151.2821713004305","latitude" : "-33.80860698809906"}, {"longitude" : "151.2757781444837","latitude" : "-33.80804136262416"}, {"longitude" : "151.2691402112606","latitude" : "-33.82764324849117"} ] } ] }
							*/
							
							// prepare the header
							putheaders = {
								'Content-Type' : 'application/json',
								'Content-Length' : Buffer.byteLength(jsonObject, 'utf8'),
								'Authorization' : authbuf
							};
							 
							// the put options
							optionsput = {
								host : gs_host,
								port : gs_port,
								path : add_path,
								method : 'PUT',
								headers : putheaders
							};
				
							console.log("Add request : " + JSON.stringify(optionsput));
				
							// do the PUT call
							var req = http.request(optionsput, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = {"statusCode" : res.statusCode};
								//var payload = "";

								res.on('data',function(chunk) {
									//payload += chunk;
								});

								res.on('end',function() {
									msg.payload = payload;
									node.send(msg);
									node.emit("input",{topic:"ping"});
								});
							});

							console.log("Region detail : " + jsonObject);							
							
							req.write(jsonObject); // write the region data
							req.end();
							
							// Handle any errors in the request
							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
								node.status({fill:"red",shape:"ring",text:err.code});
							});
							

							break;
						
						case "remove":
							// PROCESS DELETE REGION REQUEST - need to call REST Api for the Geospatial service
							node.log("Processing remove region request");
							
							var jsonObject = JSON.stringify(msg.payload);
							
							// prepare the header
							putheaders = {
								'Content-Type' : 'application/json',
								'Content-Length' : Buffer.byteLength(jsonObject, 'utf8'),
								'Authorization' : authbuf
							};
							 
							// the put options
							optionsput = {
								host : gs_host,
								port : gs_port,
								path : remove_path,
								method : 'PUT',
								headers : putheaders
							};
							 
							// do the PUT call
							var req = http.request(optionsput, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = "";

								res.on('data',function(chunk) {
									payload += chunk;
								});

								res.on('end',function() {
									node.send(msg);
								});
							});

							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
							});

							req.write(jsonObject); // write the region data
							req.end();
							// Update service status
							node.emit("input",{topic:"ping"});

							break;
							
						case "status":
							// PROCESS STATUS REQUEST 
							node.log("Processing status request");

							// prepare the header
							var getheaders = {
								'Authorization' : authbuf
							};
							 
							// the get options
							var optionsget = {
								host : gs_host,
								port : gs_port,
								path : status_path,
								method : 'GET',
								headers : getheaders
							};
							 
							// do the GET call
							var req = http.request(optionsget, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = "";

								res.on('data',function(chunk) {
									payload += chunk;
								});

								res.on('end',function() {
									msg.payload = payload; //JSON.parse(payload);
									node.send(msg);
								});
							});

							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
							});

							req.end();
							break;
							
						case "ping":
							// PROCESS 60 SECOND PING - request the status of the Geospatial service
							node.log("Processing status ping");

							// prepare the header
							var getheaders = {
								'Authorization' : authbuf
							};
							 
							// the get options
							var optionsget = {
								host : gs_host,
								port : gs_port,
								path : status_path,
								method : 'GET',
								headers : getheaders
							};
							 
							// do the GET call
							var req = http.request(optionsget, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = "";

								res.on('data',function(chunk) {
									payload += chunk;
								});

								res.on('end',function() {
									msg.payload = JSON.parse(payload);
									var numRegions = msg.payload.custom_regions.length + msg.payload.regular_regions.length;
									
									switch (msg.payload.status_code) {
										case 0:
											node.status({fill:"red",shape:"dot",text:"Not Started"});
										break;
										
										case 1:
											node.status({fill:"red",shape:"ring",text:"Starting"});
										break;
									
										case 2:
											node.status({fill:"green",shape:"dot",text:"Running : " + numRegions + " regions"});
										break;
									
										case 3:
											node.status({fill:"green",shape:"ring",text:"Stopped : " + numRegions + " regions"});
										break;
									
										case 10,11:
											node.status({fill:"red",shape:"dot",text:"Failed to start"});
										break;
									}
								});
							});

							req.end();
							
							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
							});
							
							if (node.refresh) {
								node.timer = setTimeout(function(){ node.emit("input",{topic:"ping"}); }, 60000);
							}
							
							break;
						
						case "start":
							// PROCESS A GS SERVICE START REQUEST - setup MQ broker and data parameters
							node.log("Processing start service request");

							jsonObject = JSON.stringify({
							  "mqtt_uid" : this.brokerConfig.username,
							  "mqtt_pw" : this.brokerConfig.password,
							  "mqtt_uri" :  this.brokerConfig.broker+":"+this.brokerConfig.port,
							  "mqtt_input_topics" : this.inputtopic,
							  "mqtt_notify_topic" : this.notifytopic,
							  "device_id_attr_name" : this.deviceid,
							  "latitude_attr_name" : this.latitude,
							  "longitude_attr_name" : this.longitude
							});
							
							node.log(JSON.stringify(jsonObject))
							
							// prepare the header
							var putheaders = {
								'Content-Type' : 'application/json',
								'Content-Length' : Buffer.byteLength(jsonObject, 'utf8'),
								'Authorization' : authbuf
							};
							 
							// the put options
							var optionsput = {
								host : gs_host,
								port : gs_port,
								path : start_path,
								method : 'PUT',
								headers : putheaders
							};
							
							// do the PUT call
							var req = http.request(optionsput, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = {"statusCode" : res.statusCode};;

								res.on('data',function(chunk) {
								//	payload += chunk;
								});

								res.on('end',function() {
									msg.payload = payload;
									node.send(msg);
									node.emit("input",{topic:"ping"});
								});
							});

							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
								//node.status({fill:"red",shape:"ring",text:err.code});
							});

							req.write(jsonObject); // write the region data
							req.end();
							this.status({fill:"red",shape:"ring",text:"Checking..."});
							
							
							break;
							
							
						case "stop":
							// PROCESS A SERVICE STOP REQUEST
							node.log("Processing stop service request");

							// prepare the header
							putheaders = {
								'Authorization' : authbuf
							};
							 
							// the put options
							optionsput = {
								host : gs_host,
								port : gs_port,
								path : stop_path,
								method : 'PUT',
								headers : putheaders
							};
							 
							// do the PUT call
							var req = http.request(optionsput, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = {"statusCode" : res.statusCode};

								res.on('data',function(chunk) {});

								res.on('end',function() {
									msg.payload = payload;
									node.send(msg);
									node.emit("input",{topic:"ping"});
								});
							});

							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
							});

							req.end();
							this.status({fill:"red",shape:"ring",text:"Checking..."});
							
							break;	
						
						case "restart":
							// PROCESS A SERVICE RESTART REQUEST
							node.log("Processing restart service request");
							
							// prepare the header
							putheaders = {
								'Authorization' : authbuf
							};
							 
							// the put options
							optionsput = {
								host : gs_host,
								port : gs_port,
								path : restart_path,
								method : 'PUT',
								headers : putheaders
							};
							 
							// do the PUT call
							var req = http.request(optionsput, function(res) {
								res.setEncoding('utf8');
								msg.statusCode = res.statusCode;
								msg.headers = res.headers;
								var payload = {"statusCode" : res.statusCode};

								res.on('data',function(chunk) {
								});

								res.on('end',function() {
									msg.payload = payload;
									node.send(msg);
									setTimeout(function() { node.emit("input",{topic:"ping"}) }, 30000);
								});
							});

							req.on('error',function(err) {
								msg.payload = err.toString() + " : " + url;
								msg.statusCode = err.code;
								node.error(JSON.stringify(msg)); 
								node.send(msg);
							});

							req.end();
							this.status({fill:"red",shape:"ring",text:"Checking..."});
							// Restarts seems to take a while (regions managed also intially incorrect). Hold off 5s
							
							break;	

						default:
					
					}
				}
				
            });
			
            this.client.on("connectionlost",function() {
				this.status({fill:"red",shape:"ring",text:"MQTT Disconnected..."});
				
            });
            
			this.client.on("connect",function() {
                // All good
            });
            
			this.client.connect();
			
        } else {
            This.error("Missing MQTT broker configuration");
		}
		
        this.on('close', function() {
            if (this.client) {
                this.client.disconnect();
            }
        });
    }
    RED.nodes.registerType("geospatial",GeospatialNode);

	// Clean up interval timer	
	GeospatialNode.prototype.close = function() {
        if (this.interval_id != null) {
            clearInterval(this.interval_id);
            this.log("Regular status refresh stopped");
        }
    }
}
