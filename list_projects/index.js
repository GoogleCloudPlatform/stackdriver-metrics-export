
/**
 * Copyright 2019, Google, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Based on https://github.com/GoogleCloudPlatform/gcf-gce-usage-monitoring/

'use strict';
const config = require('./config.json');
 
//  Use the Pub/Sub nodejs client library
//  https://googleapis.dev/nodejs/pubsub/latest/index.html#reference
const {PubSub} = require('@google-cloud/pubsub');
const pubsub = new PubSub();

// Use the Resource Manae nodejs client library
// https://googleapis.dev/nodejs/resource/latest/index.html#reference
const {Resource} = require('@google-cloud/resource');
const resource = new Resource();

// [START functions_getProjects]
/**
 * Calls the Cloud Resource Manager API to get a list of projects. Writes a
 * Pub/Sub message for each project
 */
async function getProjects() {

    try {
        // Lists all current projects
        const [projects] = await resource.getProjects();
        
        console.log(`Got past getProjects() call`);

        // Set a uniform endTime for all the resulting messages
        const endTime = new Date();
        const endTimeStr = endTime.toISOString();

        // sample 2019-11-12T17:58:26.068483Z

        for (var i=0; i<projects.length;i++) {
            
            // Only publish messages for active projects
            if (projects[i]["metadata"]["lifecycleState"] === config.ACTIVE)  {   
                // Construct a Pub/Sub message 
                console.log(`About to send Pub/Sub message ${projects[i]}}`);
                const pubsubMessage = {
                        "token": config.METRIC_EXPORT_PUBSUB_VERIFICATION_TOKEN,
                        "project_id": projects[i]["id"],
                        "end_time": endTimeStr
                }

                // Send the Pub/Sub message 
                const dataBuffer = Buffer.from(JSON.stringify(pubsubMessage));
                const messageId = await pubsub.topic(config.PROJECTS_TOPIC).publish(dataBuffer);
                console.log(`Published pubsub messageId: ${messageId} for project: ${projects[i]["id"]}, message ${JSON.stringify(pubsubMessage)}.`);
            } 

        }
    } catch(err) {
        console.error("Error in getProjects()");
        console.error(err);
        throw err;
    }
}
// [END functions_publishResult]


// [START functions_list_projects]
/**
 * Background Cloud Function to be triggered by a Cloud Pub/Sub message.
 *
 * @param {object} pubSubEvent The Cloud Functions event which contains a pubsub message
 * @param {object} context The Cloud Functions context 
 */
exports.list_projects = (pubSubEvent, context) => {
	console.log(`messageId: ${context.eventId}`);
    const data = Buffer.from(pubSubEvent.data, 'base64').toString();
    var jsonMessage = "";
    try {
        jsonMessage = JSON.parse(data);
    } catch(err) {
      	console.error(`Error parsing input message: ${data}`);
        console.error(err);
        throw err;
    }
    if ("token" in jsonMessage) {
        const token = jsonMessage["token"];
        if (token === config.TOKEN){
            return getProjects();
        } else {
             const err = new Error("The token property in the pubsub message does not match, not processing");
             console.error(err);
             throw err;
        } 
    } else {
        const err = new Error("No token property in the pubsub message, not processing");
        console.error(err);
        throw err;
    }	    
};
// [END functions_list_projects]
