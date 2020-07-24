"use strict";

const AWS = require("aws-sdk");
const Bluebird = require("bluebird");
const crypto = require('crypto');
var attr = require('dynamodb-data-types').AttributeValue
const { v4: uuid } = require('uuid');
let fs = require('fs')
const path = require('path')
const localpath = '/tmp/';
var s3 = new AWS.S3()
let filename = uuid() + '.txt'
console.log(localpath + filename)
AWS.config.update({ region: process.env.AWS_REGION });
const DDB = new AWS.DynamoDB({ apiVersion: "2012-10-08" });
const docClient = new AWS.DynamoDB.DocumentClient();
AWS.config.setPromisesDependency(Bluebird);
require("aws-sdk/clients/apigatewaymanagementapi");

const successfullResponse = {
  statusCode: 200,
  body: "Connected"
};

const jose = require("node-jose");
const fetch = require("node-fetch");
fetch.Promise = Bluebird;

module.exports.connectionManager = (event, context, callback) => {
  if (event.requestContext.eventType === "CONNECT") {
    addConnection(event.requestContext.connectionId)
      .then(() => {
        callback(null, successfullResponse);
      })
      .catch(err => {
        callback(null, JSON.stringify(err));
      });
      createconversationTable()
  } else if (event.requestContext.eventType === "DISCONNECT") {

    deleteConnection(event.requestContext.connectionId)
      .then(() => {
        callback(null, successfullResponse);
      })
      .catch(err => {
        callback(null, {
          statusCode: 500,
          body: "Failed to connect: " + JSON.stringify(err)
        });
      });
    callStoreConversationinS3()

    // let connectionData = await DDB.scan({
    //     TableName: process.env.CHATCONNECTION_TABLE,
    //     ProjectionExpression: "connectionId"
    //   }).promise();
    //   if(connectionData.Count===0)
    //   {
    //     storeConversationinS3()
    //   }
  }
};

module.exports.defaultMessage = (event, context, callback) => {
  callback(null);
};

module.exports.sendMessage = async (event, context, callback) => {
  let connectionData;
  try {
    connectionData = await DDB.scan({
      TableName: process.env.CHATCONNECTION_TABLE,
      ProjectionExpression: "connectionId"
    }).promise();
  } catch (err) {
    console.log(err);
    return { statusCode: 500 };
  }
  
  const data = JSON.parse(event.body).data
  const postjsonData = JSON.parse(data)
  const uid = crypto.randomBytes(16).toString("hex");
  //const {author,type,data:messageData}=JSON.parse(event.body).data
  console.log('postjsondata' + postjsonData)
  console.log('author:' + postjsonData.author)
  console.log('data' + postjsonData.data)
  console.log('text' + postjsonData.data.text)
  console.log('type' + postjsonData.type)
  const textdata = postjsonData.data.text.replace(/ *\([^)]*\) */g, "")
  const putParams = {
    TableName: 'conversation',
    Item: {
      id: { S: uid },
      type: { S: postjsonData.type },
      author: { S: postjsonData.author },
      data: { S: textdata },
    }
  };
  try {
    await DDB.putItem(putParams).promise();
  }
  catch (e) {
    console.log(e)
  }
  //   docClient.put(putParams, function(err, postjsonData) {
  //     if (err) {
  //         console.log(err);
  //     } else {
  //         console.log("PutItem succeeded:", postjsonData);
  //     }
  //  });
  const postCalls = connectionData.Items.map(async ({ connectionId }) => {
    try {
      return await send(event, connectionId.S);
    } catch (err) {
      if (err.statusCode === 410) {
        return await deleteConnection(connectionId.S);
      }
      console.log(JSON.stringify(err));
      throw err;
    }
  });

  try {
    await Promise.all(postCalls);
  } catch (err) {
    console.log(err);
    callback(null, JSON.stringify(err));
  }
  callback(null, successfullResponse);
};

const send = (event, connectionId) => {
  const postData = JSON.parse(event.body).data;
  //added by me

  // try {
  //  await DDB.putItem({ TableName: postData, postData: postData }).promise();
  // } catch (e) {
  //   return { statusCode: 500, body: e.stack };
  // }

  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: "2018-11-29",
    endpoint: event.requestContext.domainName + "/" + event.requestContext.stage
  });
  return apigwManagementApi
    .postToConnection({ ConnectionId: connectionId, Data: postData })
    .promise();
};

const addConnection = connectionId => {
  const putParams = {
    TableName: process.env.CHATCONNECTION_TABLE,
    Item: {
      connectionId: { S: connectionId }
    }
  };

  return DDB.putItem(putParams).promise();
};

const deleteConnection = connectionId => {
  const deleteParams = {
    TableName: process.env.CHATCONNECTION_TABLE,
    Key: {
      connectionId: { S: connectionId }
    }
  };

  return DDB.deleteItem(deleteParams).promise();
};

module.exports.authorizerFunc = async (event, context, callback) => {
  const keys_url =
    "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_nwiJ4YLFG/.well-known/jwks.json";
  const {
    queryStringParameters: { token },
    methodArn
  } = event;

  const app_client_id = '818mtv2418l3oaiu2kruoko7u';
  if (!token) return context.fail("Unauthorized");
  const sections = token.split(".");
  let authHeader = jose.util.base64url.decode(sections[0]);
  authHeader = JSON.parse(authHeader);
  const kid = authHeader.kid;
  const rawRes = await fetch(keys_url);
  const response = await rawRes.json();

  if (rawRes.ok) {
    const keys = response["keys"];
    let key_index = -1;
    keys.some((key, index) => {
      if (kid == key.kid) {
        key_index = index;
      }
    });
    const foundKey = keys.find(key => {
      return kid === key.kid;
    });

    if (!foundKey) {
      context.fail("Public key not found in jwks.json");
    }

    jose.JWK.asKey(foundKey).then(function (result) {
      // verify the signature
      jose.JWS.createVerify(result)
        .verify(token)
        .then(function (result) {
          // now we can use the claims
          const claims = JSON.parse(result.payload);
          // additionally we can verify the token expiration
          const current_ts = Math.floor(new Date() / 1000);
          if (current_ts > claims.exp) {
            context.fail("Token is expired");
          }
          // and the Audience (use claims.client_id if verifying an access token)
          if (claims.aud != app_client_id) {
            context.fail("Token was not issued for this audience");
          }
          context.succeed(generateAllow("me", methodArn));
        })
        .catch(err => {
          context.fail("Signature verification failed");
        });
    });
  }
};

const generatePolicy = function (principalId, effect, resource) {
  var authResponse = {};
  authResponse.principalId = principalId;
  if (effect && resource) {
    var policyDocument = {};
    policyDocument.Version = "2012-10-17"; // default version
    policyDocument.Statement = [];
    var statementOne = {};
    statementOne.Action = "execute-api:Invoke"; // default action
    statementOne.Effect = effect;
    statementOne.Resource = resource;
    policyDocument.Statement[0] = statementOne;
    authResponse.policyDocument = policyDocument;
  }
  return authResponse;
};

const generateAllow = function (principalId, resource) {
  return generatePolicy(principalId, "Allow", resource);
};

const generateDeny = function (principalId, resource) {
  return generatePolicy(principalId, "Deny", resource);
};

async function storeConversationinS3() {
  try {
    var params = {
      TableName: 'conversation',
      ProjectionExpression: "author,#data",
      ExpressionAttributeNames: {
        "#data": "data"
      }
    };

    var result = await DDB.scan(params).promise()
    const jsonvalue = []
    console.log(result.Items)
    result.Items.map(item => {

      let s2 = attr.unwrap(item)
      jsonvalue.push({ author: s2.author, text: s2.data })
    })
    console.log(jsonvalue)
    console.log("insdie" + localpath + filename)
    await fs.writeFile(localpath + filename, JSON.stringify(jsonvalue), (err) => {
      console.log('response')
      if (err) {
        console.log(err)
      }

    })
    await fs.readFile(localpath + filename, function (err, data) {
      if (err) { throw err }

      // Buffer Pattern; how to handle buffers; straw, intake/outtake analogy
      var base64data = new Buffer(data, 'binary');

      s3.putObject({
        'Bucket': 'lmsmessengerconversations',
        'Key': filename,
        'Body': base64data,
        'ACL': 'public-read'
      }, function () {
        console.log(arguments);
        console.log('Successfully uploaded, ', filename)
      })
    })
    try {
      await DDB.deleteTable({
        TableName: "conversation"
      }).promise()
      
    }
    catch (e) {
      console.log(e)
    }
  
    
  } catch (error) {
    console.error(error);
  }
 
}

async function callStoreConversationinS3() {
  try {
    let connectionData = await DDB.scan({
      TableName: process.env.CHATCONNECTION_TABLE,
      ProjectionExpression: "connectionId"
    }).promise();
    if (connectionData.Count === 0) {
      storeConversationinS3()
    }
  }
  catch (e) {
    console.log(e)
  }

}

function createconversationTable()
{
  var params = {
    TableName: "conversation",
    KeySchema: [
      //Partition key
      { AttributeName: "id", KeyType: "HASH" },
      { AttributeName: "type", KeyType: "RANGE" }
      
        //Sort key
    ],
    AttributeDefinitions: [
      
      { AttributeName: "id", AttributeType: "S" },
      
      { AttributeName: "type", AttributeType: "S" }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    }
  };

  DDB.createTable(params, function (err, data) {
    if (err) {
      console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
    }
  });
}