// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
var attr = require('dynamodb-data-types').AttributeValue
const { v4: uuid } = require('uuid');
let fs = require('fs')
const path = require('path')
const localpath = '/tmp/';
var s3 = new AWS.S3()
let filename = uuid() + '.txt'
console.log(localpath+filename)
let filename1 = filename
// Set the region 
AWS.config.update({ region: 'us-east-1' });

// Create the DynamoDB service object
var ddb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

async function scanForResults() {
    try {
        let connectionData = await ddb.scan({
          TableName: 'lms-messenger-dev-ChatConnectionsTable-1IQ5U0GHCLF68',
          ProjectionExpression: "connectionId"
        }).promise();
        console.log(connectionData.Count)
      } catch (err) {
        console.log(err);
        return { statusCode: 500 };
      }
    try {
        var params = {
            TableName: 'conversation',
            ProjectionExpression: "author,#data",
            ExpressionAttributeNames: {
                "#data": "data"
            }
        };

        var result = await ddb.scan(params).promise()
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
        fs.readFile(localpath + filename, function (err, data) {
            if (err) { throw err }

            // Buffer Pattern; how to handle buffers; straw, intake/outtake analogy
            var base64data = new Buffer(data, 'binary');

            s3.putObject({
                'Bucket': 'lmsmessengerconversations',
                'Key': filename,
                'Body': base64data,
                'ACL': 'public-read'
            }, function (resp) {
                console.log(arguments);
                console.log('Successfully uploaded, ', filename)
            })
        })
    } catch (error) {
        console.error(error);
    }

}
scanForResults()
