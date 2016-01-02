var aws = require( "aws-sdk" );
var configFile = require("./config.json");

// get configuration defaults from config file.
var tableName = configFile.tableName;
var queueUrl = configFile.queueUrl;
var configLocation = configFile.configLocation;

var dbClient = new aws.DynamoDB.DocumentClient();
var sqsClient = new aws.SQS();

// get config values from dynamodb - if the config values are found, then override existing values
// this will occur on every execution of the lambda which will allow real time configuration changes.
var updateConfig = function updateConfigValues(invokedFunction, cb) {

	var version = invokedFunction.split(":").pop();

	var params = {
		TableName: configLocation,

		Key : {
			"service" : "lambda",
			"pointer" : process.env.AWS_LAMBDA_FUNCTION_NAME + ":" + version
		}
	};

	dbClient.get(params, function(err, data) {

		if(err) {
			console.log("ERR_DYNAMODB_GET", err, params);
		}
		else if(!data || !data.Item) {
			console.log("INFO_DYNAMODB_NOCONFIG", params);
		}
		else {
			queueUrl = data.Item.config.queueUrl;
			tableName = data.Item.config.tableName;
		}

		return cb(err);
	});

}

// save the email to dynamodb using conditional write to ignore addresses already in the db
var saveEmail = function saveEmail(email, cb) {

	var params = {
		TableName:tableName,
		Item:{
			"emailaddress": email,
			"signupon": Date.now()
		},
		ConditionExpression : "attribute_not_exists(emailaddress)"
	};

	dbClient.put(params, function(err, data) {
		cb(err, data);
	});
}

var deleteMessage = function deleteMessage(receiptHandle, cb) {

	var params = {
		QueueUrl: queueUrl,
		ReceiptHandle: receiptHandle
	};

	sqsClient.deleteMessage(params, function(err, data) {
		cb(err, data);
	});

}

exports.handler = function(event, context) {

	updateConfig(context.invokedFunctionArn, function(err) {

		if(err) {
			context.done(err);
			return;
		}

		console.log("INFO_LAMBDA_EVENT", event);
		console.log("INFO_LAMBDA_CONTEXT", context);

		sqsClient.receiveMessage({MaxNumberOfMessages: 5, QueueUrl: queueUrl}, function(err, data) {

			if(err) {
				console.log("ERR_SQS_RECEIVEMESSAGE", err);
				context.done(null);
			}
			else {

				if (data && data.Messages) {

					var msgCount = data.Messages.length;

					console.log("INFO_SQS_RESULT", msgCount + " messages received");

					for(var x=0; x < msgCount; x++) {

						var message = data.Messages[x];
						console.log("INFO_SQS_MESSAGE", message);
						var messageBody = JSON.parse(message.Body);

						saveEmail(messageBody.Message, function(err, data) {

							if (err && err.code && err.code ==="ConditionalCheckFailedException") {
								console.error("INFO_DYNAMODB_SAVE", messageBody.Message + " already subscribed");
								deleteMessage(message.ReceiptHandle, function(err) {
									if(!err) {
										console.error("INFO_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, "successful");
									} else {
										console.error("ERR_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, err);
									}
									context.done(err);
								});

							}
							else if (err) {
								console.error("ERR_DYNAMODB_SAVE", "receipt handle: " + message.ReceiptHandle, err);
								context.done(err);
							}
							else {
								console.log("INFO_DYNAMODB_SAVE", "email_saved", "receipt handle: " + message.ReceiptHandle, messageBody.Message);
								deleteMessage(message.ReceiptHandle, function(err) {
									if(!err) {
										console.error("INFO_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, "successful");
									} else {
										console.error("ERR_SQS_MESSAGE_DELETE", "receipt handle: " + message.ReceiptHandle, err);
									}
									context.done(err);
								});
							}


						});

					}
				} else {
					console.log("INFO_SQS_RESULT", "0 messages received");
					context.done(null);
				}
			}
		});
	});

}