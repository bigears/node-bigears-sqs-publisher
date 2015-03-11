var Promise      = require('bluebird')
  , regionBucket = require('region-bucket')
  , uuid         = require('node-uuid')
  , debug        = require('debug')('bigears-sqs-queue')
  , sqsQueue     = require('sqs-queue');

var BigearsSqsQueue = function(region, name)
{
  this.region = region;
  this.name = name;

  // Create the destination queue
  this.queue = sqsQueue(region, name);

  // Create the destination bucket
  this.bucket = regionBucket(
    region,
    name,
    { 
      ACL: 'private',
      create: true
    }
  );

  this.bucket.catch(function(err) {
    throw err;
  });
};

/*
* Add the whole message to the queue
*/
function publishInline (bsq, id, payload)
{
  debug('queue inline');

  var body = inlinePayload(bsq.name, bsq.region, id, payload);

  return publishToQueue(
    bsq,
    'inline',
    id,
    body
  );
}

function publishToQueue(bsq, location, id, body)
{
  return bsq.queue.then(function(sqs)
  {
    return sqs.sendMessageAsync({
      MessageBody: JSON.stringify(body),
      MessageAttributes: {
        UUID: {
          DataType: 'String',
          StringValue: id
        },
        Location: {
          DataType: 'String',
          StringValue: location
        },
        Bucket: {
          DataType: 'String',
          StringValue: bsq.name
        },
        Region: {
          DataType: 'String',
          StringValue: bsq.region
        }
      }
    });
  });
}

/*
* Add the payload to S3 and then add the message to the queue
*/
function publishS3 (bsq, id, payload)
{
  debug('queue s3');

  var key = id + '/payload.json';
  var body = metaPayload(bsq.name, bsq.region);
  body['Location'] = 's3';
  body['Key'] = key;

  var upload = bsq.bucket.then(function(bucket)
  {
    return bsq.bucket.putObjectAsync({
      Body: JSON.stringify(payload),
      Key: key,
      ContentType: 'application/json'
    });
  });

  return upload.then(function() {
    return publishToQueue(
      bsq,
      's3',
      id,
      body
    );
  });
}

function metaPayload(name, region, id)
{
  return {
    id: id,
    Bucket: name,
    Region: region,
    Location: 'S3'
  };
}

function inlinePayload(name, region, id, payload)
{
  var meta = metaPayload(name, region, id);
  meta['Body'] = payload;
  meta['Location'] = 'inline'
  return meta;
}

function generateId()
{
  return uuid.v4();
}

BigearsSqsQueue.prototype.publish = function(payload)
{
  var id = generateId();
  var length = JSON.stringify(inlinePayload(this.name, this.region, id, payload))).length
  var method = length >= 65536 ? publishS3 : publishInline;

  return method.call(this, id, payload).then(function() {
    return id;
  });
};

module.exports = BigearsSqsQueue;

