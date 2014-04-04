/* 
 * thumbnail-cache.js
 * 
 * Cache image thumbnail.
 */

exports.cache = cache;

var util    = require('util'), 
    path    = require('path'),
    uuid    = require('node-uuid'), 
    aws     = require('aws-sdk'), 
    request = require('request');

var config  = require(path.join(__dirname, 'config.json'));

aws.config.loadFromPath(path.join(__dirname, '../../awsconfig.json'));
var s3       = new aws.S3(), 
    dynamodb = new aws.DynamoDB();

/*
 * Cache image thumbnail.
 * 
 * Parameters: 
 *   image   - (Object) S3 image object
 *     Bucket - (String) image bucket
 *     Key    - (String) image key
 *   options - (Object) thumbnail options
 *     width  - (Number) thumbnail width
 *     height - (Number) thumbnail height
 *     crop   - (String) crop method, 'Center' or 'North'
 *
 * Callback:
 *   callback - (Function) function(err, item) {} 
 *     err  - (Object) error object, set to null if succeed
 *     item - (Object) cached DynamoDB thumbnail item
 */
function cache(image, options, callback) {
  var item = {};
  item.TableName = config.TABLE;
  item.Key = {};
  item.Key.Index = {S: index(image, options)};
  dynamodb.getItem(item, function(err, cachedItem) {
    if (err) {
      callback(err, null);
      return;
    }

    // no cached thumbnail  
    if (!cachedItem.Item) {
      var thumb = {};
      thumb.Bucket = config.BUCKET;
      thumb.Key    = uuid.v1();
      create(image, thumb, options, function(err, addedItem) {
        if (err) {
          callback(err, null);
        } else {
          addedItem.Status = {S: 'added'};
          callback(null, addedItem);
        }
      });
      return;
    }
		
    // check update
    s3.headObject(image, function(err, imageData) {
      if (err) {
        callback(err, null);
        return;
      }

      if (imageData.ETag == cachedItem.Item.ImageETag.S) {
        cachedItem.Item.Status = {S: 'cached'};
        callback(null, cachedItem.Item);
      } else {
        var thumb = {};
        thumb.Bucket = cachedItem.Item.ThumbBucket.S;
        thumb.Key    = cachedItem.Item.ThumbKey.S;
        create(image, thumb, options, function(err, updatedItem) {
          if (err) {
            callback(err, null);
          } else {
            updatedItem.Status = {S: 'updated'};
            callback(null, updatedItem);
          }
        });
      }
    });  // s3.headObject
  });  // dynamodb.getItem
}

/*  
 * Create image thumbnail.
 * 
 * Parameters: 
 *  image - (Object) S3 image object
 *    Bucket - (String) image bucket
 *    Key    - (String) image key
 *  thumb - (Object) S3 thumbnail object
 *    Bucket - (String) thumbnail bucket
 *    Key    - (String) thumbnail key
 *  options - (Object) thumbnail options
 *    width  - (Number) thumbnail width
 *    height - (Number) thumbnail height
 *    crop   - (String) crop method, 'Center' or 'North'
 * 
 * Callback:
 *  callback - (Function) function(err, item) {}
 *    err  - (Object) error object, set to null if succeed.
 *    item - (Object) DynamoDB thumbnail item
 */
function create(image, thumb, options, callback) {
  var uri = config.S3THUMBNAILAPI + '?'    + 
            'imagebucket='  + image.Bucket + 
            '&imagekey='    + image.Key    + 
            '&thumbbucket=' + thumb.Bucket + 
            '&thumbkey='    + thumb.Key;
  if (options.width) 
    uri = uri + '&width='  + options.width;
  if (options.height) 
    uri = uri + '&height=' + options.height;
  if (options.crop) 
    uri = uri + '&crop='   + options.crop;
  util.log(uri);
  request(uri, function(err, res, body) { 
    if (err) {
      callback(err, null);
      return;
    }
    if (res.statusCode != 200) {
      var err = {};
      err.message = 's3 thumbnail API request error: ' + res.statusCode;
      callback(err, null);
      return;
    }

    var data = JSON.parse(body);
    util.log(JSON.stringify(data, null, 2));

    var item = {};
    item.TableName = config.TABLE;
    item.Item = {};
    item.Item.Index            = {S: index(image, options)};
    item.Item.ImageBucket      = {S: image.Bucket};
    item.Item.ImageKey         = {S: image.Key};
    item.Item.ImageETag        = {S: data.imageETag};
    item.Item.ThumbBucket      = {S: thumb.Bucket};
    item.Item.ThumbKey         = {S: thumb.Key};
    item.Item.ThumbContentType = {S: data.thumbType};
    if (options.width)
      item.Item.Width          = {N: options.width.toString()};
    if (options.height)
      item.Item.Height         = {N: options.height.toString()};
    if (options.crop)	
      item.Item.Crop           = {S: options.crop};
    dynamodb.putItem(item, function(err, data) {
      callback(err, err ? null : item.Item);
    });  // dynamodb.putItem
  });  // s3Thumb.create
}

function index(image, options) {
  var index = 'b=' + image.Bucket + 'k=' + image.Key;
  if (options.width)
    index += 'w=' + options.width;
  if (options.height)
    index += 'h=' + options.height;
  if (options.crop)
    index += 'c=' + options.crop;
  return index;
}

