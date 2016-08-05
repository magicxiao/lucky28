_     = require 'underscore'
async = require 'async'

dimensionKey = (dimensions = [], object = {}) ->
  key = ''
  for dim in dimensions
    if not _.isNull(object[dim]) && not _.isUndefined(object[dim])
      key += object[dim].toString() 
  key

aggregate = (metrics = [], object1 = {}, object2 = {}) ->
  for metric in metrics
    object1[metric] = object1[metric] + object2[metric]

# list = [
#   [ {a:'a1', b: 'b1', c: 1, d: 1} ],
#   [ {a:'a2', b: 'b2', c: 2, d: 3} ],
#   [ {a:'a1', b: 'b1', c: 1, d: 1} ]
# ]
# 作用：将上述形式list合并维度，聚合指标（指标相加）
aggregation = (list = [], dimensions, metrics, callback) ->
  sumResults  = []
  keyIndexMapper = {}

  sumFunction = (innerList, callback) ->
    for innerObject in innerList
      key = dimensionKey(dimensions, innerObject)

      foundItem = keyIndexMapper[key]

      # foundItem = _.find sumResults, (item) -> 
      #   if item
      #     dimensionKey(dimensions, item) is key

      if foundItem
        aggregate metrics, foundItem, innerObject
      else
        keyIndexMapper[key] = innerObject
        sumResults.push innerObject

    callback()

  async.eachLimit list, 1, sumFunction, (err) ->
    callback err, sumResults 

module.exports = { aggregation }