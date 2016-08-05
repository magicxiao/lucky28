DB      = require 'database'
db      = new DB('0', 'lucky28')
request = require 'request'
$       = require 'cheerio'
_       = require 'underscore'
fs      = require 'fs'
async   = require 'async'

url     = "http://www.bwlc.gov.cn/bulletin/keno.html"

_sqlLine = (arr) ->
  nums = arr[2].split(',')
  nums = _.map nums, (n) ->
    parseInt n
  nums = _.sortBy nums, (n) ->
    n
  arr1 = nums.slice 0, 6
  arr2 = nums.slice 6, 12
  arr3 = nums.slice 12, 18
  num1 = 0
  num2 = 0
  num3 = 0
  _.each arr1, (n) ->
    n = parseInt n
    num1+= n
  _.each arr2, (n) ->
    n = parseInt n
    num2+= n
  _.each arr3, (n) ->
    n = parseInt n
    num3+= n
  luckynum = num1 % 10 + num2 % 10 + num3 % 10

  datetime = "#{arr[4]} #{arr[5]}"
  "(#{arr[1]}, #{luckynum}, #{arr[2]}, #{arr[3]}, '#{datetime}')"

handle = (config, callback) ->
  async.waterfall [
    (cb) ->
      request url, (err, response, body) ->
        return cb err if err
        if response.statusCode is 200
          # fs.writeFileSync './bj28.html', body
          dom = $.load body, {normalizeWhitespace: true}
          trArr = dom('.lott_cont .tb').children()
          records = []
          for tr in trArr
            text = $(tr).text()
            arr = text.split(' ')
            # console.log arr
            if arr.length > 6
              records.push _sqlLine arr
          cb null, records
        else
          cb Error "statusCode error: #{statusCode}"
    , 
    (records, cb) ->
      console.log records
      SQL = "INSERT IGNORE INTO beijing28 VALUES #{records.join ','}"
      db.query SQL, (err) ->
        cb()
  ], (err) ->
    callback err

module.exports = handle