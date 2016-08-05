DB      = require 'database'
db      = new DB('0', 'lucky28')
request = require 'request'
$       = require 'cheerio'
_       = require 'underscore'
fs      = require 'fs'
async   = require 'async'

url     = "http://www.bwlc.gov.cn/bulletin/keno.html"

_sqlLine = (arr) ->
  datetime = "#{arr[4]} #{arr[5]}"
  "(#{arr[1]}, #{arr[2]}, #{arr[3]}, '#{datetime}')"

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