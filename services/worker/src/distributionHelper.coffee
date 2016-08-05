DB       = require 'database'
reportDB = require('../../configs').reportDB
db       = new DB '1', reportDB
async    = require 'async'
_        = require 'underscore'
mysql    = require 'mysql'

_getColumns = (object) ->
  _.keys object

_getValues = (object) ->
  vals = _.values object
  _.map(vals, (item) -> mysql.escape(item))

distribute = (list = [], distribution, callback) ->
  if not distribution.delDim || not distribution.delArg || not distribution.table
    return callback new Error 'Distribution args error'

  # 结果分组
  # groupByKey = 'app_id'
  # groups = _.groupBy list, (item) -> item.app_id
  deleteSQL = "DELETE FROM #{distribution.table} WHERE #{distribution.delDim} = '#{distribution.delArg}'"
  execSQLFun = (useless, cb) ->
    async.waterfall [
      (cb1) ->
        db.query deleteSQL, (err) ->
          cb1 err
      (cb1) ->
        data = list[0]
        if data
          cols = _getColumns data

          valList = _.map list, (item) ->
            vArr = _getValues item
            "(#{vArr.join ','})"
          vals = valList.join ','

          insertSQL = "INSERT INTO #{distribution.table}(#{cols.join ','}) VALUES #{vals}"
          # console.log insertSQL
          db.query insertSQL, (err) ->
            cb1 err
        else
          cb1()
    ], (err) ->
      cb err

  async.eachLimit ['useless'], 1, execSQLFun, (err) ->
    callback err

# distribution =
#   delDim: 'date'
#   delArg: '2015-06-13'
#   table:  'report_click_to_active_daily'
# testData = [{"app_id":50784,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":279},{"app_id":50787,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":2533},{"app_id":51467,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":867},{"app_id":51532,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":1694},{"app_id":51880,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":64107},{"app_id":55406,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":1728},{"app_id":56463,"media":"3101","t60":0,"t3600":0,"t86400":0,"t86400more":139},{"app_id":57060,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":20},{"app_id":59136,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":31658},{"app_id":59136,"media":"3188","t60":4,"t3600":76,"t86400":66,"t86400more":0},{"app_id":59138,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":5982},{"app_id":59138,"media":"3188","t60":17,"t3600":101,"t86400":67,"t86400more":0},{"app_id":60446,"media":"","t60":0,"t3600":0,"t86400":0,"t86400more":2242},{"app_id":60469,"media":"2310","t60":0,"t3600":0,"t86400":0,"t86400more":20},{"app_id":60469,"media":"3027","t60":0,"t3600":0,"t86400":0,"t86400more":577},{"app_id":60469,"media":"3028","t60":0,"t3600":0,"t86400":0,"t86400more":281}]
# groups = _.groupBy testData, (item) -> item.app_id
# data = groups[59138]
# if data
#   data = _.map data, (d) ->
#     _.omit d, 'app_id'
#   head = data[0]
#   cols = _.keys head
#   valList = []
#   _.each data, (d) ->
#     valList.push "('#{_.values(d).join '\',\''}')"
#   vals = valList.join ','
#   insertSQL = "INSERT INTO #{distribution.table} (#{cols.join ','}) VALUES #{vals}"
#   console.log insertSQL


module.exports = exports = { distribute }
