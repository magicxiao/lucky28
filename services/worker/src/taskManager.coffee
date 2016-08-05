DB     = require 'database'
db     = new DB('0', 'system')
async  = require 'async'
moment = require 'moment'
_      = require 'underscore'

_addslashes = (str)->
  if _.isString str
    str = str.replace /\\/g, '\\\\'
    str = str.replace /\'/g, '\\\''
    str = str.replace /\"/g, '\\"'
    str = str.replace /\0/g, '\\0'
  str

# 获取一个task
getTask = (callback) ->
  async.waterfall [
    (cb) ->
      sql = "SELECT * FROM task WHERE status = 'ready' ORDER BY id LIMIT 1"
      db.query sql, (err, ret) ->
        cb err, ret
    ,
    (ret, cb) ->
      if ret.length is 1
        task = ret[0]
        sql = "UPDATE task SET status = 'doing', startTime = #{moment().unix()} WHERE id = #{task.id}"
        db.query sql, (err) ->
          cb err, task
      else
        cb()
  ], (err, task) ->
    callback err, task

updateTask = (id, opts, callback) ->
  updateClause = []
  updateClause.push "status = '#{opts.status}'" if opts.status
  updateClause.push "sqlClause = '#{_addslashes(opts.SQL)}'" if opts.SQL
  updateClause.push "result = '#{opts.result}'" if opts.result
  updateClause.push "runTime = #{moment().unix()} - startTime" if opts.runTime
  if updateClause.length > 0
    sql = "UPDATE task SET #{updateClause.join ','} WHERE id = #{id}"
    db.query sql, (err) ->
      callback err
  else
    callback()

# 完成一个任务
finishTask = (id, result, callback) ->
  opts = 
    status: 'done'
    result: ''
    runTime: yes

  if result.error 
    opts.result = _addslashes result.error.toString()
    opts.status = 'error'

  updateTask id, opts, (err) ->
    callback err

# 发布一个任务
createTask = (name, args, callback) ->
  argString = JSON.stringify args
  sql = "INSERT task(name, args, result) VALUES('#{name}', '#{argString}', '')"
  db.query sql, (err, ret) ->
    callback err, ret
  yes

module.exports = { getTask, finishTask, createTask, updateTask }