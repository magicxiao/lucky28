pool  = require '../../lib/poolCluster'
mysql = require 'mysql'
DB    = require 'database'
async = require 'async'
_     = require 'underscore'
MAX_VALUES_INSERT = 10000

_getColumns = (object) ->
  _.keys object

_getValues = (object) ->
  vals = _.values object
  _.map(vals, (item) -> mysql.escape(item))

_batchInsert = (databases, table, ignore, columns, values, callback) ->
  return callback() if values.length is 0

  async.eachLimit databases, 1, (dbconfig, cb) ->
    db = new DB(dbconfig.id, dbconfig.name)
    cols = columns.join ','
    vals = values.join ','
    ignore = if ignore then 'IGNORE' else ''
    sql = "INSERT #{ignore} INTO #{table}(#{cols}) VALUES #{vals}"
    db.query sql, cb
  , (err) ->
    callback err

_batchCreateDatabase = (databases, table, schemaSql, callback) ->
  async.eachLimit databases, 1, (dbconfig, cb) ->
    db = new DB(dbconfig.id, dbconfig.name)
    sql = "DROP TABLE IF EXISTS #{table}; #{schemaSql}"
    db.query sql, (err, ret) ->
      cb err
  , (err) ->
    callback err

transfer = (database, config, callback) ->
  async.eachLimit config.origin.tables, 1, (table, cb) ->
    console.log "[process] dbid:#{database.id}, dbname: #{database.name}, table: #{table} is transfering."
    async.waterfall [
      (cb) ->
        db = new DB(database.id, database.name)
        db.query "SHOW CREATE TABLE #{table}", (err, ret) ->
          if err
            cb err
          else if ret && ret[0]
            cb err, ret[0]['Create Table']
          else
            cb Error "no table schema: #{table}"
      # 新建要迁移的表结构
      (schemaSql, cb) ->
        _batchCreateDatabase config.target.databases, table, schemaSql, cb
      (cb) ->
        pool.getConn database.id, (err, conn) ->
          cb err, conn
      ,
      (conn, cb) ->
        conn.changeUser {database: database.name}, (err) ->
          conn.release() if err
          cb err, conn
      (conn, cb) ->
        columns = []
        values = []
        valuseCounter = 0
        query = conn.query "SELECT * FROM #{table}"
        query.on 'error', (err) ->
          console.log err
        query.on 'end', ->
          _batchInsert config.target.databases, table, config.ignore, columns, values, (err) ->
            pool.releaseConn conn, (releaseErr) ->
              cb err
        query.on 'fields', (fields)->
          columns = _.map fields, (f) -> f.name
          # console.log columns
          yes
        query.on 'result', (row) ->
          valuseCounter++
          values.push "(#{_getValues(row).join ','})"
          if valuseCounter >= MAX_VALUES_INSERT
            conn.pause()
            _batchInsert config.target.databases, table, config.ignore, columns, values, (err) ->
              valuseCounter = 0
              values = []
              if err
                pool.releaseConn conn, (releaseErr) ->
                  return cb err
              else
                conn.resume()
          yes
    ], (err) ->
      cb err
  , (err) ->
    callback err

module.exports = { transfer }

# test
# config = 
#   origin:
#     databases: [
#       {id: 0, name:'db_201607'}
#     ]
#     tables: ['tmp_apps_devices', 'tmp_devices_specimens']
#   # 要迁移的目标dbid和目标数据库
#   target: 
#     databases: [
#       { id: '1', name: 'planning_dev'}
#       { id: '1', name: 'planning_test'}
#     ]

# transfer {id: 0, name:'db_201607'}, config, (err, ret) ->
#   console.log 'end'

# yes