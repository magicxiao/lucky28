_           = require 'underscore'
DB          = require 'database'
db          = new DB()
async       = require 'async'
utils       = require './aggregation'
taskconf    = require './taskconf'
taskManager = require './taskManager'
calcPlugins = require './calculatePlugins'
lib         = require 'lib'
fs          = require 'fs'

nodesLength = 0
calcedNodes = 0

_initProgress = (total) ->
  nodesLength = total
  calcedNodes = 0

_progress = ->
  calcedNodes++
  console.info "Progress is #{Math.floor((calcedNodes/nodesLength) * 100)}%"

_csvFormat = (list = []) ->
  return '' if list.length is 0

  header = list[0]
  keys   = _.keys header

  ret    = keys.join ','
  for item in list
    values = _.values item
    ret += "\n#{values.join ','}"

  ret

_getCalcConfiguration = (config) ->
  if config.taskName
    calcFunction = (useless, callback) ->
      calcPlugins[config.taskName] config, callback
    [['useless'], calcFunction]
  else
    noCalcFunction = (useless, callback) ->
      callback()
    [['useless'], noCalcFunction]

# 返回给上层task config
complete = (err, taskConfig = {}, callback) ->
  callback err, taskConfig

run = (task, callback) ->
  config = taskconf task

  if not config
    return complete Error("#{task.name} task without SQL config"), null, callback

  console.info "Task #{task.id} SQL: #{config.SQL}"

  [calcNodes, calcFunction] = _getCalcConfiguration config

  async.waterfall [
    (cb) ->
      taskManager.updateTask task.id, { SQL: config.SQL }, (err) ->
        cb err
    ,
    (cb) ->
      async.mapLimit calcNodes, 5, calcFunction, (err, nodesResults) ->
        cb err, nodesResults
  ], (err, nodesResults) ->
    return complete err, config, callback if err
    return complete err, config, callback if config.noResult

    console.info "Aggregation start."
    startTime = (new Date()).getTime()
    utils.aggregation nodesResults, config.dimensions, config.metrics, (err, results) ->
      endTime = (new Date()).getTime()
      console.info "Aggregation finish. Cost times: #{Math.floor((endTime - startTime)/1000)}s. Results length is #{results.length}"
      handleResults results, task, config, callback

handleResults = (results, task, config, callback) ->
  async.waterfall [
    # 处理插件
    (cb) ->
      if typeof config.plugin is 'function'
        config.plugin task, config, results, (err) ->
          cb err
      else
        cb()
    ,
    # 处理csv，或者将结果处理成string
    (cb) ->
      if config.format is 'csv'
        console.info "[feature] results will convert csv format."
        cb null, _csvFormat results
      else
        cb null, JSON.stringify results
    ,
    # 将结果写在本地文件
    (results, cb) ->
      fname = lib.taskFilePath task
      fs.writeFile fname, results, (err) ->
        cb err, fname, results
    ,
    # 将结果记录到数据库
    (fname, results, cb) ->
      opts = 
        result: fname

      if config.resultRecordToTable
        console.info "[feature] resultRecordToTable enabled."
        if results.length > 21880
          console.warn "results length gt 21880, truncation."
          results = results.substr 0, 21880
        opts.result = results
       
      taskManager.updateTask task.id, opts, (err) ->
        cb err 
  ], (err) ->
    complete err, config, callback

module.exports = { run }