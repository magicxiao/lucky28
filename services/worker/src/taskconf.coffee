plugins  = require './resultPlugins'

taskConfig = 
  beijing28: (args) ->
    args.noResult = true
    args

module.exports = (task) ->
  fun = taskConfig[task.name]
  if fun
    args         = JSON.parse task.args
    args.appId   = task.appId
    args.taskName = task.name
    fun args
  else
    false
