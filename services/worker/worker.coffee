cronJob     = require('cron').CronJob
async       = require 'async'
work        = require './src/worker'
taskManager = require './src/taskManager.coffee'

lock = false

locked = ->
  lock = true

unlock = ->
  lock = false

_triggerTask = (triggers = [], args = {}, callback) ->
  async.eachLimit triggers, 1, (triggerName, cb) ->
    console.log "Create trigger task : #{triggerName}"
    taskManager.createTask triggerName, args, (err) ->
      cb err
  , (err) ->
    callback err

scanTask = ->
  return if lock
    
  locked()

  async.waterfall [
    (cb) ->
      taskManager.getTask (err, task) ->
        cb err, task
    (task, cb) ->
      if task
        work.run task, (err, config) ->
          cb err, task, config
      else
        cb()
  ], (error, task, config) ->
    if task
      async.waterfall [
        # 完成任务
        (cb) ->
          taskManager.finishTask task.id, { error: error }, (err) ->
            cb err    
        ,
        # 触发新任务
        (cb) ->
          if config.triggers
            console.log "[feature] trigger enabled."
            _triggerTask config.triggers, config.args, cb
          else
            cb()
      ], (err) ->
        console.log "Task #{task.id} finished, #{if error then 'has error.' else 'no error.'}"
        console.error error if error    

        if err
          console.warn 'Has error when finish task:'
          console.error err 
        console.log '\n'
        unlock()
        # 如果有任务完成，立刻扫描一下次任务，直到没有后续任务，减少任务等待时间
        scanTask()
    else
      console.error error if error
      unlock()

# 每10秒扫描系统有没有任务执行
new cronJob 
  cronTime: '*/10 * * * * *'
  onTick: ->
    scanTask()
  start: yes
  onComplete: (err) ->
    console.error err

scanTask()