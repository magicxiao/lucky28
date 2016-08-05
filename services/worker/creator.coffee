# 任务发布者模块
cronJob     = require('cron').CronJob
taskInfo    = require './src/taskInfo'
taskManager = require './src/taskManager.coffee'
moment      = require 'moment'

# 注册任务
registerTask = (task)->
  # validTask()
  new cronJob 
    cronTime: task.cronTime
    onTick: ->
      todayMoment     = moment()
      timestamp       = todayMoment.unix()
      todayDate       = todayMoment.format('YYYY-MM-DD')
      yesterdayMoment = todayMoment.subtract(1, 'day')
      yesterdayDate   = yesterdayMoment.format('YYYY-MM-DD')
      yesterdayMonth  = yesterdayMoment.format('YYYYMM')

      weekMoment = moment().isoWeekday(1)
      thisWeekMonday = weekMoment.format('YYYY-MM-DD')
      lastWeekMonday = weekMoment.subtract(7, 'day').format('YYYY-MM-DD')
      lastWeekSunday = weekMoment.add(6, 'day').format('YYYY-MM-DD')

      systemArgs = { 
        todayDate, 
        yesterdayDate, 
        yesterdayMonth,
        lastWeekMonday, 
        lastWeekSunday, 
        timestamp 
      }
      # console.log systemArgs

      args = task.getArgs systemArgs
      taskManager.createTask task.name, args, (err, ret) ->
        console.error err if err
    start: yes
    onComplete: (err) ->
      console.error err

for task in taskInfo
  registerTask task