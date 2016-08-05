moment = require 'moment'

module.exports =
  [
    {
      name: 'beijing28',
      getArgs: (systemArgs) ->
        return {}
      ,
      cronTime: '59 */5 * * * *'
    }
  ]