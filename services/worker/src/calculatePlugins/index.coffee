fs   = require 'fs'
path = require 'path'

loadPlugins = ->
  obj = {}
  dir = __dirname
  fs.readdirSync(dir).forEach (file) ->
    fpath = path.join(dir, file)
    ext = path.extname(file)
    name = path.basename(file, ext)

    # if (fs.lstatSync(fpath).isDirectory()) {
    #   return obj[file] = module.exports(fpath, iterator)
    # }
    
    # Never load `index` 
    return if name in ['index', 'helper']

    mod = require(fpath)

    obj[name] = mod if (mod) 

  obj

plugins = loadPlugins()
module.exports = plugins