# 助手模块
fs     = require 'fs'
path   = require 'path'
async  = require 'async'
mkdirp = require 'mkdirp'
exec   = require('child_process').exec

uploadFiles = (files, callback) ->
  async.eachLimit files, 1, uploadGoogleData, (err) ->
    callback err

# 174机器利用sftp命令行工具上载google统计数据
# 例如:sftp -P19321 -b ./sftp_batchfile feeds-j1hk6a@partnerupload.google.com >> sftp_output.log 2>&1
# author: magicxiao
uploadGoogleData = (file, callback) ->
  files = getBatchfile(file)
  if not files
    return callback Error 'getBatchfile function error, dir created failed.'

  [batchFile, logFile] = files
  cmd = "sftp -P19321 -b #{batchFile} feeds-j1hk6a@partnerupload.google.com >> #{logFile} 2>&1"
  console.log "cmd: #{cmd}"
  # test cmd
  # cmd = "cat #{batchFile}"
  exec cmd, (err, stdout, stderr) ->
    callback err, stdout, stderr

getBatchfile = (fname) ->
  dir = resultDir 'google'
  return if not dir

  batchFile = path.join dir, 'sftp_batchfile'
  logFile = path.join dir, 'upload.log'

  content = [
    "-put #{fname}"
    "-rm #{fname}"
  ].join '\n'

  fs.writeFileSync batchFile, content

  [batchFile, logFile]

resultDir = (dirname) ->
  dir = path.join __dirname, "../../results/#{dirname}"
  try
    mkdirp.sync dir
    dir 
  catch error
    console.log error
    no

module.exports = { uploadFiles, resultDir }

# test:
# uploadFiles ['ttt1', 'ttt3'], (err) ->
#   console.log err
# console.log  resultDir('google')