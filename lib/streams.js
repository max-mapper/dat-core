var fs = require('fs')
var pbs = require('pbs')

module.exports = pbs(fs.readFileSync(__dirname + '/../schema.proto'))
