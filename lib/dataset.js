var Dataset = function (name, dat) {
  if (!(this instanceof Dataset)) return new Dataset(name, dat)
  this.name = name || ''
  this.dat = dat
}

Dataset.prototype.get = function (key, cb) {
  this.dat.get(key, {dataset: this.name}, cb)
}

Dataset.prototype.put = function (key, value, cb) {
  this.dat.put(key, value, {dataset: this.name}, cb)
}

Dataset.prototype.del = function (key, cb) {
  this.dat.del(key, {dataset: this.name}, cb)
}

Dataset.prototype.batch = function (batch, cb) {
  this.dat.batch(batch, {dataset: this.name}, cb)
}

Dataset.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}
  opts.dataset = this.name
  return this.dat.createReadStream(opts)
}

Dataset.prototype.createWriteStream = function (opts) {
  if (!opts) opts = {}
  opts.dataset = this.name
  return this.dat.createWriteStream(opts)
}

module.exports = Dataset
