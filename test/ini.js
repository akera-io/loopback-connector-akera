module.exports = require('should');

var datasource = require('loopback-datasource-juggler').DataSource;

global.getDataSource = function() {
  var config = {
    host : '10.10.10.6',
    port : 38900,
    useSSL : false,
    database : 'sports2000',
    debug : true
  };
  var ds = new datasource(require('../'), config);
  return ds;
};