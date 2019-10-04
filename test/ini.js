module.exports = require('should');

var datasource = require('loopback-datasource-juggler').DataSource;

global.getDataSource = function() {
  var config = {
    host : '192.168.10.18',
    port : 8900,
    useSSL : false,
    debug : true
  };
  var ds = new datasource(require('../'), config);
  return ds;
};