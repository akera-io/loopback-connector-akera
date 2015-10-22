var akeraApi = require('akera-api');
var debug = require('debug')('loopback:connector:akera');

var AkeraConnector = function AkeraConnector(cfg) {
  var config = cfg;
  var akeraConn = null;
  var modelDefinitions = {};
  var self = this;
  var debugEnable = debug.enable || cfg.debug;

  this.debuglog = function() {
    if (debugEnable === true)
      debug.apply(null, arguments);
  };

  // connects to an Akera Application Server
  this.connect = function(callback) {
    if (akeraConn !== null) {
      callback();
    } else {
      akeraApi.connect(config).then(
          function(conn) {
            akeraConn = conn;
            akeraConn.autoReconnect = true;
            self.debuglog('Connection established: %s.', config.host + ':'
                + config.port);
            callback();
          }, function(err) {
            self.debuglog('Connection error: %j', err);
            callback(err);
          });
    }
  };

  // closes the active connection
  this.disconnect = function(callback) {
    if (akeraConn === null) {
      callback();
    } else {
      var closeCallback = function(err) {
        akeraConn = null;
        callback(err);
      };
      akeraConn.disconnect().then(closeCallback, closeCallback);
    }
  };

  this.getMetaData = function(callback) {
    try {
      callback(null, akeraConn.getMetaData());
    } catch (err) {
      this.connect(function(connerr) {
        if (connerr !== undefined)
          callback(connerr);
        else
          self.getMetaData(callback);
      });
    }
  };

  // sends a select database request to Akera Application Server
  this.selectDatabase = function(dbName, callback) {
    self.debuglog('Selecting the database: %s', dbName);
    akeraConn.selectDatabase(dbName).then(callback, callback);
  };

  var transformSwitch = {
    like : function(where, key, keys) {
      changeOperator(where, key, keys, akeraApi.query.filter.operator.matches);
    },
    between : function(where, key) {
      var arr = where[key].between;
      if (arr.length !== 2)
        throw new Error('Incorrect between condition');
      if (!where[akeraApi.query.filter.operator.and])
        where[akeraApi.query.filter.operator.and] = [];
      var s = {};
      var e = {};
      s[key] = {
        ge : arr[0]
      };
      e[key] = {
        le : arr[1]
      };
      where[akeraApi.query.filter.operator.and].push(s, e);
      delete where[key];
    },
    gte : function(where, key, keys) {
      changeOperator(where, key, keys, akeraApi.query.filter.operator.ge);
    },
    lte : function(where, key, keys) {
      changeOperator(where, key, keys, akeraApi.query.filter.operator.le);
    },
    neq : function(where, key, keys) {
      changeOperator(where, key, keys, akeraApi.query.filter.operator.ne);
    },
    inq : function(where, key) {
      inqNin(where, key, akeraApi.query.filter.operator.or, 'inq');
    },
    nin : function(where, key) {
      inqNin(where, key, akeraApi.query.filter.operator.and, 'nin');
    }
  };

  var optionalsSwitch = {
    fields : function(q, filter) {
      if (filter.fields) {
        q.fields(filter.fields);
      }
    },
    limit : function(q, filter) {
      if (filter.limit) {
        q.limit(filter.limit);
      }
    },
    offset : function(q, filter) {
      var offset = 0;

      if (typeof filter.offset === 'number')
        offset += filter.offset;
      if (typeof filter.skip === 'number')
        offset += filter.skip;

      q.offset(offset);
    },
    order : function(q, filter) {
      if (filter.order) {
        if (filter.order instanceof Array) {
          for ( var i in filter.order) {
            setSorting(q, filter.order[i]);
          }
        } else if (typeof filter.order === 'string') {
          setSorting(q, filter.order);
        }
      }
    }
  };

  function setFilter(query, filter, qryType) {
    if (filter === null)
      return;

    self.debuglog('Query filter: %j', filter);

    filterWhere(query, filter);
    optionals(query, filter, qryType);
  }

  // converts from loopback filter sintax to akera-api filter
  // sintax(lte-lt,gte-gt,etc..);
  function filterWhere(q, filter) {
    if (typeof filter !== 'object' || filter.where === undefined)
      return;

    if (Object.keys(filter.where).length !== 0) {
      check(filter.where);
      q.where(filter.where);
    }
  }

  // case of and/or block
  function checkGroup(filter) {
    if (filter instanceof Array) {
      // check each condition from group
      for ( var c in filter) {
        check(filter[c]);
      }
    }
  }

  function checkSingle(where, key) {
    if (typeof where[key] === 'object') {
      var keys = Object.keys(where[key]);
      if (keys.length === 1) {
        if (transformSwitch[keys[0].toLowerCase()]) {
          transformSwitch[keys[0].toLowerCase()](where, key, keys);
        }
      }
    }
  }

  function check(where) {
    for ( var key in where) {
      if (key === akeraApi.query.filter.operator.and
          || key === akeraApi.query.filter.operator.or) {
        checkGroup(where[key]);
      } else {
        checkSingle(where, key);
      }
    }
  }

  function inqNin(where, key, op, oldOp) {
    var arr = where[key][oldOp];
    var c = oldOp === 'inq' ? akeraApi.query.filter.operator.eq
        : akeraApi.query.filter.operator.ne;

    if (!where[op])
      where[op] = [];

    for ( var i in arr) {
      var obj = {};
      var prop = {};
      prop[c] = arr[i];
      obj[key] = prop;
      where[op].push(obj);
    }

    delete where[key];
  }

  function changeOperator(where, key, keys, op) {
    var c = where[key];
    where[key] = {};
    where[key][op] = c[keys[0]];
  }

  function optionals(q, filter, qryType) {
    // defaults fields to *
    if (qryType === akeraApi.query.constants.action.select) {
      q.fields();
    }
    for ( var key in filter) {
      if (optionalsSwitch[key])
        optionalsSwitch[key](q, filter, qryType);
    }
  }

  function setSorting(q, sortData) {
    try {
      var s;
      s = sortData.split(' ');
      switch (s[1].toUpperCase()) {
      case 'DESC':
        q.by(s[0], true);
        break;
      case 'ASC':
        q.by(s[0], false);
        break;
      }
    } catch (err) {
    }
  }

  /**
   * Creates a local copy of the given models(pk, relations);
   * 
   * @param {model}
   *         model The model name
   */
  this.define = function(model) {
    try {
      var modelName = model.model.definition.name.toLowerCase();
      var pkName = null;

      self.debuglog('Define model: %s', modelName);

      try {
        // method may be undefined
        pkName = model.model.getIdName();
      } catch (err) {
        for ( var key in model.model.definition.properties) {
          if (model.model.definition.properties[key].id) {
            pkName = key;
            break;
          }
        }
      }

      var modelDefinition = {
        pk : pkName,
        relations : model.model.settings.relations
      };

      self.debuglog('Model definition: %j', modelDefinition);

      modelDefinitions[modelName] = modelDefinition;

    } catch (err) {
      self.debuglog('Model definition error: %j', err);
    }
  };

  /**
   * Finds all model instances matched by where
   * 
   * @param {model}
   *         model The model name
   * @param {filter}
   *         filter The where condition
   * @param {Function}
   *         callback The callback function
   */
  this.all = function(model, filter, callback) {

    var query = akeraConn.query.select(model);

    try {
      setFilter(query, filter, akeraApi.query.constants.action.select);
      query.all().then(function(rspData) {
        callback(null, rspData);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Set filter error %j', err);
      callback(err);
      return;
    }
  };

  /**
   * Counts all model instances matched by where
   * 
   * @param {model}
   *         model The model name
   * @param {Function}
   *         callback The callback function
   * @param {filter}
   *         filter The where condition
   */
  this.count = function(model, callback, filter) {

    var query = akeraConn.query.select(model);

    try {
      setFilter(query, filter, akeraApi.query.constants.action.select);
      query.count().then(function(rspData) {
        callback(null, rspData);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Set filter error %j', err);
      callback(err);
    }
  };

  /**
   * Creates a new model instance
   * 
   * @param {model}
   *         model The model name
   * @param {data}
   *         data Data to be inserted
   * @param {Function}
   *         callback The callback function
   */
  this.create = function(model, data, callback) {
    try {
      var pk = modelDefinitions[model.toLowerCase()].pk;
      if (!pk) {
        callback(new Error('No primary key definition for model ' + model));
        return;
      }

      var query = akeraConn.query.insert(model);

      query.set(data);

      query.fetch().then(function(row) {
        // returns only the value of the primary key
        callback(null, row[pk]);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      callback(new Error('No definition found for model ' + model));
    }
  };

  /**
   * Destroy all model instances matched by filter
   * 
   * @param {model}
   *         model The model name
   * @param {filter}
   *         filter The where condition
   * @param {Function}
   *         callback The callback function
   */
  this.destroyAll = function(model, filter, options, callback) {
    var query = akeraConn.query.destroy(model);

    try {
      setFilter(query, filter, akeraApi.query.constants.action.destroy);

      query.go().then(function(rspData) {
        callback(null, rspData);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Set filter error %j', err);
      callback(err);
    }
  };
  /**
   * Updates a model instance matched by filter
   * 
   * @param {model}
   *         model The model name
   * @param {filter}
   *         filter The where filter
   * @param {data}
   *         data Data for which the record to be updated with
   * @param {Function}
   *         callback The callback function
   */
  this.update = function(model, filter, data, callback) {
    var query = akeraConn.query.update(model);

    try {
      setFilter(query, filter, akeraApi.query.constants.action.update);
      query.set(data);

      query.fetch().then(function(rspData) {
        callback(null, rspData);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Set filter error %j', err);
      callback(err);
    }
  };

  /**
   * Update a model instance matched by id or creates a new instance
   * 
   * @param {model}
   *         model The model name
   * @param {data}
   *         data Data for which the record to be updated with
   * @param {Function}
   *         callback The callback function
   */
  this.updateOrCreate = function(model, data, filter, callback) {
    var query = akeraConn.query.upsert(model);

    try {
      setFilter(query, filter, akeraApi.query.constants.action.upsert);
      query.set(data);

      query.fetch().then(function(rspData) {
        callback(null, rspData);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Set filter error %j', err);
      callback(err);
    }
  };

  /**
   * Update a model instance matched by id
   * 
   * @param {model}
   *         model The model name
   * @param {id}
   *         id Primary key value
   * @param {data}
   *         data Data for which the record to be updated with
   * @param {Function}
   *         callback The callback frunction
   */
  this.updateAttributes = function(model, id, data, callback) {
    try {
      var pk = modelDefinitions[model.toLowerCase()].pk;
      if (!pk) {
        callback(new Error('No primary key definition for model ' + model));
        return;
      }

      var query = akeraConn.query.update(model);
      var filter = {
        where : {}
      };
      filter.where[pk] = id;

      try {
        query.set(data);
        setFilter(query, filter, akeraApi.query.constants.action.update);

        query.fetch().then(function(row) {
          // returns only the value of the primary key
          callback(null, row);
        }, function(err) {
          self.debuglog('Query execution error: %j', err);
          callback(err);
        });
      } catch (err) {
        self.debuglog('Set filter error %j', err);
        callback(err);
      }
    } catch (err) {
      callback(new Error('No definition found for model ' + model));
    }
  };
};

/**
 * Initialize the Akera connector for the given datasource
 * 
 * @param {datasource}
 *         datasource loopback-datasource-juggler datasource
 * @param {Function}
 *         callback datasource callback function
 */
exports.initialize = function(datasource, callback) {
  var config = {
    host : datasource.settings.host || 'localhost',
    port : datasource.settings.port || '3000',
    useSSL : datasource.settings.useSSL || false,
    database : datasource.settings.database || null,
    debug : datasource.settings.debug || false
  };

  var connector = new AkeraConnector(config);
  datasource.connector = connector;

  datasource.connector.connect(function(err) {
    if (err !== undefined)
      callback(err);
    else {
      if (config.database) {
        datasource.connector.selectDatabase(config.database, function(err) {
          callback(err);
        });
      } else {
        callback();
      }
    }
  });
};

exports.AkeraConnector = AkeraConnector;

require('./discovery')(AkeraConnector);

