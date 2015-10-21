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

  // formats the strongloop filter to akera filter
  function getQuery(queryType, parameters, callback) {
    var model = parameters.model.toLowerCase();
    var filter = parameters.filter || null;
    var data = parameters.data || null;

    try {
      var q = akeraConn.query[queryType](model);

      try {
        setFilter(q, filter, queryType);
      } catch (err) {
        self.debuglog('Set filter error %j', err);
        callback(err);
        return;
      }

      if (data !== null)
        q.set(data);

      return q;
    } catch (err) {
      self.debuglog('Query build error %j', err);
      return null;
    }
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
   * Convenience method for querying the server
   * 
   * @param {parameters}
   *         Parameters [model, filter, data]
   * @param {methods}
   *         methods Set of methods to be called for creating the filter and
   *         running the query
   * @param {Function}
   *         callback The callback function
   */
  var qry = function(queryType, queryAction, parameters, callback) {
    var query = null;

    self.debuglog('Build query: %s', queryType + ':' + queryAction);

    try {
      query = getQuery(queryType, parameters, callback);
      self.debuglog('Query: %j', query.build());
    } catch (err) {
      self.debuglog('Query build error: %s', err);

      callback(err);
      return;
    }

    if (query === null)
      return;

    query[queryAction]().then(function(rspData) {
      callback(null, rspData);
    }, function(err) {
      self.debuglog('Query execution error: %s', err);
      callback(err);
    });

  };

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
    qry(akeraApi.query.constants.action.select,
        AkeraConnector.constants.execute.get, {
          model : model,
          filter : filter
        }, callback);
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

    qry(akeraApi.query.constants.action.select,
        AkeraConnector.constants.execute.count, {
          model : model,
          filter : filter
        }, function(err, num) {
          callback(err, num);
        });
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
    } catch (err) {
      callback(new Error('No primary key definition for model ' + model));
      return;
    }

    qry(akeraApi.query.constants.action.insert,
        AkeraConnector.constants.execute.fetch, {
          model : model,
          data : data
        }, function(err, iRow) {
          if (err)
            callback(err);
          else {
            // returns only the value of the primary key
            var pkVal = iRow[pk];
            callback(null, pkVal);
          }
        });
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
    qry(akeraApi.query.constants.action.destroy,
        AkeraConnector.constants.execute.go, {
          model : model,
          filter : filter
        }, callback);
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
    qry(akeraApi.query.constants.action.update,
        AkeraConnector.constants.execute.fetch, {
          model : model,
          filter : filter,
          data : data
        }, callback);
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
    qry(akeraApi.query.constants.action.upsert,
        AkeraConnector.constants.execute.fetch, {
          model : model,
          filter : filter,
          data : data
        }, callback);
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
    var pk = modelDefinitions[model.toLowerCase()].pk;

    if (!pk) {
      callback(new Error('No primary key definition for model ' + model));
      return;
    }

    var filter = {
      where : {}
    };
    filter.where[pk] = id;

    qry(akeraApi.query.constants.action.update,
        AkeraConnector.constants.execute.fetch, {
          model : model,
          filter : filter,
          data : data
        }, function(err, rsp) {
          callback(null, false);
          if (err) {
            callback(err);
            return;
          }
          callback(null, rsp);
        });
  };
};

AkeraConnector.constants = {
  execute : {
    get : 'all',
    count : 'count',
    go : 'go',
    fetch : 'fetch'
  }
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