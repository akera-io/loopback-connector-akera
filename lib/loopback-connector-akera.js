var akeraApi = require('akera-api');
var debug = require('debug')('loopback:connector:akera');

var AkeraConnector = function AkeraConnector(cfg) {
  this.connection = null;

  var config = cfg;
  var modelDefinitions = {};
  var self = this;
  var debugEnable = debug.enable || cfg.debug;

  this.debuglog = function() {
    if (debugEnable === true)
      debug.apply(null, arguments);
  };

  // connects to an Akera Application Server
  this.connect = function(callback) {
    if (self.connection !== null) {
      callback();
    } else {
      akeraApi.connect(config).then(
          function(conn) {
            self.connection = conn;
            self.connection.autoReconnect = true;
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
    if (self.connection === null) {
      callback();
    } else {
      var closeCallback = function(err) {
        self.connection = null;
        callback(err);
      };
      self.connection.disconnect().then(closeCallback, closeCallback);
    }
  };

  this.getMetaData = function(callback) {
    try {
      callback(null, self.connection.getMetaData());
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
    self.connection.selectDatabase(dbName).then(callback, callback);
  };

  var transformSwitch = {
    like : function(where, key, op) {
      renameProperty(where[key], op, akeraApi.query.filter.operator.matches);
    },
    between : function(where, key, op) {
      var arr = where[key][op];
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
    gte : function(where, key, op) {
      renameProperty(where[key], op, akeraApi.query.filter.operator.ge);
    },
    lte : function(where, key, op) {
      renameProperty(where[key], op, akeraApi.query.filter.operator.le);
    },
    neq : function(where, key, op) {
      renameProperty(where[key], op, akeraApi.query.filter.operator.ne);
    },
    inq : function(where, key, op) {
      inqNin(where, key, akeraApi.query.filter.operator.or, op);
    },
    nin : function(where, key, op) {
      inqNin(where, key, akeraApi.query.filter.operator.and, op);
    }
  };

  var optionalsSwitch = {
    fields : function(q, filter) {
      if (filter.fields) {
        var modelFields = q.model && q.model.fields;

        if (modelFields) {
          var fields = filter.fields.forEach(function(field) {
            return modelFields[field] || field;
          });

          q.fields(fields);

        } else {
          q.fields(filter.fields);
        }
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
        offset = filter.offset;
      else if (typeof filter.skip === 'number')
        offset = filter.skip;

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

  function setFilter(query, filter) {
    if (filter === null)
      return;

    self.debuglog('Query filter: %j', filter);

    filterWhere(query, filter);
    optionals(query, filter);
  }

  // converts from loopback filter sintax to akera-api filter
  // sintax(lte-lt,gte-gt,etc..);
  function filterWhere(q, filter) {
    if (typeof filter !== 'object' || filter.where === undefined)
      return;

    if (Object.keys(filter.where).length !== 0) {
      check(q, filter.where);
      q.where(filter.where);
    }
  }

  // case of and/or block
  function checkGroup(query, group) {
    if (group instanceof Array) {
      // check each condition from group
      for ( var c in group) {
        check(query, group[c]);
      }
    }
  }

  function checkSingle(where, key) {

    if (typeof where[key] === 'object' && where[key] !== null) {
      var keys = Object.keys(where[key]);
      if (keys.length === 1) {
        var op = keys[0].toLowerCase();
        if (transformSwitch[op]) {
          transformSwitch[op](where, key, op);
        }
      }
    }
  }

  function renameProperty(obj, name, alias) {
    if (obj[name] !== undefined) {
      obj[alias] = obj[name];
      delete obj[name];
    }
  }

  function renameProperties(obj, fieldMap) {
    if (fieldMap && typeof fieldMap === 'object') {
      for ( var key in fieldMap) {
        renameProperty(obj, key, fieldMap[key]);
      }
    }

    return obj;
  }

  function modelToRow(data, model) {

    data = renameProperties(data, model.fieldMap);

    for ( var key in data) {
      if (Buffer.isBuffer(data[key])) {
        data[key] = data[key].toString('base64');
      }
    }

    return data;
  }

  function switchKeyValues(obj) {
    if (obj && typeof obj === 'object') {
      var flip = {};
      for ( var key in obj) {
        flip[obj[key]] = key;
      }

      return flip;
    }
    return obj;
  }

  function rowToModel(row, model, mapFields) {

    if (mapFields === true)
      row = renameProperties(row, switchKeyValues(model.fieldMap));

    for ( var key in model.fields) {
      if (row[key]) {
        switch (model.fields[key].type) {
        case 'Buffer':
          row[key] = new Buffer(row[key], 'base64');
          break;
        default:
          break;
        }
      }
    }

    return row;
  }

  function check(query, where) {
    var fieldMap = query.model && query.model.fieldMap;

    for ( var key in where) {
      if (key === akeraApi.query.filter.operator.and
          || key === akeraApi.query.filter.operator.or) {
        checkGroup(query, where[key]);
      } else {
        // filter set on aliased field
        if (fieldMap && fieldMap[key]) {
          renameProperty(where, key, fieldMap[key]);
          checkSingle(where, fieldMap[key]);
        } else {
          checkSingle(where, key);
        }
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

  function optionals(q, filter) {
    for ( var key in filter) {
      if (optionalsSwitch[key])
        optionalsSwitch[key](q, filter);
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

  function getAkeraField(name, field) {
    if (field.akera && field.akera.columnName
        && name !== field.akera.columnName) {
      return {
        name : field.akera.columnName,
        alias : name,
        type : field.type.name || field.type
      };
    } else {
      return name;
    }
  }

  function getFieldMap(fields) {
    var fieldMap = null;

    fields.filter(function(field) {
      if (field.alias)
        return field;
    }).forEach(function(field) {
      fieldMap = fieldMap || {};
      fieldMap[field.alias] = field.name;
    });

    return fieldMap;
  }

  /**
   * Creates a local copy of the given models(pk, relations);
   * 
   * @param {schema}
   *         schema The schema model
   */
  this.define = function(schema) {
    try {
      var model = schema.model || schema;
      var modelName = model.definition.name.toLowerCase();
      var fields = [];
      var pk = [];

      self.debuglog('Define model: %s', modelName);

      for ( var key in model.definition.properties) {
        var field = model.definition.properties[key];

        if (field.id) {
          pk[field.id - 1] = key;
        }

        fields.push(getAkeraField(key, field));
      }

      var modelDefinition = {
        fields : fields,
        pk : pk,
        fieldMap : getFieldMap(fields),
        relations : model.relations
      };

      try {
        var settings = model.settings.settings || model.settings;
        modelDefinition.table = settings.akera.schema ? settings.akera.schema
            + '.' + settings.akera.table : settings.akera.table;
      } catch (e) {
        modelDefinition.table = modelName;
      }

      self.debuglog('Model definition: %j', modelDefinition);

      modelDefinitions[modelName] = modelDefinition;

    } catch (err) {
      self.debuglog('Model definition error: %j', err);
    }
  };

  /**
   * Finds all model instances matched by where
   * 
   * @param {String}
   *         model The model name
   * @param {Object}
   *         filter The where condition
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.all = this.find = function(model, filter, options, callback) {
    // might have been added in newer versions
    if (!callback && typeof options === 'function')
      callback = options;

    var modelInfo = modelDefinitions[model.toLowerCase()];
    var query = self.connection.query.select(modelInfo.table);

    // if no field selection we return all model fields
    query.fields(self.getSelectedFields(modelInfo, filter.fields));
    query.model = modelInfo;

    try {
      setFilter(query, filter);
      query.all().then(function(rows) {
        if (Array.isArray(rows)) {
          for ( var i in rows) {
            rows[i] = rowToModel(rows[i], modelInfo, false);
          }
        }
        callback(null, rows);
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
  this.count = function(model, where, options, callback) {
    var modelInfo = modelDefinitions[model.toLowerCase()];
    var query = self.connection.query.select(modelInfo.table);

    if (typeof where === 'function') {
      // Backward compatibility for 1.x style signature:
      // count(model, callback, where)
      var tmp = options;
      callback = where;
      where = tmp;
    }

    try {
      if (where) {
        query.model = modelInfo;
        setFilter(query, {
          where : where
        });
      }

      query.count().then(function(rspData) {
        callback(null, Number(rspData));
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Count statement error %j', err);
      callback(err);
    }
  };

  /**
   * Creates a new model instance
   * 
   * @param {String}
   *         model The model name
   * @param {Object}
   *         data Data to be inserted
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.create = function(model, data, options, callback) {
    // might have been added in newer versions
    if (!callback && typeof options === 'function')
      callback = options;

    try {
      var modelInfo = modelDefinitions[model.toLowerCase()];
      var query = self.connection.query.insert(modelInfo.table);

      query.set(modelToRow(data, modelInfo));

      query.fetch().then(function(row) {
        try {
          var rowid = self.getPKValues(modelInfo, row, true);

          // one column pk only value is expected :(
          if (modelInfo.pk.length === 1)
            callback(null, rowid[modelInfo.pk[0]]);
          else
            callback(null, rowid);
        } catch (err) {
          callback(err);
        }

      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      callback(new Error('No definition found for model ' + model));
    }
  };

  /**
   * Update a model instance or create a new one if not found
   * 
   * @param {String}
   *         model The model name
   * @param {Object}
   *         data Data to be inserted
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.updateOrCreate = function(model, data, options, callback) {
    // might have been added in newer versions
    if (!callback && typeof options === 'function')
      callback = options;
    try {
      var modelInfo = modelDefinitions[model.toLowerCase()];
      var query = self.connection.query.upsert(modelInfo.table);

      query.set(modelToRow(data, modelInfo));

      query.fetch().then(function(row) {
        callback(null, rowToModel(row, modelInfo, true));
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      callback(new Error('No definition found for model ' + model));
    }
  };

  /**
   * Check if a model instance exists for given id
   * 
   * @param {model}
   *         model The model name
   * @param {*}
   *         id The primary key value(s)
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.exists = function(model, id, options, callback) {

    try {
      var modelInfo = modelDefinitions[model.toLowerCase()];
      var where = self.getPKFilter(modelInfo, id);

      self.count(model, where, options, function(err, count) {
        if (err)
          callback(err);
        else
          callback(null, count >= 1);
      });
    } catch (err) {
      self.debuglog('Delete statement error %j', err);
      if (!callback && typeof options === 'function')
        callback = options;
      callback(err);
    }

  };

  /**
   * Destroy a model instance by id
   * 
   * @param {model}
   *         model The model name
   * @param {*}
   *         id The primary key value(s)
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.destroy = this.deleteById = this.destroyById = function(model, id,
      options, callback) {

    try {
      var modelInfo = modelDefinitions[model.toLowerCase()];
      var where = self.getPKFilter(modelInfo, id);

      self.destroyAll(model, where, options, callback);
    } catch (err) {
      self.debuglog('Delete statement error %j', err);
      if (!callback && typeof options === 'function')
        callback = options;
      callback(err);
    }

  };

  /**
   * Destroy all model instances matched by filter
   * 
   * @param {model}
   *         model The model name
   * @param {Object}
   *         where The where condition
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.destroyAll = function(model, where, options, callback) {
    var modelInfo = modelDefinitions[model.toLowerCase()];
    var query = self.connection.query.destroy(modelInfo.table);

    query.model = modelInfo;

    try {
      setFilter(query, {
        where : where
      });

      query.go().then(function(affectedRows) {
        callback(null, {
          count : affectedRows
        });
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      self.debuglog('Delete statement error %j', err);
      callback(err);
    }
  };
  /**
   * Updates a model instance matched by filter
   * 
   * @param {String}
   *         model The model name
   * @param {Object}
   *         filter The where filter
   * @param {Object}
   *         data Data for which the record to be updated with
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.update = this.updateAll = function(model, filter, data, options,
      callback) {

    // might have been added in newer versions
    if (!callback && typeof options === 'function')
      callback = options;

    var modelInfo = modelDefinitions[model.toLowerCase()];
    var query = self.connection.query.update(modelInfo.table);

    query.model = modelInfo;

    try {
      setFilter(query, filter);
      query.set(modelToRow(data, modelInfo));

      query.fetch().then(function(rows) {
        if (Array.isArray(rows)) {
          for ( var i in rows) {
            rows[i] = rowToModel(rows[i], modelInfo, true);
          }
        }
        callback(null, rows);
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
   * @param {String}
   *         model The model name
   * @param {Object}
   *         data Data for which the record to be updated with
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.save = function(model, data, options, callback) {
    var modelInfo = modelDefinitions[model.toLowerCase()];

    self.updateAttributes(model, self.getPKValues(modelInfo, data), data,
        options, callback);
  };

  /**
   * Update a model instance matched by id
   * 
   * @param {String}
   *         model The model name
   * @param {*}
   *         id Primary key value(s)
   * @param {Object}
   *         data Data for which the record to be updated with
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  this.updateAttributes = function(model, id, data, options, callback) {

    // might have been added in newer versions
    if (!callback && typeof options === 'function')
      callback = options;

    try {
      var modelInfo = modelDefinitions[model.toLowerCase()];
      var filter = self.getPKFilter(modelInfo, id);
      var query = self.connection.query.update(modelInfo.table);
      query.model = modelInfo;

      query.set(modelToRow(data, modelInfo));
      setFilter(query, filter);

      query.fetch().then(function(row) {
        // usually on update fields value might change on the back-end, we send
        // the updated values as well not just the affected rows count
        var result = {
          data : rowToModel(row, modelInfo, true)
        };

        result.count = Array.isArray(row) ? row.length : 1;
        callback(null, result);
      }, function(err) {
        self.debuglog('Query execution error: %j', err);
        callback(err);
      });
    } catch (err) {
      callback(err);
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

AkeraConnector.prototype.getPKValues = function(modelInfo, row, reverseMap) {
  var pk = modelInfo.pk;

  if (!pk || pk.length === 0)
    return null;

  // returns only the value of the primary key(s)
  var rowid = {};
  reverseMap = reverseMap || false;

  pk
      .forEach(function(key) {
        var dbKey = (reverseMap && modelInfo.fieldMap && modelInfo.fieldMap[key]) ? modelInfo.fieldMap[key]
            : key;

        if (!row[dbKey])
          throw new Error('Primary key value not set: ' + key);

        rowid[key] = row[dbKey];
      });

  return rowid;
};

AkeraConnector.prototype.getPKFilter = function(modelInfo, id) {
  var self = this;
  var pk = modelInfo && modelInfo.pk;

  if (!pk || pk.length === 0) {
    throw new Error('No primary key definition.');
  }

  var filter = {};

  if (pk.length === 1)
    filter[pk] = Array.isArray(id) ? id[0] : id;
  else {
    if (pk.length !== id.length) {
      throw new Error('Not all primary key values provided.');
    }

    var keyFilters = [];
    for ( var key in pk) {
      var keyFilter = {};
      keyFilter[pk[key]] = id[key];
      keyFilters.push(keyFilter);
    }

    filter = self.connection.query.filter.and(keyFilters);
  }

  return {
    where : filter
  };
};

AkeraConnector.prototype.getSelectedFields = function(modelInfo, fields) {
  // if no field selection we return all model fields
  if (fields && Array.isArray(fields)) {
    var select = [];

    modelInfo.fields.forEach(function(field) {
      if (typeof field === 'string' && fields.indexOf(field) !== -1)
        select.push(field);
      if (typeof field === 'object' && fields.indexOf(field.alias) !== -1)
        select.push(field);
    });

    return select;
  } else {
    return modelInfo.fields;
  }
};

exports.AkeraConnector = AkeraConnector;

require('./discovery')(AkeraConnector);
