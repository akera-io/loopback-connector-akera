module.exports = DiscoveryDecorator;

/*
 * ! @param {AkeraConnector} Akera connector class
 */
function DiscoveryDecorator(AkeraConnector) {

  function getDatabasesMeta(conn, cb) {
    conn.getMetaData(function(err, meta) {
      if (err !== null)
        cb(err);
      else {
        meta.allDatabases().then(function(dbs) {
          cb(null, dbs);
        }, cb);
      }
    });
  }

  function getTableMeta(conn, schema, table, cb) {
    getDatabasesMeta(conn,
        function(err, dbs) {
          if (err !== null)
            cb(err);
          else {
            var callback = function(err) {
              if (err !== null)
                cb(err);

              for ( var t in tables) {
                if (table === tables[t].getName()) {
                  cb(null, tables[t]);
                  return;
                }
              }

              cb(new Error('Table not found in database: ' + schema + '.'
                  + table));
            };

            // select first database as default if not specified
            if (!schema)
              schema = dbs[0].getLname();

            for ( var d in dbs) {
              if (schema === dbs[d].getLname()) {
                var tables = [];
                var info = {
                  idx : d,
                  schema : schema
                };

                fetchSchemaTables(dbs, info, tables, callback);

                return;
              }
            }
            cb(new Error('Database not connected: ' + schema));
          }
        });
  }

  var dataTypeConversion = {
    DATE : 'Date',
    DATETIME : 'Date',
    'DATETIME-TZ' : 'Date',
    LOGICAL : 'Boolean',
    DECIMAL : 'Number',
    INTEGER : 'Number',
    INT64 : 'Number',
    CHARACTER : 'String',
    CLOB : 'String',
    BLOB : 'Buffer'
  };

  function getTableModel(tblMeta, cb) {
    tblMeta.getAllFields().then(function(fields) {
      var result = [];

      for ( var f in fields) {
        var field = fields[f];
        
        result.push({
          owner : tblMeta.getDatabase(),
          tableName : tblMeta.getName(),
          columnName : field.name,
          dataType : field.type,
          columnType : field.type,
          dataLength: null,
          dataPrecision: field.type.toUpperCase() === 'DECIMAL' ? field.decimals : null,
          dataScale: null,
          nullable : field.mandatory ? 'N' : 'Y',
          type : dataTypeConversion[field.type.toUpperCase()],
          generated: false
        });
      }
      console.log(result);
      cb(null, result);
    }, cb);
  }

  function getTablePrimaryKeys(tblMeta, cb) {
    tblMeta.getPk().then(function(pk) {
      var result = [];

      for ( var f in pk.fields) {
        var field = pk.fields[f];

        result.push({
          owner : tblMeta.getDatabase(),
          tableName : tblMeta.getName(),
          columnName : field.name,
          keySeq : field.fld_pos,
          pkName : field.idx_name
        });
      }

      cb(null, result);
    }, cb);
  }

  function getDatabaseSchemas(dbs, options) {
    var offset = 0;
    var limit = dbs.length;
    var result = [];

    if (options !== null && typeof options === 'object') {
      if (typeof options.offset === 'number')
        offset = options.offset;
      if (typeof options.skip === 'number')
        offset += options.skip;
      if (typeof options.limit === 'number')
        limit = options.limit;
    }

    for (var i = offset; i < limit; i++) {
      result.push({
        catalog : dbs[i].getLname(),
        schema : dbs[i].getLname()
      });
    }

    return result;
  }

  function getModelDefinitions(tblsMeta) {
    var result = [];

    for ( var t in tblsMeta) {
      result.push({
        type : 'table',
        name : tblsMeta[t].getName(),
        owner : tblsMeta[t].getDatabase()
      });
    }

    return result;
  }

  function getDatabaseTables(dbMeta, cb) {
    dbMeta.allTables().then(function(tables) {
      cb(null, tables);
    }, cb);
  }

  function fetchSchemaTables(dbs, info, tables, cb) {
    getDatabaseTables(dbs[info.idx], function(err, tbls) {
      if (err !== null)
        cb(err);
      else {
        for ( var t in tbls) {
          tables.push(tbls[t]);
        }
        // reach the end of databases or found the one we're looking for
        if ((info.schema && info.schema === dbs[info.idx].getLname())
            || ++info.idx === dbs.length)
          cb(null, tables);
        else
          fetchSchemaTables(dbs, info, tables, cb);
      }
    });
  }

  AkeraConnector.prototype.discoverDatabaseSchemas = function(options, cb) {
    if (!cb && typeof options === 'function') {
      cb = options;
      options = null;
    }

    getDatabasesMeta(this, function(err, dbs) {
      if (err !== null)
        cb(err);
      else {
        cb(null, getDatabaseSchemas(dbs, options));
      }
    });
  };

  /**
   * Discover model definitions
   * 
   * @param {Object}
   *         options Options for discovery
   * @param {Function}
   *         [cb] The callback function
   */
  AkeraConnector.prototype.discoverModelDefinitions = function(options, cb) {
    if (!cb && typeof options === 'function') {
      cb = options;
      options = null;
    }

    var schema = null;
    if (options !== null && typeof options === 'object') {
      schema = options.owner || options.schema;
    }

    getDatabasesMeta(this, function(err, dbs) {
      if (err !== null)
        cb(err);
      else {
        var tables = [];
        var info = {
          idx : 0,
          schema : schema
        };

        fetchSchemaTables(dbs, info, tables, function(err) {
          if (err !== null)
            cb(err);

          cb(null, getModelDefinitions(tables));
        });
      }
    });

  };

  /**
   * Discover model properties from a table
   * 
   * @param {String}
   *         table The table name
   * @param {Object}
   *         options The options for discovery (schema/owner)
   * @param {Function}
   *         [cb] The callback function
   * 
   */
  AkeraConnector.prototype.discoverModelProperties = function(table, options,
      cb) {
    var schema = null;

    if (!cb && typeof options === 'function') {
      cb = options;
      options = null;
    }

    if (table === null && typeof table !== 'string') {
      cb(new Error('Table name is mandatory for model properties discovery.'));
      return;
    }

    if (options !== null && typeof options === 'object') {
      schema = options.owner || options.schema;
    }

    if (!schema) {
      schema = this.getDefaultSchema();
    }

    getTableMeta(this, schema, table, function(err, tblMeta) {
      if (err) {
        cb(err);
      } else {
        getTableModel(tblMeta, cb);
      }
    });
  };

  /**
   * Discover model primary keys from a table
   * 
   * @param {String}
   *         table The table name
   * @param {Object}
   *         options The options for discovery (schema/owner)
   * @param {Function}
   *         [cb] The callback function
   * 
   */
  AkeraConnector.prototype.discoverPrimaryKeys = function(table, options, cb) {
    var schema = null;

    if (!cb && typeof options === 'function') {
      cb = options;
      options = null;
    }

    if (table === null && typeof table !== 'string') {
      cb(new Error('Table name is mandatory for model primary keys discovery.'));
      return;
    }

    if (options !== null && typeof options === 'object') {
      schema = options.owner || options.schema;
    }

    if (!schema) {
      schema = this.getDefaultSchema();
    }

    getTableMeta(this, schema, table, function(err, tblMeta) {
      if (err) {
        cb(err);
      } else {
        getTablePrimaryKeys(tblMeta, cb);
      }
    });
  };

  AkeraConnector.prototype.getDefaultSchema = function() {
    if (this.dataSource && this.dataSource.settings
        && this.dataSource.settings.database) {
      return this.dataSource.settings.database;
    }
    return undefined;
  };
}
