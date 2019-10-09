
import { Class, Entity, DataObject, Filter, Where, Count, Callback, AnyObject } from '@loopback/repository';
import { AkeraConnector, ConnectionOptions } from './akera-connector';
import { DiscoverModelDefinitionsOptions, ModelDefinition, ModelPropertyDefinition, ModelKeyDefinition } from './akera-discovery';

export interface Datasource {
  settings?: ConnectionOptions
}

/**
 * The signature of loopback connector is different from that
 * on @loopback/repository, we implement the later and use a proxy.
 */
export class AkeraConnectorProxy {
  private connector: AkeraConnector;

  constructor(
    config?: ConnectionOptions
  ) {
    if (!!config)
      this.connector = new AkeraConnector(config);
  }

  /**
 * Initialize the Akera connector for the given datasource
 * 
 * @param {datasource}
 *         datasource loopback-datasource-juggler datasource
 * @param {Function}
 *         callback datasource callback function
 */
  public initialize = function (datasource: Datasource, callback?: Callback<undefined>) {
    console.log('Akera Proxy initialize');
    this.connector = new AkeraConnector(datasource.settings);
  };

  // connects to an Akera Application Server
  public connect(callback?: Callback<undefined>): void {
    console.log('Akera Proxy connect');
    this.connector.connect().then(() => {
      callback && callback(null);
    }).catch(err => {
      callback && callback(err);
    });
  }

  // closes the active connection
  public disconnect(callback?: Callback<undefined>): void {
    console.log('Akera Proxy disconect');
    this.connector.disconnect().then(() => {
      callback && callback(null);
    }).catch(err => {
      callback && callback(err);
    });
  }


  /**
   * Creates a local copy of the given models(pk, relations);
   * 
   * @param {schema}
   *         schema The schema model
   */
  public define(schema: { model: Class<Entity> }) {
    if (schema && schema.model)
      this.connector.define(schema.model);
  }

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
  public all(modelName: string, filter: Filter, options: AnyObject, callback: Callback<DataObject<Entity>[]>) {
    console.log('Akera Proxy all');
    this.connector.find(this.connector.getModel(modelName), filter, options).then((rows) => {
      callback && callback(null, rows);
    }).catch((err) => {
      callback && callback(err);
    });
  }

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
  public count(modelName: string, where: Where, options: AnyObject, callback: Callback<Count>) {
    console.log('Akera Proxy count');
    this.connector.count(this.connector.getModel(modelName), where, options).then((count) => {
      callback && callback(null, count);
    }).catch((err) => {
      callback && callback(err);
    });
  }

  /**
   * Creates a new model instance
   * 
   * @param {String}
   *         modelName The model name
   * @param {Object}
   *         data Data to be inserted
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  public create(modelName: string, data: DataObject<Entity>[] | DataObject<Entity>, options: AnyObject, callback: Callback<DataObject<Entity>[] | DataObject<Entity>>) {
    //test if data is an array of objects who must be created
    console.log(data);
    if (Array.isArray(data)) {
      console.log('Akera Proxy createAll');
      this.connector.createAll(this.connector.getModel(modelName), data, options)
        .then((response) => {
          callback && callback(null, response);
        })
        .catch((err) => {
          callback && callback(err);
        });
    }
    else {
      console.log('Akera Proxy create');
      this.connector.create(this.connector.getModel(modelName), data, options)
        .then((response) => {
          callback && callback(null, response);
        })
        .catch((err) => {
          callback && callback(err);
        });
    }
  }

  /**
   * Discover model definitions
   * 
   * @param {Object}
   *         options Options for discovery
   * @param {Function}
   *         [callback] The callback function
   */
  public discoverModelDefinitions(options: DiscoverModelDefinitionsOptions | Callback<ModelDefinition[]>,
    callback?: Callback<ModelDefinition[]>) {
    const cb = typeof options === 'function' ? options : callback;
    const opts = typeof options === 'function' ? {} : options;

    this.connector.getDiscovery().discoverModelDefinitions(opts).then((definitions) => {
      cb && cb(null, definitions);
    }).catch((err) => {
      cb && cb(err);
    });
  }

  /**
   * Discover model properties from a table
   * 
   * @param {String}
   *         table The table name
   * @param {Object}
   *         options The options for discovery (schema/owner)
   * @param {Function}
   *         [callback] The callback function
   * 
   */
  public discoverModelProperties(table: string,
    options: DiscoverModelDefinitionsOptions | Callback<ModelPropertyDefinition[]>,
    callback?: Callback<ModelPropertyDefinition[]>) {
    const cb = typeof options === 'function' ? options : callback;
    const opts = typeof options === 'function' ? {} : options;
    this.connector.getDiscovery().discoverModelProperties(table, opts).then((definitions) => {
      cb && cb(null, definitions);
    }).catch((err) => {
      cb && cb(err);
    });
  }

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
  public discoverPrimaryKeys(table: string,
    options: DiscoverModelDefinitionsOptions | Callback<ModelKeyDefinition[]>,
    callback?: Callback<ModelKeyDefinition[]>) {
    const cb = typeof options === 'function' ? options : callback;
    const opts = typeof options === 'function' ? {} : options;
    this.connector.getDiscovery().discoverPrimaryKeys(table, opts).then((definitions) => {
      cb && cb(null, definitions);
    }).catch((err) => {
      cb && cb(err);
    });
  }

}

