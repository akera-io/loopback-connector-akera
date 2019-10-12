
import { Class, Entity, DataObject, Filter, Where, Count, Callback, AnyObject } from '@loopback/repository';
import { AkeraConnector, ConnectionOptions } from './akera-connector';
import { DiscoverModelDefinitionsOptions, ModelDefinition, ModelPropertyDefinition, ModelKeyDefinition } from './akera-discovery';

export interface Datasource {
  settings?: ConnectionOptions
}

export class NotFoundError extends Error {
  public statusCode = 404;
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

  public get name(): string {
    return this.connector.name;
  }

  public initialize (datasource: Datasource, callback?: Callback<void>): void {
    if (!!datasource && !this.connector)
      this.connector = new AkeraConnector(datasource.settings);

    datasource['connector'] = this;
    this.connect(() => {
      callback && callback(null);
    });
  }

  ping(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  execute?(...args: any[]): Promise<AnyObject> {
    throw new Error("Method not implemented.");
  }

  // connects to an Akera Application Server
  public connect(callback?: Callback<void>): void {
    this.connector.connect().then(() => {
      callback && callback(null);
    }).catch(err => {
      callback && callback(err);
    });
  }

  // closes the active connection
  public disconnect(callback?: Callback<void>): void {
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
    this.connector.find(this.connector.getModel(modelName), filter, options).then((rows) => {
      callback && callback(null, rows);
    }).catch((err: Error) => {
      delete err.stack;
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
  public count(modelName: string, where: Where, options: AnyObject, callback: Callback<number>) {
    this.connector.count(this.connector.getModel(modelName), where, options).then((count) => {
      callback && callback(null, count && count.count || 0);
    }).catch((err: Error) => {
      delete err.stack;
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
  public create(modelName: string, data: DataObject<Entity>, options: AnyObject, callback: Callback<DataObject<Entity>>) {
    this.connector.create(this.connector.getModel(modelName), data, options)
      .then((response) => {
        callback && callback(null, response);
      })
      .catch((err: Error) => {
        delete err.stack;
        callback && callback(err);
      });
  }

  /**
  * Update model used by PUT verb
  * 
  * @param {String}
  *         modelName The model name
  * @param {String}
  *         id The model id
  * @param {Object}
  *         data Data to be updated
  * @param {Object}
  *         options The options object
  * @param {Function}
  *         callback The callback function
  */
  public replaceById(modelName: string, id: string, data: DataObject<Entity>, options: AnyObject, callback: Callback<void>) {
    this.connector.replaceById(this.connector.getModel(modelName), id, data, options)
      .then((response) => {
        if (response)
          callback && callback(null);
        else
          callback && callback(new NotFoundError(`Entity "${modelName}" with id "${id}" does not exist.`));
      })
      .catch((err) => {
        callback && callback(err);
      });
  }

  /**
   * Update model used by PATCH verb
   * 
   * @param {String}
   *         modelName The model name
   * @param {String}
   *         id The model id
   * @param {Object}
   *         data Data to be updated
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  public update(modelName: string, where: Where<Entity>, data: DataObject<Entity>, options: AnyObject, callback: Callback<Count>) {
    this.connector.updateAll(this.connector.getModel(modelName), data, where, options)
      .then((response) => {
        callback && callback(null, response);
      })
      .catch((err) => {
        callback && callback(err);
      });
  }

  /**
   * Delete model used by DELETE verb
   * 
   * @param {String}
   *         modelName The model name
   * @param {String}
   *         id The model id
   * @param {Object}
   *         data Data to be updated
   * @param {Object}
   *         options The options object
   * @param {Function}
   *         callback The callback function
   */
  public destroyAll(modelName: string, where: Where<Entity>, options: AnyObject, callback: Callback<Count>) {
    this.connector.deleteAll(this.connector.getModel(modelName), where, options)
      .then((response) => {
        callback && callback(null, response);
      })
      .catch((err) => {
        callback && callback(err);
      });
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

