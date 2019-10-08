
import { Class, Entity, DataObject, Filter, Where, Count, Callback, AnyObject } from '@loopback/repository';
import { AkeraConnector, ConnectionOptions } from './akera-connector';


/**
 * The signature of loopback connector is different from that
 * on @loopback/repository, we implement the later and use a proxy.
 */
export class AkeraConnectorProxy {
  private connector: AkeraConnector;

  constructor(
    config: ConnectionOptions
  ) {
    this.connector = new AkeraConnector(config);
  }

  // connects to an Akera Application Server
  public connect(callback?: Callback<undefined>): void {
    this.connector.connect().then(() => {
      callback && callback(null);
    }).catch(err => {
      callback && callback(err);
    });
  }

  // closes the active connection
  public disconnect(callback?: Callback<undefined>): void {
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
  public create(modelName: string, data: DataObject<Entity>[] | DataObject<Entity>, options: AnyObject, callback: Callback<DataObject<Entity>[] | DataObject<Entity>>){
    //test if data is an array of objects who must be created
    if ( Array.isArray(data) ){
      this.connector.createAll( this.connector.getModel(modelName), data, options)
        .then( (response) => {
          callback && callback(null, response);
        })
        .catch( (err) => {
          callback && callback(err);
        });
    }
    else {
      this.connector.create( this.connector.getModel(modelName), data, options )
      .then( (response) => {
        callback && callback(null, response);
      })
      .catch( (err) => {
        callback && callback(err);
      });
    }
  }

  

}

