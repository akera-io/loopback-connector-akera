import { connect, IConnection, ISetField, IQuerySelect, SelectionMode, Filter as AkeraFilter, QueryFilter, QuerySelect, QueryDelete, QueryUpdate, Record, IConnectionMeta } from '@akeraio/api';
import { debug, Debugger } from 'debug';
import { ConnectInfo } from '@akeraio/net';
import { ModelDefinition, CrudConnector, Command, Class, Entity, DataObject, Filter, Where, Count, Model, Callback, AndClause, OrClause, PredicateComparison, ShortHandEqualType, AnyObject } from '@loopback/repository';
import { AkeraDiscovery } from './akera-discovery';

export declare type ComparableType = string | number | Date;

export interface ConnectionOptions extends ConnectInfo {
  debug?: boolean
}

export interface SchemaModel {
  [name: string]: Class<Entity>
}

export class AkeraConnector implements CrudConnector {

  public name: 'akera.io';
  public configModel?: Model;
  public interfaces?: string[];

  private connection: IConnection;
  private discovery: AkeraDiscovery;
  private models: SchemaModel = {};
  private debugger: Debugger;

  constructor(
    private config: ConnectionOptions
  ) {
    this.connection = null;
    this.debugger = debug('loopback:connector:akera');
    this.debugger.enabled = this.debugger.enabled || config.debug;
  }

  public debuglog(...args: any[]) {
    this.debugger && this.debugger(args);
  }

  public getMetaData (): IConnectionMeta {
      return this.connection.meta;
  }

  public getDiscovery () {
    if (!this.discovery)
        this.discovery = new AkeraDiscovery(this);

    return this.discovery;
  }

  public define(model: Class<Entity>) {
    if (model && model.name)
      this.models[model.name] = model;
  }

  public getModel(modelName: string) {
    return this.models[modelName];
  }

  // connects to an Akera Application Server
  public connect(): Promise<void> {
    if (this.connection !== null && !this.connection.closed) {
      return Promise.resolve();
    } else {
      return connect(this.config)
        .then(rsp => {
          this.connection = rsp;
          this.connection.autoReconnect = true;
          this.debuglog('Connection established: %s.', this.config.host + ':' + this.config.port);
        })
        .catch(err => {
          this.debuglog('Connection error: %j', err);
          throw err;
        });
    }
  }

  // closes the active connection
  public disconnect(): Promise<void> {
    if (this.connection === null || this.connection.closed) {
      this.connection = null;
      return Promise.resolve();
    } else {
      return this.connection.disconnect().then(() => {
        this.debuglog('Connection closed: %s.', this.config.host + ':' + this.config.port);
      }).catch((err) => {
        this.debuglog('Connection close error: %s %j.', this.config.host + ':' + this.config.port, err);
        throw err;
      }).finally(() => {
        this.connection = null;
      })
    }
  }

  async create(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<DataObject<Entity>> {
    await this.connect();
    const fields: ISetField[] = [];

    for (let p in entity) {
      fields.push({ name: p, value: entity[p] });
    }

    return this.connection.create({ table: modelClass.modelName, fields: fields });
  }

  async createAll?(modelClass: Class<Entity>, entities: DataObject<Entity>[], options?: AnyObject): Promise<DataObject<Entity>[]> {
    return Promise.all(entities.map((entity) => {
      return this.create(modelClass, entity, options);
    }));
  }
  
  save?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<DataObject<Entity>> {
    throw new Error("Method not implemented.");
  }

  async find(modelClass: Class<Entity>, filter?: Filter<AnyObject>, options?: AnyObject): Promise<DataObject<Entity>[]> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();

    const qry: IQuerySelect = { tables: [{ name: model.name, select: SelectionMode.EACH }] };

    // transform loopback filter in akera.io format
    this.applyFilter(qry, model, filter);

    return this.connection.select(qry);
  }

  async findById?<IdType>(modelClass: Class<Entity>, id: IdType, options?: AnyObject): Promise<DataObject<Entity>> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();
    const where = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);

    const qry: QuerySelect = this.connection.query.select(model.name, this.getWhereClause(model, where));

    return qry.all().then((rows) => {
      if (rows.length === 1)
        return rows[0];

      throw new Error(`No ${model.name} record found with id: ${JSON.stringify(id)}.`);
    });

  }

  update?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
    throw new Error("Method not implemented.");
  }

  delete?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
    throw new Error("Method not implemented.");
  }

  async updateAll(modelClass: Class<Entity>, data: DataObject<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();
    const qry: QueryUpdate = this.connection.query.update(model.name, data as Record, this.getWhereClause(model, where));

    return qry.go().then((count) => {
      return { count: count };
    });
  }

  async updateById?<IdType>(modelClass: Class<Entity>, id: IdType, data: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();
    const filter = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);
    const qry: QueryUpdate = this.connection.query.update(model.name, data as Record, this.getWhereClause(model, filter));

    return qry.go().then((count) => {
      return count > 0;
    });
  }

  replaceById?<IdType>(modelClass: Class<Entity>, id: IdType, data: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
    return this.updateById(modelClass, id, data, options);
  }

  async deleteAll(modelClass: Class<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();

    const qry: QueryDelete = this.connection.query.delete(model.name, this.getWhereClause(model, where));

    return qry.go().then((count) => {
      return { count: count };
    });
  }

  async deleteById?<IdType>(modelClass: Class<Entity>, id: IdType, options?: AnyObject): Promise<boolean> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();
    const filter = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);
    const qry: QueryDelete = this.connection.query.delete(model.name, this.getWhereClause(model, filter));

    return qry.go().then((count) => {
      return count > 0;
    });
  }

  async count(modelClass: Class<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
    const model: ModelDefinition = modelClass.definition;

    // make sure we're connected or throw
    await this.connect();

    const qry: QuerySelect = this.connection.query.select(model.name, this.getWhereClause(model, where));

    return qry.count().then((count) => {
      return { count: count };
    });
  }

  exists?<IdType>(modelClass: Class<Entity>, id: IdType, options?: AnyObject): Promise<boolean> {
    const model: ModelDefinition = modelClass.definition;

    if (Array.isArray(id)) {
      return this.count(modelClass, this.getFilterByIds(model, id)).then((count) => {
        return count.count > 0;
      });
    } else {
      return this.count(modelClass, this.getFilterById(model, id)).then((count) => {
        return count.count > 0;
      });
    }
  }

  ping(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  execute?(command: Command, parameters: any[] | AnyObject, options?: AnyObject): Promise<AnyObject> {
    throw new Error("Method not implemented.");
  }

  private getModelIds(model: ModelDefinition): string[] {
    if (model.idProperties().length === 0)
      throw new Error(`The model ${model.name} does not have any primary key defined.`);

    return model.idProperties();
  }

  private getFilterById?<IdType>(model: ModelDefinition, id: IdType): Where<Entity> {
    const keys = this.getModelIds(model);

    if (keys.length === 1)
      return { [keys[0]]: id };

    throw new Error(`Not all values for the primary key of model ${model.name} provided: ${keys.slice(1).join(",")}.`);
  }

  private getFilterByIds?<IdType>(model: ModelDefinition, id: IdType[]): Where<Entity> {
    const keys = this.getModelIds(model);

    if (keys.length > id.length)
      throw new Error(`Not all values for the primary key of model ${model.name} provided: ${keys.slice(id.length).join(",")}.`);

    return {
      and: keys.map((k, i) => {
        return { [k]: id[i] };
      })
    };
  }

  private applyFilter(qry: IQuerySelect, model: ModelDefinition, filter?: Filter<AnyObject>) {
    if (qry.tables.length === 0)
      return;

    if (filter) {
      if (filter.limit > 0)
        qry.limit = filter.limit;
      if (filter.offset || filter.skip)
        qry.offset = filter.offset || filter.skip;

      if (filter.fields) {
        qry.tables[0].fields = [];

        if (Array.isArray(filter.fields)) {
          for (let fld of filter.fields) {
            if (!model.properties[fld])
              throw new Error(`Invalid field selection, the field ${fld} is not part of the model.`);
            qry.tables[0].fields.push(fld);
          }
        } else {
          for (let fld in filter.fields) {

            if (filter.fields[fld] === true) {
              if (!model.properties[fld])
                throw new Error(`Invalid field selection, the field ${fld} is not part of the model.`);
              qry.tables[0].fields.push(fld);
            }
          }
        }
      }

      if (filter.order) {
        qry.sort = [];

        for (let sort of filter.order) {
          const info = sort.trim().split(' ');

          if (!model.properties[info[0]])
            throw new Error(`Invalid sort option, the field ${info[0]} is not part of the model.`);

          let sortFld = {};
          sortFld[info[0]] = info[info.length - 1].toUpperCase() === 'DESC';
          qry.sort.push(sortFld)

        }
      }

      if (filter.where)
        qry.tables[0].filter = this.getWhereClause(model, filter.where);
    }

    // select all fields if not specified
    if (!qry.tables[0].fields || qry.tables[0].fields.length === 0)
      qry.tables[0].fields = Object.keys(model.properties);

  }

  private getWhereClause(model: ModelDefinition, where: Where<AnyObject>): AkeraFilter {
    if (Object.keys(where).length > 1)
      throw new Error('Invalid where filter, only single condition or and/or group allowed.');

    if (where['and'])
      return this.getWhereClauseAnd(model, where as AndClause<AnyObject>);

    if (where['or'])
      return this.getWhereClauseOr(model, where as OrClause<AnyObject>);

    for (let key in where) {
      if (!model.properties[key])
        throw new Error(`Invalid where filter, the field ${key} is not part of the model.`);

      if (typeof where[key] === 'object')
        return this.getWhereClausePredicate(key, where[key]);

      return QueryFilter.eq(key, where[key]);
    }

  }

  private getWhereClausePredicate(fieldName: string, condition: PredicateComparison<AnyObject>): AkeraFilter {
    if (!!condition.eq)
      return QueryFilter.eq(fieldName, condition.eq as ShortHandEqualType);

    if (!!condition.neq)
      return QueryFilter.ne(fieldName, condition.neq as ShortHandEqualType);

    if (!!condition.gt)
      return QueryFilter.gt(fieldName, condition.gt as ComparableType);

    if (!!condition.gte)
      return QueryFilter.ge(fieldName, condition.gte as ComparableType);

    if (!!condition.lt)
      return QueryFilter.lt(fieldName, condition.lt as ComparableType);

    if (!!condition.lte)
      return QueryFilter.le(fieldName, condition.lte as ComparableType);

    if (!!condition.like)
      return QueryFilter.like(fieldName, condition.like.toString());

    if (!!condition.nlike)
      return QueryFilter.not(QueryFilter.like(fieldName, condition.nlike.toString()));

    if (!!condition.between)
      return QueryFilter.and(QueryFilter.ge(fieldName, condition.between[0] as ComparableType),
        QueryFilter.le(fieldName, condition.between[1] as ComparableType));

    if (!!condition.inq)
      return QueryFilter.or(condition.inq.map((c) => {
        return QueryFilter.eq(fieldName, c as ShortHandEqualType);
      }));

    if (!!condition.nin)
      return QueryFilter.and(condition.nin.map((c) => {
        return QueryFilter.ne(fieldName, c as ShortHandEqualType);
      }));

    throw new Error(`Filter condition not supported: ${JSON.stringify(condition)}.`);
  }

  private getWhereClauseAnd(model: ModelDefinition, where: AndClause<AnyObject>): AkeraFilter {
    return QueryFilter.and(where.and.map((condition) => {
      return this.getWhereClause(model, condition);
    }));
  }

  private getWhereClauseOr(model: ModelDefinition, where: OrClause<AnyObject>): AkeraFilter {
    return QueryFilter.or(where.or.map((condition) => {
      return this.getWhereClause(model, condition);
    }));
  }
}



