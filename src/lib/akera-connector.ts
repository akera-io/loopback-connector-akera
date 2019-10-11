import { connect, IConnection, ISetField, IQuerySelect, SelectionMode, Filter as AkeraFilter, QueryFilter, QuerySelect, QueryDelete, QueryUpdate, Record, IConnectionMeta } from '@akeraio/api';
import { debug, Debugger } from 'debug';
import { ConnectInfo } from '@akeraio/net';
import { ModelDefinition, CrudConnector, Command, Class, Entity, DataObject, Filter, Where, Count, Model, Callback, AndClause, OrClause, PredicateComparison, ShortHandEqualType, AnyObject } from '@loopback/repository';
import { AkeraDiscovery } from './akera-discovery';
import { EventEmitter } from 'events';
import { ConnectionState } from '@akeraio/api/dist/lib/Connection';

export declare type ComparableType = string | number | Date;

export interface ConnectionOptions extends ConnectInfo {
    database?: string,
    debug?: boolean,
    connectPoolSize?: number,
    connectTimeout?: number
}

export interface SchemaModel {
    [name: string]: Class<Entity>
}

export class AkeraConnector implements CrudConnector {

    public configModel?: Model;
    public interfaces?: string[];

    private _available: IConnection[];
    private _busy: IConnection[];
    private _connAvailableEvt: EventEmitter;
    private discovery: AkeraDiscovery;
    private models: SchemaModel = {};
    private debugger: Debugger;

    constructor(
        private config: ConnectionOptions
    ) {


        this.debugger = debug('loopback:connector:akera');
        this.debugger.enabled = this.debugger.enabled || config.debug;

        this._available = [];
        this._busy = [];

        this._connAvailableEvt = new EventEmitter();

        if (this.poolingEnabled) {
            this.debuglog(`Connection pooling enabled: ${this.config.connectPoolSize || 'unlimitted'}.`);
        }
    }

    public get name(): string {
        if (!this.config)
            return 'akera.io';

        return `akera.io (${this.config.host}:${this.config.port})`;
      }

    public get poolingEnabled(): boolean {
        return this.config.connectPoolSize === undefined || this.config.connectPoolSize > 0;
    }

    private get connection(): IConnection {
        if (this._available.length > 0) {
            const conn = this._available.pop();

            this._busy.push(conn);
            this.debuglog('Reuse one connection from the connection pool.');

            return conn;
        }

        this.debuglog('No connection available in the connection pool.');

        throw new Error('No connection available in the connection pool.');
    }

    private set connection(conn: IConnection) {
        conn.autoReconnect = true;

        conn.stateChange.on('state', (state) => {
            if (state === ConnectionState.API) {
                const idx = this._busy.indexOf(conn);

                if (idx !== -1) {
                    conn = this._busy.splice(idx, 1)[0];

                    if (this.poolingEnabled) {
                        if (this._available.length > 1 && this._available.length > this._busy.length * 2) {

                            this.debuglog(`Clossing connection from the pool because of low load: ${this._available.length}/${this._busy.length}.`);

                            conn.disconnect().catch(err => {
                                this.debuglog(`Connection close error: ${err.message}.`);
                            });

                        } else {
                            this.debuglog('Add the connection back into the connection pool.');

                            this._available.push(conn);
                            this._connAvailableEvt.emit('available');
                        }
                    } else {
                        // no pooling used, make the connection available again
                        this._available.push(conn);
                        this._connAvailableEvt.emit('available');
                    }
                }
            }
        });

        // when new connection is made it is used by a request
        this._busy.push(conn);
    }

    private getConnection(): Promise<IConnection> {
        try {
            // try to resolve using one already available
            return Promise.resolve(this.connection);
        } catch (err) {
            // no connection available, try to start a new one if pool size not exceeded
            if (this._busy.length === 0 || !this.config.connectPoolSize || this._available.length < this.config.connectPoolSize) {
                return connect(this.config)
                    .then(async (conn) => {
                        this.connection = conn;
                        this.debuglog(`Connection established: ${this.config.host}:${this.config.port}.`);

                        if (this.config.database)
                            await conn.selectDatabase(this.config.database);

                        return conn;
                    })
                    .catch(err => {
                        this.debuglog(`Connection error: ${err.message}.`);
                        throw err;
                    });
            } {
                this.debuglog('No connection available, waiting for one to finish.');

                return new Promise((resolve, reject) => {
                    let timedOut = false;
                    let timer: NodeJS.Timeout;

                    if (this.config.connectTimeout > 0) {
                        timer = setTimeout(() => {
                            timedOut = true;
                            this.debuglog(`Connection time out: ${this.config.connectTimeout}.`);
                            reject(new Error(`Connection time out: ${this.config.connectTimeout}.`));
                        }, this.config.connectTimeout);
                    }

                    this._connAvailableEvt.once('available', () => {
                        // one connection became available, try to use that one if not timed out already
                        if (!timedOut) {
                            clearTimeout(timer);
                            this.getConnection().then(resolve).catch(reject);
                        }
                    });
                });

            }

        }
    }

    public debuglog(...args: any[]) {
        this.debugger && this.debugger(args);
    }

    public async getMetaData(): Promise<IConnectionMeta> {
        const conn = await this.getConnection();

        return conn.meta;
    }

    public getDiscovery() {
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
        return this.getConnection().then((conn) => {

        });
    }

    // closes the (all) active connection(s)
    public disconnect(): Promise<void> {
        return Promise.all(this._available.map((conn) => {
            return conn.disconnect();
        })).then(() => {

        });
    }

    async create(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<DataObject<Entity>> {
        const conn = await this.getConnection();
        const fields: ISetField[] = [];

        for (let p in entity) {
            fields.push({ name: p, value: entity[p] });
        }

        return conn.create({ table: modelClass.modelName, fields: fields });
    }

    async createAll?(modelClass: Class<Entity>, entities: DataObject<Entity>[], options?: AnyObject): Promise<DataObject<Entity>[]> {
        return Promise.all(entities.map((entity) => {
            return this.create(modelClass, entity, options);
        }));
    }

    save?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<DataObject<Entity>> {
        const model: ModelDefinition = modelClass.definition;
        const modelId = this.getModelId(model, entity);

        return this.updateById(modelClass, modelId, entity, options).then(result => {
            return this.findById(modelClass, modelId);
        });
    }

    async find(modelClass: Class<Entity>, filter?: Filter<AnyObject>, options?: AnyObject): Promise<DataObject<Entity>[]> {
        const model: ModelDefinition = modelClass.definition;

        const conn = await this.getConnection();

        const qry: IQuerySelect = { tables: [{ name: model.name, select: SelectionMode.EACH }] };

        // transform loopback filter in akera.io format
        this.applyFilter(qry, model, filter);

        return conn.select(qry);
    }

    async findById?<IdType>(modelClass: Class<Entity>, id: IdType, options?: AnyObject): Promise<DataObject<Entity>> {
        const model: ModelDefinition = modelClass.definition;

        const conn = await this.getConnection();
        const where = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);

        const qry: QuerySelect = conn.query.select(model.name, this.getWhereClause(model, where));

        return qry.all().then((rows) => {
            if (rows.length === 1)
                return rows[0];

            throw new Error(`No ${model.name} record found with id: ${JSON.stringify(id)}.`);
        });

    }

    update?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
        const model: ModelDefinition = modelClass.definition;

        return this.updateById(modelClass, this.getModelId(model, entity), entity, options);
    }

    delete?(modelClass: Class<Entity>, entity: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
        const model: ModelDefinition = modelClass.definition;

        return this.deleteById(modelClass, this.getModelId(model, entity), options);
    }

    async updateAll(modelClass: Class<Entity>, data: DataObject<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
        const model: ModelDefinition = modelClass.definition;
        const conn = await this.getConnection();

        const qry: QueryUpdate = conn.query.update(model.name, data as Record, this.getWhereClause(model, where));

        return qry.go().then((count) => {
            return { count: count > 0 ? count : 0 };
        });
    }

    async updateById?<IdType>(modelClass: Class<Entity>, id: IdType, data: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
        const model: ModelDefinition = modelClass.definition;
        const conn = await this.getConnection();

        const filter = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);
        const qry: QueryUpdate = conn.query.update(model.name, data as Record, this.getWhereClause(model, filter));

        return qry.go().then((count) => {
            return count > 0;
        });
    }

    replaceById?<IdType>(modelClass: Class<Entity>, id: IdType, data: DataObject<Entity>, options?: AnyObject): Promise<boolean> {
        const model: ModelDefinition = modelClass.definition;

        return this.updateById(modelClass, id, this.resetMissingProperties(data, model, id), options);
    }

    async deleteAll(modelClass: Class<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
        const model: ModelDefinition = modelClass.definition;
        const conn = await this.getConnection();

        const qry: QueryDelete = conn.query.delete(model.name, this.getWhereClause(model, where));

        return qry.go().then((count) => {
            return { count: count > 0 ? count : 0 };
        });
    }

    async deleteById?<IdType>(modelClass: Class<Entity>, id: IdType, options?: AnyObject): Promise<boolean> {
        const model: ModelDefinition = modelClass.definition;
        const conn = await this.getConnection();

        const filter = Array.isArray(id) ? this.getFilterByIds(model, id) : this.getFilterById(model, id);
        const qry: QueryDelete = conn.query.delete(model.name, this.getWhereClause(model, filter));

        return qry.go().then((count) => {
            return count > 0;
        });
    }

    async count(modelClass: Class<Entity>, where?: Where<Entity>, options?: AnyObject): Promise<Count> {
        const model: ModelDefinition = modelClass.definition;
        const conn = await this.getConnection();

        const qry: QuerySelect = conn.query.select(model.name, this.getWhereClause(model, where));

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

    private resetMissingProperties<IdType>(data: DataObject<Entity>, model: ModelDefinition, id: IdType): DataObject<Entity> {
        for (let p in model.properties) {
            if (model.properties[p].id)
                data[p] = id;
            else {
                if (data[p] === undefined)
                    data[p] = null;
            }
        }

        return data;
    }

    private getModelIds(model: ModelDefinition): string[] {
        if (model['__pks__'])
            return model['__pks__'];

        const ids: { name: string, id: number }[] = [];
        let idx = 1;

        for (let p in model.properties) {
            let fldId = model.properties[p].id;
            if (fldId)
                ids.push({ name: p, id: typeof fldId === 'number' ? fldId : idx++ });
        }

        if (ids.length === 0)
            throw new Error(`The model ${model.name} does not have any primary key defined.`);

        model['__pks__'] = ids.sort((a, b) => {
            return a.id - b.id;
        }).map(item => {
            return item.name;
        });

        return model['__pks__'];
    }

    private getModelId<IdType>(model: ModelDefinition, entity: DataObject<Entity>): IdType {
        const keys = this.getModelIds(model);

        if (keys.length === 1)
            return entity[keys[0]];

        throw new Error(`The primary key for model ${model.name} is composed: ${keys.join(",")}.`);
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



