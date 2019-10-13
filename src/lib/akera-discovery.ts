
import { AkeraConnector } from './akera-connector';
import { FieldDataType } from '@akeraio/api';
import { Table } from '@akeraio/api/dist/lib/meta/Table';
import { ModelProperties, Schema, Options } from 'loopback-datasource-juggler';

export interface DiscoverPagingOptions {
    offset?: number,
    skip?: number,
    limit?: number
}

export interface DiscoverModelDefinitionsOptions extends DiscoverPagingOptions {
    schema?: string,
    owner?: string,
    loadProperties?: boolean
}

export interface DatabaseSchema {
    schema: string,
    models?: Schema[]
}

export enum ModelPropertyType {
    Boolean,
    Buffer,
    Date,
    Number,
    String
}

export interface ModelKeyDefinition {
    owner: string,
    tableName: string,
    columnName: string,
    keySeq: number
}

/**
 * The signature of loopback connector is different from that
 * on @loopback/repository, we implement the later and use a proxy.
 */
export class AkeraDiscovery {

    private schemas: DatabaseSchema[];

    constructor(
        private connector: AkeraConnector
    ) {
    }

    /**
   * Discover databases definitions
   * 
   * @param {Object}
   *         options Options for discovery
   */
    public async discoverDatabaseSchemas(options?: Options): Promise<DatabaseSchema[]> {

        if (this.schemas)
            return Promise.resolve(this.schemas);

        const meta = await this.connector.getMetaData();
        const dbs = await meta.getDatabases();

        this.schemas = dbs.map((db, i) => {
            return { schema: db.lname };
        });

        return this.schemas;
    }

    /**
   * Discover model definitions
   * 
   * @param {Object}
   *         options Options for discovery
   */
    public async discoverModelDefinitions(options: DiscoverModelDefinitionsOptions): Promise<Schema[]> {
        const meta = await this.connector.getMetaData();
        const schema = await this.getSchema(options);

        if (schema.models)
            return schema.models;

        const db = await meta.getDatabase(schema.schema);
        const definitions: Schema[] = [];

        let tables = await db.getTables();
        for (let table of tables) {
            const properties = options.loadProperties ? await this.discoverModelProperties(table.name, options) : undefined;

            definitions.push(
                {
                    name: table.name,
                    properties: properties
                }
            );
        }

        schema.models = definitions;

        return definitions;
    }

    /**
   * Discover model properties from a table
   * 
   * @param {String}
   *         tableName The table name
   * @param {Object}
   *         options The options for discovery (schema/owner)
   * 
   */
    public async discoverModelProperties(tableName: string, options: DiscoverModelDefinitionsOptions): Promise<ModelProperties> {
        const schema = await this.getSchema(options);
        let model: Schema;

        if (schema.models) {
            const models = schema.models.filter((model) => {
                return model.name === tableName;
            });
            if (models.length > 0) {
                model = models[0];
                if (model.properties != undefined)
                    return model.properties;
            }
        }

        const table = await this.getTable(tableName, schema.schema);
        const fields = await table.getFields();
        const pk = await table.getPrimaryKey();
        let properties: ModelProperties = {};

        fields.forEach((field) => {
            properties[field.name.toLowerCase()] = {
                columnName: field.name,
                dataType: FieldDataType[field.type.toUpperCase()],
                dataLength: null,
                dataPrecision: field.type === FieldDataType.DECIMAL ? field.decimals : null,
                dataScale: null,
                generated: false,
                nullable: !field.mandatory,
                owner: table.database.lname,
                tableName: tableName,
                type: this.getPropertyType(field.type)
            };
        });

        if (pk) {
            if (pk.fields.length === 1)
                properties[pk.fields[0].field.toLowerCase()].id = true;
            else {
                pk.fields.forEach((fld, idx) => {
                    properties[fld.field.toLowerCase()].id = idx + 1;
                })
            }
        }

        if (!model) {
            schema.models = schema.models || [];
            schema.models.push({ name: tableName, properties: properties });
        } else {
            model.properties = properties;
        }

        return properties;

    }

    /**
     * Discover model primary keys from a table
     * 
     * @param {String}
     *         tableName The table name
     * @param {Object}
     *         options The options for discovery (schema/owner)
     * 
     */
    public async discoverPrimaryKeys(tableName: string, options: DiscoverModelDefinitionsOptions): Promise<ModelKeyDefinition[]> {
        const schema = await this.getSchema(options);
        const properties = await this.discoverModelProperties(tableName, options);
        const keys: ModelKeyDefinition[] = [];

        for (let key in properties) {
            const id = properties[key].id;

            if (id) {
                keys.push({
                    owner: schema.schema,
                    tableName: tableName,
                    columnName: key,
                    keySeq: typeof id === 'number' ? id : 1
                });
            }
        }
        return keys;
    }

    private async getTable(tableName: string, schema: string): Promise<Table> {
        if (!tableName && tableName.trim().length === 0)
            return Promise.reject('Table name is mandatory for model discovery.');

        const meta = await this.connector.getMetaData();
        const db = await meta.getDatabase(schema);

        return db.getTable(tableName);
    }

    private getPropertyType(fieldType: FieldDataType): string {
        switch (fieldType) {

            case FieldDataType.BLOB:
                return ModelPropertyType[ModelPropertyType.Buffer];
            case FieldDataType.LOGICAL:
                return ModelPropertyType[ModelPropertyType.Boolean];
            case FieldDataType.INTEGER:
            case FieldDataType.INT64:
            case FieldDataType.DECIMAL:
                return ModelPropertyType[ModelPropertyType.Number];
            case FieldDataType.DATE:
            case FieldDataType.DATETIME:
            case FieldDataType.DATETIMETZ:
                return ModelPropertyType[ModelPropertyType.Date];
            default:
                return ModelPropertyType[ModelPropertyType.String];
        }
    }

    private async getSchema(options: DiscoverModelDefinitionsOptions): Promise<DatabaseSchema> {
        const schema = options.schema || options.owner || '';

        if (!this.schemas)
            this.schemas = await this.discoverDatabaseSchemas();

        if (this.schemas.length === 0)
            throw new Error(`No schemas available for this connection.`);

        if (schema.length > 0) {
            if (this.schemas[schema])
                return this.schemas[schema];

            throw new Error(`Schema ${schema} not found.`);
        }

        for (let schema in this.schemas)
            return this.schemas[schema];
    }

}

