
import { AkeraConnector } from './akera-connector';
import { FieldDataType } from '@akeraio/api';
import { Field } from '@akeraio/api/dist/lib/meta/Field';
import { Table } from '@akeraio/api/dist/lib/meta/Table';

export interface DiscoverPagingOptions {
    offset?: number,
    skip?: number,
    limit?: number
}

export interface DiscoverModelDefinitionsOptions extends DiscoverPagingOptions {
    schema?: string,
    owner?: string
}

export interface DatabaseSchema {
    schema: string,
    catalog: string
}

export interface ModelDefinition {
    type: string,
    name: string,
    owner: string
}

export enum ModelPropertyType {
    Boolean,
    Buffer,
    Date,
    Number,
    String
}

export interface ModelPropertyDefinition {
    owner: string,
    tableName: string,
    columnName: string,
    dataType: FieldDataType,
    columnType: FieldDataType,
    dataLength?: number,
    dataPrecision?: number,
    dataScale?: number,
    nullable: boolean,
    type: string,
    generated: boolean
}

export interface ModelKeyDefinition {
    owner: string,
    tableName: string,
    columnName: string,
    keySeq: number,
    pkName: string
}

/**
 * The signature of loopback connector is different from that
 * on @loopback/repository, we implement the later and use a proxy.
 */
export class AkeraDiscovery {

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
    public async discoverDatabaseSchemas(options: DiscoverPagingOptions): Promise<DatabaseSchema[]> {
        const meta = await this.connector.getMetaData();

        const dbs = await meta.getDatabases();
        const start = options.offset || options.skip || 0;
        const end = options.limit ? start + options.limit : dbs.length;

        return dbs.slice(start, end).map((db, i) => {
            return { catalog: db.lname, schema: db.lname };
        });
    }

    /**
   * Discover model definitions
   * 
   * @param {Object}
   *         options Options for discovery
   */
    public async discoverModelDefinitions(options: DiscoverModelDefinitionsOptions): Promise<ModelDefinition[]> {
        const meta = await this.connector.getMetaData();

        const dbs = (await meta.getDatabases()).filter(db => {
            return !options.schema || options.schema === db.lname;
        });

        const models: ModelDefinition[] = [];

        for (let db of dbs) {
            let tables = await db.getTables();
            tables.forEach(table => {
                models.push({
                    type: 'table',
                    name: table.name,
                    owner: db.lname
                });
            });
        }

        return models;
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
    public async discoverModelProperties(tableName: string, options: DiscoverModelDefinitionsOptions): Promise<ModelPropertyDefinition[]> {
        const table = await this.getTable(tableName, options);
        const fields = await table.getFields();

        return fields.map((field) => {
            return {
                columnName: field.name,
                columnType: FieldDataType[field.type],
                dataType: FieldDataType[field.type],
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
        const table = await this.getTable(tableName, options);
        const pk = await table.getPrimaryKey();

        return pk.fields.map((field, idx) => {
            return {
                owner: table.database.lname,
                tableName: table.name,
                columnName: field.field,
                keySeq: idx + 1,
                pkName: pk.name
            };
        });
    }

    private async getTable(tableName: string, options: DiscoverModelDefinitionsOptions): Promise<Table> {
        if (!tableName && tableName.trim().length === 0)
            return Promise.reject('Table name is mandatory for model discovery.');

        const meta = await this.connector.getMetaData();

        const dbs = (await meta.getDatabases()).filter(db => {
            return !options.schema || options.schema === db.lname;
        });

        for (let db of dbs) {
            let tables = await db.getTables();

            for (let table of tables) {
                if (table.name === tableName) {
                    return Promise.resolve(table);
                }
            }
        }

        return Promise.reject(`Table not found: ${tableName}.`);
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

}

