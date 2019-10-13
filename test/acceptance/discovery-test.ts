import { DataSource, ModelDefinition } from "loopback-datasource-juggler";
import { InitTests } from "./init";
import * as should from "should";

let ds: DataSource;


describe('Test model discovery', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
    });

    after('after test actions', async () => {
        if (ds && ds.connected)
            ds.disconnect();
    })

    it('test load models (no properties)', async function () {
        this.timeout(30000);

        const discover = ds.discoverModelDefinitions();

        if (discover instanceof Promise) {
            const models: ModelDefinition[] = await discover;

            models.forEach((model) => {
                should(model).have.property('name').not.undefined;
                should(model).have.property('properties').undefined;
            })
        }
    })

    it('test load models (with properties)', async function () {
        this.timeout(30000);

        const discover = ds.discoverModelDefinitions({ loadProperties: true });

        if (discover instanceof Promise) {
            const models: ModelDefinition[] = await discover;

            models.forEach((model) => {
                should(model).have.property('name').not.undefined;
                should(model).have.property('properties').not.undefined;

                for (let k in model.properties) {
                    let prop = model.properties[k];

                    should(prop).have.properties('type', 'columnName', 'tableName', 'owner', 'nullable').not.undefined;
                }
            })
        }
    })

    it('test load model (invalid)', async function () {
        this.timeout(30000);

        const discover = ds.discoverModelProperties('Customere');

        if (discover instanceof Promise) {
            let err: Error;

            try {
                await discover;
            } catch (e) {
                err = e;
            }

            should(err).not.be.undefined();
        }
    })

    it('test load model', async function () {
        this.timeout(30000);

        const discover = ds.discoverModelProperties('Customer');

        if (discover instanceof Promise) {

            const properties = await discover;

            should(properties).not.be.undefined();
            for (let prop of properties) {

                should(prop).have.properties('columnName', 'dataType', 'nullable', 'tableName', 'owner', 'type');
                if (prop.columnName === 'CustNum')
                    should(prop).have.property('id', true);
            }
        }
    })

});