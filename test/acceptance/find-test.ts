import { DataSource } from "loopback-datasource-juggler";
import { InitTests } from "./init";
import * as should from "should";

let ds: DataSource;


describe('Test find method', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
    });

    after('after test actions', async () => {
        await ds.disconnect();
    })


    it('test if we are connected', () => {
        let conn = ds.connect();

        if (conn instanceof Promise)
            return conn.then(() => {
                should(ds.connected).be.true('Data source should be connected');
            });
        
    });

    it('test find no filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({}, {});

        should(data).be.instanceOf(Array, 'Find should return an array');
        should(data.length).be.greaterThan(0, 'Result array should not be empty');
        
        data.forEach((row) => {
            should(row).have.properties('region', 'state', 'statename');
        })


    })

    it('test find - paging', async () => {
        const State: any = ds.getModel('State');

        let two = await State.find({ limit: 2 }, {});
        let one = await State.find({ limit: 1, offset: 2 }, {});

        should(two.length).be.equal(2, 'Size should be 2 when limit is 2');
        should(one.length).be.equal(1, 'Size should be 1 when limit is 1');

        should(one[0].state).be.equal(two[1].state, 'Entry in second select should match the second one in first select');
    })

    it('test find - like filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { like: 'we*' } } }, {});

        data.forEach((row) => {
            should(row.statename.toLowerCase()).startWith('we');
        })
    })

});