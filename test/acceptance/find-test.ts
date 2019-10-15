import { DataSource } from "loopback-datasource-juggler";
import { InitTests } from "./init";
import * as should from "should";

let ds: DataSource;
let initCount: number;
let midPosition: number;


describe('Test find method', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
        const State: any = ds.getModel('State');

        initCount = await State.count({});
        if ( initCount > 0)
            midPosition = Math.floor( initCount / 2 );
        else {
            //add 10 records to table for testing purposes
        }

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
        should(data.length).be.equal(initCount, 'Result array should have length equal to initial count');
        
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

    it('test find - eq filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { eq: 'Arkansas' } } }, {});

        should(data.length).be.equal(1, 'Size should be 1 for eq filter when statename is Arkansas');

        should(data[0].statename).be.equal('Arkansas', 'Result should have one field matching with eq property');

    })

    it('test find - neq filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { neq: 'Arkansas' } } }, {});

        should(data.length).be.greaterThan(1, 'Size should be greater than 1 for neq filter when statename is Arkansas');
        
        data.forEach((row) => {
            should(row.statename).not.be.equal('Arkansas', 'Result should not include state defined in neq');
        })
    })

    it('test find - gt filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { gt: 'Alabama' } } }, {});
        
        data.forEach((row) => {
            should(row.statename > 'Alabama').be.equal(true, 'Result should have filter property greater than Alabama');
        })
    })

    it('test find - gte filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { gte: 'Alabama' } } }, {});
        
        data.forEach((row) => {
            should(row.statename >= 'Alabama').be.equal(true, 'Result should have filter property greater or equal than Alabama');
        })

    })

    it('test find - lt filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { lt: 'Idaho' } } }, {});
        
        data.forEach((row) => {
            should(row.statename < 'Idaho').be.equal(true, 'Result should have filter property lower than Idaho');
        })
    })

    it('test find - lte filter', async () => {
        const State: any = ds.getModel('State');

        let data = await State.find({ where: { statename: { lte: 'Idaho' } } }, {});
        
        data.forEach((row) => {
            should(row.statename <= 'Idaho').be.equal(true, 'Result should have filter property lower or equal than Idaho');
        })

    })



});