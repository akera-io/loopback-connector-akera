import { DataSource } from "loopback-datasource-juggler";
import { InitTests } from "./init";
import * as should from "should";

let ds: DataSource;


describe('Test count method', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
    });

    after('after test actions', async () => {
        if (ds && ds.connected)
            ds.disconnect();
    })

    it('test count no filter', async () => {
        const State: any = ds.getModel('State');

        let count = await State.count({});

        should(count).be.Number();
        should(count).be.greaterThan(0, 'Count should be greater than zero');

    })

    it('test count filter', async () => {
        const State: any = ds.getModel('State');

        let countAll = await State.count({});
        let state = (await State.find({limit: 1}))[0];
        let count = await State.count({state: {neq: state.state}});

        should(count).be.equal(countAll - 1, 'Count when filter out one state should be one less than all');

    })

    it('test count filter (false)', async () => {
        const State: any = ds.getModel('State');

        let count = await State.count({and: [{state: 'a'} , {state: {neq: 'a'}}]});

        should(count).be.equal(0, 'Count when filter false should be zero');

    })

});