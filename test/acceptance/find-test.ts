import { DataSource, PersistedModel, Filter } from "loopback-datasource-juggler";
import { InitTests } from "../init";
import * as should from "should";

let ds: DataSource;
let initCount: number;
let midPosition: number;
let refObj: any;
let states: PersistedModel[];
let State: typeof PersistedModel;

function doFind(filter?: Filter): Promise<PersistedModel[]> {
    let find = State.find(filter);

    if (find instanceof Promise)
        return find;

    return Promise.resolve([]);
}

class OrderObj {
    region: string = '';
    startState: string = '';
    endState: string = '';
}

describe('Test find method', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
        State = ds.getModel('State') as typeof PersistedModel;
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

        states = await doFind();

        initCount = states.length;
        if (initCount > 0)
            midPosition = Math.floor(initCount / 2);
        else {
            //add 10 records to table for testing purposes
        }

        //load obj from middle of result for future tests
        refObj = states[midPosition];

        should(states).be.instanceOf(Array, 'Find should return an array');
        should(states.length).be.greaterThan(0, 'Result array should not be empty');
        should(states.length).be.equal(initCount, 'Result array should have length equal to initial count');

        states.forEach((row) => {
            should(row).have.properties('region', 'state', 'statename');
        })


    })

    it('test find - paging - limit, offset allias skip', async () => {
        let first = await doFind({ limit: midPosition });
        let second = await doFind({ limit: 1, offset: midPosition });
        let third = await doFind({ limit: 1, skip: midPosition });

        should(first.length).be.equal(midPosition, `Size should be ${midPosition} when limit is ${midPosition}`);
        should(second.length).be.equal(1, 'Size should be 1 when limit is 1 and offset is 1');
        should(third.length).be.equal(1, 'Size should be 1 when limit is 1 and skip is 1');

        should(second[0]['state']).be.equal(first[midPosition - 1]['state'], 'Entry in second select should match the last one in first select');
        should(second[0]['state']).be.equal(third[0]['state'], 'Objects for skip and offset should match');

    })

    it('test find - where - like filter', async () => {
        const likeFilter = refObj.state[0];

        let data = await doFind({ where: { state: { like: `${likeFilter}*` } } });

        data.forEach((row) => {
            should(row['state'].toLowerCase()).startWith(likeFilter.toLowerCase());
        })
    })

    it('test find - where - nlike filter', async () => {
        const nlikeFilter = refObj.state[0];

        let data = await doFind({ where: { state: { nlike: `${nlikeFilter}*` } } });

        data.forEach((row) => {
            should(row['state'].toLowerCase()).not.startWith(nlikeFilter.toLowerCase());
        })
    })

    it('test find - where - eq filter', async () => {
        const eqFilter = refObj.state;

        let data = await doFind({ where: { state: { eq: eqFilter } } });

        should(data.length).be.equal(1, 'Size should be 1 for eq filter when key is involved');

        should(data[0]['state']).be.equal(eqFilter, 'Result should have key field matching with eq property');

    })

    it('test find - where - neq filter', async () => {
        const neqFilter = refObj.state;

        let data = await doFind({ where: { state: { neq: neqFilter } } });

        should(data.length).be.equal(initCount - 1, 'Size should be total count minus 1 because of neq filter set on key field');

        data.forEach((row) => {
            should(row['state']).not.be.equal(neqFilter, 'Result should not include object with field defined in neq');
        })
    })

    it('test find - where - gt filter', async () => {
        const gtFilter = refObj.state;

        let data = await doFind({ where: { state: { gt: gtFilter } } });

        data.forEach((row) => {
            should(row['state'] > gtFilter).be.equal(true, `Result should have filter property greater than ${gtFilter}`);
        })
    })

    it('test find - where - gte filter', async () => {
        const gteFilter = refObj.state;

        let data = await doFind({ where: { state: { gte: gteFilter } } });

        data.forEach((row) => {
            should(row['state'] >= gteFilter).be.equal(true, `Result should have filter property greater or equal than ${gteFilter}`);
        })

    })

    it('test find - where - lt filter', async () => {
        const ltFilter = refObj.state;

        let data = await doFind({ where: { state: { lt: ltFilter } } });

        data.forEach((row) => {
            should(row['state'] < ltFilter).be.equal(true, `Result should have filter property lower than ${ltFilter}`);
        })
    })

    it('test find - where - lte filter', async () => {
        const lteFilter = refObj.state;

        let data = await doFind({ where: { state: { lte: lteFilter } } });

        data.forEach((row) => {
            should(row['state'] <= lteFilter).be.equal(true, `Result should have filter property lower or equal than ${lteFilter}`);
        })

    })

    it('test find - where - between filter', async () => {
        const betweenStop = refObj.state;
        const betweenStart = states[0]['state'];

        let data = await doFind({ where: { state: { between: [betweenStart, betweenStop] } } });

        should(data.length).be.equal(midPosition + 1, 'Result size length should be midPosition + 1');

        data.forEach((row) => {
            should(row['state'] <= betweenStop).be.equal(true, `Result should have filter property lower or equal than ${betweenStop}`);
            should(row['state'] >= betweenStart).be.equal(true, `Result should have filter property lower or equal than ${betweenStart}`);
        })

    })

    it('test find - where - inq filter', async () => {
        const inq1 = refObj.state;
        const inq2 = states[0]['state'];
        const inq3 = states[initCount - 1]['state'];

        let data = await doFind({ where: { state: { inq: [inq1, inq2, inq3] } } });

        should(data.length).be.equal(3, 'Result size length should be 3');

        data.forEach((row) => {
            should(row['state'] == inq1 || row['state'] == inq2 || row['state'] == inq3).be
                .equal(true, `Result should have filter property equal with ${inq1} or ${inq2} or ${inq3}`);
        })
    })

    it('test find - where - nin filter', async () => {
        const nin1 = refObj.state;
        const nin2 = states[0]['state'];
        const nin3 = states[initCount - 1]['state'];

        let data = await doFind({ where: { state: { nin: [nin1, nin2, nin3] } } });

        should(data.length).be.equal(initCount - 3, 'Result size length should be initial count minus 3');

        data.forEach((row) => {
            should(row['state'] != nin1 && row['state'] != nin2 && row['state'] != nin3).be
                .equal(true, `Result should have filter property equal with ${nin1} or ${nin2} or ${nin3}`);
        })
    })

    it('test find - where - composed filter for same property using and operator', async () => {
        const neq = refObj.state;
        const gt = states[0]['state'];
        const lt = states[initCount - 1]['state'];

        let data = await doFind(
            {
                where:
                {
                    and: [
                        { state: { gt: gt } },
                        { state: { lt: lt } },
                        { state: { neq: neq } }
                    ]
                }
            });
        
        should(data.length).be.equal(initCount - 3, 'Result size length should be initial count minus 3');
        
        data.forEach((row) => {
            should(row['state'] != neq && row['state'] != gt && row['state'] != lt).be
                .equal(true, `Result should have filter property equal with ${neq} or ${gt} or ${lt}`);
        })

    })

    it('test find - where - composed filter for different properties using and operator', async () => {
        const stateGt = states[0]['state'];
        const statenamelt = refObj.statename;

        let data = await doFind(
            {
                where:
                {
                    and: [
                        { state: { gt: stateGt } },
                        { statename: { lt: statenamelt } }
                    ]
                }
            }
        );
        
        should(data.length).be.equal(midPosition - 2, 'Result size length should be mid position count minus 2');
    })

    it('test find filter - fields', async () => {

        let data = await doFind({
                fields: {
                    state: false,
                    region: false
                }
            }
        );

        should(data.length).be.greaterThan(0, 'Result array should not be empty');

        data.forEach((row) => {
            should(row).have.property('statename');
            should(row['state'] == undefined &&  row['region'] == undefined ).be.equal(true, 'Record should not have property excluded from fields');
        })
    })

    it('test find filter - order ASC unique propery', async () => {

        let data = await doFind({
                order: ['statename ASC']
            }
        );

        should(data.length).be.greaterThan(0, 'Result array should not be empty');

        let minState = data[0];

        data.forEach((row) => {
            should(row['statename'] >= minState['statename'] ).be.equal(true, 'Records should be in ascendence order by selected field');
            minState = row;
        });
    })

    it('test find filter - order for multiple properties 1', async () => {

        let data = await doFind({
                order: ['region ASC', 'state DESC']
            }
        );

        should(data.length).be.greaterThan(0, 'Result array should not be empty');

        let currentState =  data[0]['state'];
        let refRegion, currentRegion = data[0]['region'];

        data.forEach((row) => {
            should( row['region'] >= currentRegion ).be.equal( true, 'Results should be sorted asc after first property');
            currentRegion = row['region'];

            if ( row['region'] != refRegion ) {
                refRegion = row['region'];
                currentState = row['state'];
            } else {
                should(row['state'] <= currentState ).be.equal( true, 'Results should be sorted desc after second property');
                currentState = row['state'];
            }
        })
    })

    it('test find filter - order for multiple properties 2', async () => {

        let data = await doFind({
                order: ['region DESC', 'state ASC']
            }
        );

        should(data.length).be.greaterThan(0, 'Result array should not be empty');

        let currentState =  data[0]['state'];
        let refRegion, currentRegion = data[0]['region'];

        data.forEach((row) => {
            should( row['region'] <= currentRegion ).be.equal( true, 'Results should be sorted asc after first property');
            currentRegion = row['region'];

            if ( row['region'] != refRegion ) {
                refRegion = row['region'];
                currentState = row['state'];
            } else {
                should(row['state'] >= currentState ).be.equal( true, 'Results should be sorted desc after second property');
                currentState = row['state'];
            }
        })
    })

    it('test find filter - multiple conditions', async () => {

        const statenameNeq = refObj.statename;

        let data = await doFind({
                order: ['region DESC'],
                where: { statename : { neq : statenameNeq}},
                limit: 1,
                skip: 1
            }
        );

        should(data.length).be.equal(1, 'Result array should have one record');
    })

});