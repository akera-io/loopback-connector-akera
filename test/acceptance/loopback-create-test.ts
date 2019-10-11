import { DataSource } from "loopback-datasource-juggler";
import { InitTests } from "./init";
import { equal } from "assert";

var ds: DataSource;
var stateObj: {
    region: string,
    statename: string,
    state: string
}
var State: any;

describe('Test create records in State table', () => {
    before('before tests actions', () => {
        ds = InitTests.getDataSource();
        State = ds.createModel( 'State', 
        {
            region: {
                type: String
            },
            statename: {
                type: String
            },
            state: {
                type: String,
                id: true
            }
        })
        

    });

    after('after test actions', () => {
        ds.disconnect();
    })

    it('test if we are connected', () => {
        equal( ds.connected, true, 'connect should be true');
        State.create()

        
        
    })

});