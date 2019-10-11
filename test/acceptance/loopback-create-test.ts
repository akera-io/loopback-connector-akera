import { DataSource } from "loopback-datasource-juggler";
//import { InitTests } from "./init";
import { equal } from "assert";
import { AkeraConnectorProxy } from '../../dist/lib/akera';

var ds: DataSource;
var stateObj: {
    region: string,
    statename: string,
    state: string
}
var State: any;

describe('Test create records in State table', () => {
    before('before tests actions', async () => {
        //ds = InitTests.getDataSource();
        ds = new DataSource (
            new AkeraConnectorProxy({
                host: '192.168.10.18',
                port: 8900,
                debug: true
            }));
        
        
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

        // ds.connect((err, ret) => {
        //     console.log(' connect', err, ret);
        // })
    });

    after('after test actions', () => {
        ds.disconnect();
    })

    it('test if we are connected', () => {
        //equal( ds.connected, true, 'connect should be true');
        State.all((err, ret) => {
            console.log('all', err, ret);
        })

        
        
    })

});