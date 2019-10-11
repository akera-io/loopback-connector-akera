import { DataSource } from 'loopback-datasource-juggler';
import { AkeraConnectorProxy } from '../../dist/lib/akera';

export class InitTests {

    static getDataSource(): DataSource {
        var ds = new DataSource(
            new AkeraConnectorProxy({
                host: '192.168.10.18',
                port: 8900,
                debug: true
            })
        );
        return ds;
    }
}