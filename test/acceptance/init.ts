import { DataSource } from 'loopback-datasource-juggler';

export class InitTests {

    static getDataSource(): DataSource {
        var ds = new DataSource( 
            require('../../'), 
            {
                host: '192.168.10.18',
                port: 8900,
                useSSL: false,
                debug: true
            });
        return ds;
    }
}