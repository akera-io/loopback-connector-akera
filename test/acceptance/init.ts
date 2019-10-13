import { DataSource } from 'loopback-datasource-juggler';

export class InitTests {

    static getDataSource(): DataSource {
        const config = {
            host: '192.168.10.18',
            port: 8900,
            debug: true
        };

        config['connector'] = require('../..');

        const ds = new DataSource(
            config
        );

        ds.createModel('State',
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
            });

        return ds;
    }
}