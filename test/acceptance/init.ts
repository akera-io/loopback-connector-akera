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

        ds.createModel('Warehouse',
            {
                address: {
                    type: String,
                    required: false
                },
                address2: {
                    type: String,
                    required: false
                },
                city: {
                    type: String,
                    required: false
                },
                country: {
                    type: String,
                    required: false
                },
                phone: {
                    type: String,
                    required: false
                },
                postalcode: {
                    type: String,
                    required: false
                },
                state: {
                    type: String,
                    required: false
                },
                warehousename: {
                    type: String,
                    required: false
                },
                warehousenum: {
                    type: Number,
                    id: true,
                    required: false
                }
            });

        return ds;
    }
}