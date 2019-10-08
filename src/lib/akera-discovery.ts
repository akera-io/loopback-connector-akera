
import { AkeraConnector } from './akera-connector';
import { IConnectionMeta } from '@akeraio/api';

export interface DiscoverDatabaseSchemasOptions {
    offset?: number,
    skip?: number,
    limit?: number
}

export interface DiscoverModelDefinitionsOptions {
    schema?: string,
    owner?: string
}


/**
 * The signature of loopback connector is different from that
 * on @loopback/repository, we implement the later and use a proxy.
 */
export class AkeraDiscovery {

    private meta: IConnectionMeta;

    constructor(
        private connector: AkeraConnector
    ) {
        this.meta = connector.getMetaData();
    }

    public discoverDatabaseSchemas(options: DiscoverDatabaseSchemasOptions) {

    }

    public discoverModelDefinitions (options: DiscoverModelDefinitionsOptions) {

    }

}

