##loopback-connector-akera
 `@akeraio/loopback-connector` is the LoopBack connector for [Akera.io](http://www.akera.io) Application Server 
 for [loopback-datasource-juggler] (https://github.com/strongloop/loopback-datasource-juggler/).

##Installation
```sh
npm install @akeraio/loopback-connector --save
```

##Basic use

To use this connector you need `loopback-datasource-juggler` and [akera-api](http://www.akera.io).

1.`package.json` must contain these dependencies:

```json
    {
      ...
      "dependencies": {
        "loopback-datasource-juggler": "latest",
        "@akeraio/loopback-connector": "latest"
      },
      ...
    }
```

2. Quick start:

```javascript
	var datasource = require('loopback-datasource-juggler').DataSource;
	var config = {
		host: 'localhost',
		port: 8900,
		useSSL: false,
		database: 'sports2000',
		debug: true
	};
	var ds = new datasource('@akeraio/loopback-connector', config);
```

For information on configuring the connector in a LoopBack application, please refer to [LoopBack documentation](https://loopback.io/doc/en/lb4/DataSources.html).

For Loopback 4 the datasource configuration looks something like this:

```json
{
  "name": "akera",
  "connector": "@akeraio/loopback-connector",
  "host": "localhost",
  "port": 8900
}
```

3. Tests:
To run the test suite set-up a working 'sports2000' Progress database on a Akera Application Server
then run `npm test`.
	
