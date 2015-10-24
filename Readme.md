##loopback-connector-akera
 `loopback-connector-akera` is the LoopBack connector for [Akera.io](http://www.akera.io) Application Server 
 for [loopback-datasource-juggler] (https://github.com/strongloop/loopback-datasource-juggler/).

##Installation
```sh
npm set registry http://repository.akera.io
npm install loopback-connector-akera --save
```

##Basic use

To use this connector you need `loopback-datasource-juggler` and [akera-api](http://repository.akera.io).

1.`package.json` must contain these dependencies:

```json
    {
      ...
      "dependencies": {
        "loopback-datasource-juggler": "latest",
        "loopback-connector-akera": "latest"
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
	var ds = new datasource('akera', config);
```

For information on configuring the connector in a LoopBack application, please refer to [LoopBack documentation](https://docs.strongloop.com/display/public/LB/Connecting+models+to+data+sources).

3. Tests:
To run the test suite set-up a working 'sports2000' Progress database on a Akera Application Server
then run `npm test`.
	
