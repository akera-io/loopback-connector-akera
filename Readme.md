 ##loopback-connector-akera
 `loopback-connector-akera` is the Akera Application Server for [loopback-datasource-juggler]
(https://github.com/strongloop/loopback-datasource-juggler/).

##Installation
````sh
npm install loopback-connector-akera --save
````

##Basic use

To use this connector you need `loopback-datasource-juggler`.

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
2.Quick start:

```javascript
	var datasource = require('loopback-datasource-juggler').DataSource;
	var config = {
		host: '10.10.10.6',
		port: 38900,
		useSSL:false,
		database:'sports2000'
	};
	var ds = new datasource('akera', config);
```

3.Tests:
To run the test suite set-up a working 'sports2000' Progress database on a Akera Application Server
then run `npm test`.
	
