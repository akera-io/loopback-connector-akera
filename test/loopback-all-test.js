var should = require('./ini.js');

var Customer, ds;
describe('Akera connector', function() {
	before(function() {
		ds = getDataSource();
		Order = ds.define('Order', {
			OrderNum: {
				type: String,
				id: true
			},
			CustNum: {
				type: String
			}
		});
		Customer = ds.define('Customer', {
			CustNum: {
				type: String,
				id: true,
				required: true
			},
			Name: {
				type: String
			},
			City: {
				type: String
			}
		});
		Employee = ds.define('Employee', {

		});
	});
	after(function(callback) {
		ds.disconnect(function(){
			callback();
		});
	});
	describe('Datasource all method', function() {
		it('should fetch data based on a empty filter where', function(done) {
			this.timeout(5000);
			Customer.all({
				limit: 10
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				should(data).be.an.instanceOf(Array);
				done();
			});
		});
		it('should fetch data based on a null filter (will fetch all)', function(done) {
			this.timeout(5000);
			Customer.all(null, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				should(data).be.an.instanceOf(Array);
				done();
			});
		});
		it('should fetch data based on a simple where filter', function(done) {
			Customer.all({
				where: {
					CustNum: 5
				}
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				should(data).be.an.instanceOf(Array);
				done();
			});
		});
		it('should fetch data based on a complex where filter', function(done) {
			Customer.all({
				where: {
					and: [{
						CustNum: {
							lte: 10
						}
					}, {
						Country: 'USA'
					}]
				}
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				should(data).be.an.instanceOf(Array);
				done();
			});
		});
		it('should fetch data based on a fields filter', function(done) {
			Customer.all({
				fields: {
					'CustNum': true,
					'Name': true
				},
				limit: 10
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				for (var key in data) {
					should(data[key]).have.property('CustNum');
					should(data[key]).have.property('Name');
				}
				done();
			});
		});
		it('should fetch data based on a exclude fields filter', function(done) {
			Customer.all({
				fields: {
					'CustNum': false
				},
				limit: 10
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				for (var key in data) {
					should(data[key]).have.property('Name');
					should(data[key]).have.property('City');
				}
				done();
			});
		});
		it('should fetch data based on a skip filter(offset)', function(done) {
			Customer.all({
				limit: 10,
				skip: 5
			}, function(err, data) {
				if (err) {
					done(err);
					return;
				}
				should(data).be.an.instanceOf(Array);
				done();
			});
		});
		it('should fetch data based on a order by filter descending (sort)', function(done) {
			Customer.all({
				order: 'CustNum DESC',
				limit: 10
			}, function(err, records) {
				if (err) {
					done(err);
					return;
				}
				should(records).be.an.instanceOf(Array);
				var lastId;
				for (var i = 0; i < records.length; i++) {
					if (lastId) {
						if (parseInt(lastId) < parseInt(records[i].CustNum)){
							done('Sorting failed, incorrect order');
							return;
						}
					}
					lastId = records[i].CustNum;
				}
				done();
			});
		});
		it('should fetch data based on a order by filter ascending (sort)', function(done) {
			Customer.all({
				order: 'CustNum ASC',
				limit: 10
			}, function(err, records) {
				if (err) {
					done(err);
					return;
				}
				should(records).be.an.instanceOf(Array);
				var lastId;
				for (var i = 0; i < records.length; i++) {
					if (lastId) {
						if (parseInt(lastId) > parseInt(records[i].CustNum)) {
							done('Sorting failed, incorrect order');
							return;
						}
					}
					lastId = records[i].CustNum;
				}
				done();
			});
		});
		it('should fetch data based on a between condition', function(done) {
			Customer.all({
				where: {
					CustNum: {
						between: [0, 5]
					}
				}
			}, function(err, records) {
				should(records).be.an.instanceOf(Array);
				if (records.length > 6) {
					done('Expected no more than 6 rows, got ' + records.length + ' rows');
					return;
				}
				done();
			});
		});
		it('should fetch data based on a like(contains) condition', function(done) {
			Customer.all({
				where: {
					Name: {
						like: 'Lift*'
					}
				}
			}, function(err, records) {
				should(records).be.an.instanceOf(Array);
				records[0].Name.should.startWith('Lift');
				done();
			});
		});

		it('should fetch data based on a neq (not equals) condition', function(done) {
			Customer.all({
				where: {
					CustNum: {
						neq: 3
					}
				},
				limit: 3
			}, function(err, records) {
				should(records).be.an.instanceOf(Array);
				for (var i in records) {
					if (records[i].CustNum === 3) {
						done('A record with CustNum 3 exists');
						return;
					}
				}
				done();
			});
		});

		it('should fetch data based on a inq(in an array of values) filter', function(done) {
			var inqArr = [1, 2, 3];
			Customer.all({
				where: {
					CustNum: {
						inq: inqArr
					}
				},
				limit: 10,
				order: 'CustNum ASC'
			}, function(err, records) {
				should(records).be.an.instanceOf(Array);
				for (var i in records) {
					if (records[i].CustNum !== inqArr[i]) {
						done('Incorrect records were fetched');
						return;
					}
				}
				done();
			});
		});


		it('should fetch data based on a nin(not in an array of values) filter', function(done) {
			var ninArr = [1, 2, 3];
			Customer.all({
				where: {
					CustNum: {
						nin: ninArr
					}
				},
				limit: 3,
				order: 'CustNum ASC'
			}, function(err, records) {
				should(records).be.an.instanceOf(Array);
				for (var i in records) {
					if (records[i].CustNum === ninArr[i]) {
						done('Incorrect records were fetched');
						return;
					}
				}
				done();
			});
		});

		it('should fetch data based on the model id', function(done) {
			Customer.findById(2, function(err, record) {
				if (err) {
					done(err);
					return;
				}
				should(record).be.an.instanceOf(Object);
				(record.CustNum).should.be.exactly('2');
				done();
			});
		});
	});
});