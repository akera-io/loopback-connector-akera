var should = require('./ini.js');

var Customer, ds;
describe('Akera connector', function() {
	before(function() {
		ds = getDataSource();
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
	});
	after(function(callback) {
		ds.disconnect(function(){
			callback();
		});
	});
	describe('Datasource count method', function() {
		it('should count the records based on a empty filter', function(done) {
			Customer.count({}, function(err, nr) {
				if (err) {
					done(err);
					return;
				}
				should(nr).be.type('number');
				done();
			});
		});
		it('should count the records based on a null filter', function(done) {
			Customer.count(null, function(err, nr) {
				if (err) {
					done(err);
					return;
				}
				should(nr).be.type('number');
				done();
			});
		});
		it('should count the records based on a simple where filter', function(done) {
			Customer.count({
				where: {
					CustNum: {
						lte: 40
					}
				}
			}, function(err, nr) {
				if (err) {
					done(err);
					return;
				}
				should(nr).be.type('number');
				done();
			});
		});
		it('should count the records based on a complex where filter', function(done) {
			Customer.count({
				where: {
					and: [{
						CustNum: {
							lte: 5
						}
					}, {
						Country: "USA"
					}]
				}
			}, function(err, nr) {
				if (err) {
					done(err);
					return;
				}
				should(nr).be.type('number');
				done();
			});
		});
	});
});