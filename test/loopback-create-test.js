var should = require('./ini.js');

var Customer, ds;
describe('Akera connector', function() {
  before(function() {
    ds = getDataSource();
    Customer = ds.define('Customer', {
      idInjection : false,
      CustNum : {
        type : Number,
        id : true,
        required : true
      },
      Name : {
        type : String
      },
      City : {
        type : String
      }
    });
  });
  after(function(callback) {
    ds.disconnect(function() {
      callback();
    });
  });

  describe('Datasource CRUD methods', function() {
    it('should be able to insert a row in the database', function(done) {
      var toInsert = {
        CustNum : 134,
        Name : 'NodeApi',
        City : 'Cluj'
      };
      Customer.create(toInsert, function(err, iRow) {
        if (err)
          done(err);
        else {
          try {
            (iRow.CustNum).should.be.exactly(toInsert.CustNum);
            (iRow.Name).should.be.exactly(toInsert.Name);
            (iRow.City).should.be.exactly(toInsert.City);
            done();
          } catch (err) {
            done(err);
          }
        }
      });
    });

    it('should throw error when trying to insert if record exists (code 711)',
        function(done) {
          this.timeout(3000);
          Customer.create({
            CustNum : 134,
            Name : "NodeApi",
            City : "Cluj"
          }, function(err, rsp) {
            if (err) {
              (err.code).should.be.exactly(711);
              done();
            } else if (rsp) {
              done('Error: records were inserted');
            }
          });
        });
    var uData;
    it('should be able to update a row', function(done) {
      uData = {
        CustNum : 134,
        Name : 'UpdateTest',
        City : "Cluj"
      };
      Customer.updateAll({
        where : {
          CustNum : 134
        }
      }, uData, function(err, recordArr) {
        if (err)
          done(err);
        else {
          try {
            should(recordArr).be.an.instanceOf(Array);
            var r = recordArr[0];
            (r.CustNum).should.be.exactly(uData.CustNum);
            (r.Name).should.be.exactly(uData.Name);
            (r.City).should.be.exactly(uData.City);
            done();
          } catch (err) {
            done(err);
          }
        }
      });
    });

    it('should be able to update or insert a row (upsert)', function(done) {
      this.timeout(3000);
      uData = {
        CustNum : 134,
        Name : 'UpsertTest',
        City : "ClujU"
      };
      Customer.upsert(uData, function(err, record) {
        if (err)
          done(err);
        else {
          try {
            should(record).be.an.instanceOf(Object);

            (record.CustNum).should.be.exactly(uData.CustNum);
            done();
          } catch (err) {
            done(err);
          }
        }
      });
    });

    it('Delete should be able to delete a record ', function(done) {
      Customer.destroyAll({
        CustNum : 134
      }, function(err, delRows) {
        if (err)
          done(err);
        else {
          try {
            (delRows.count).should.be.exactly(1);
            done();
          } catch (err) {
            data(err);
          }
        }
      });
    });
  });
});