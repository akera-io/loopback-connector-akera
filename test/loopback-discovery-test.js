var should = require('./ini.js');

var ds;

describe('Akera connector discovery', function() {
  before(function() {
    ds = getDataSource();
  });

  after(function(callback) {
    ds.disconnect(callback, callback);
  });

  it('should return default database', function() {
    should(ds.connector.getDefaultSchema()).be.undefined;
  });

  it('should return schemas', function(done) {
    this.timeout(5000);

    ds.connector.discoverDatabaseSchemas(function(err, schema) {
      if (err !== null)
        done(err);
      else {
        should(schema.length).be.above(0);
        should(schema[0]).have.properties('schema', 'catalog');
        done();
      }
    });
  });

  it('should return models', function(done) {
    this.timeout(5000);

    ds.connector.discoverModelDefinitions(function(err, models) {
      if (err !== null)
        done(err);
      else {
        should(models.length).be.above(0);
        should(models[0]).have.properties('name', 'type', 'owner');
        done();
      }
    });
  });

  it('should return model for Customer table', function(done) {
    this.timeout(5000);

    ds.connector.discoverModelProperties('Customer', function(err, columns) {
      if (err !== null)
        done(err);
      else {
        should(columns.length).be.above(0);
        should(columns[0]).have.properties('tableName', 'columnName',
            'dataType', 'type', 'owner');
        should(columns[0]).have.property('tableName', 'Customer');

        done();
      }
    });
  });

  it('should return PK for Customer table', function(done) {
    this.timeout(5000);

    ds.connector.discoverPrimaryKeys('Customer', function(err, pks) {
      if (err !== null)
        done(err);
      else {
        should(pks.length).be.above(0);
        should(pks[0]).have.properties('tableName', 'columnName', 'keySeq',
            'pkName', 'owner');
        should(pks[0]).have.property('tableName', 'Customer');
        should(pks[0]).have.property('keySeq', 1);
        done();
      }
    });
  });

  it('should discover Customer model', function(done) {
    this.timeout(5000);

    try {
      ds.discoverAndBuildModels('Customer', {}, function(err, models) {
        if (err !== null)
          done(err);
        else {
          should(models).have.property('Customer');
          should(models.Customer).have
              .properties('find', 'count', 'definition');
          done();
        }
      });
    } catch (err) {
      done(err);
    }
  });
});