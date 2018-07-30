var nodeunit = require('nodeunit');
var jinst = require('../lib/jinst');
var JDBC = require('../lib/jdbc');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/Altibase.jar',
                        './drivers/Altibase6_5.jar']);
}

var config = {
url: 'jdbc:Altibase://testserver:20999/mydb',
  properties: {
    user: 'sys',
    password: 'manager'
  }
};

var altidb = new JDBC(config);
var testconn = null;

module.exports = {
  setUp: function(callback) {
    if (testconn === null && altidb._pool.length > 0) {
      altidb.reserve(function(err, conn) {
        testconn = conn;
        callback();
      });
    } else {
      callback();
    }
  },
  tearDown: function(callback) {
    if (testconn) {
      altidb.release(testconn, function(err) {
        callback();
      });
    } else {
      callback();
    }
  },
  testinitialize: function(test) {
    altidb.initialize(function(err) {
      test.expect(1);
      test.equal(null, err);
      test.done();
    });
  },
  testcreatetable: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("CREATE TABLE blah (id INT, name VARCHAR(10), date DATE);", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.equal(0, result);
          test.done();
        });
      }
    });
  },
  testinsert: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("INSERT INTO blah VALUES (1, 'Jason', SYSDATE);", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.equal(1, result);
          test.done();
        });
      }
    });
  },
  testupdate: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("UPDATE blah SET id = 2 WHERE name = 'Jason';", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.ok(1, result);
          test.done();
        });
      }
    });
  },
  testselect: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeQuery("SELECT * FROM blah;", function(err, resultset) {
          test.expect(5);
          test.equal(null, err);
          test.ok(resultset);
          resultset.toObjArray(function(err, results) {
            test.equal(results.length, 1);
            test.equal(results[0].NAME, 'Jason');
            test.ok(results[0].DATE);
            test.done();
          });
        });
      }
    });
  },
  testdelete: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("DELETE FROM blah WHERE id = 2;", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.equal(1, result);
          test.done();
        });
      }
    });
  },
  testdroptable: function(test) {
    testconn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("DROP TABLE blah;", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.equal(0, result);
          test.done();
        });
      }
    });
  }
};
