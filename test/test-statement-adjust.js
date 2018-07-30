var nodeunit = require('nodeunit');
var jinst = require('../lib/jinst');
var JDBC = require('../lib/jdbc');
var asyncjs = require('async');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/Altibase.jar',
                        './drivers/Altibase6_5.jar']);
}

var altidb = new JDBC({
  url: 'jdbc:Altibase://testserver:20999/mydb',
  user: 'sys',
  password: 'manager'
});

var testconn = null;

module.exports = {
  setUp: function (callback) {
    if (testconn === null && altidb._pool.length > 0) {
      altidb.reserve(function (err, conn) {
        testconn = conn;
        callback();
      });
    } else {
      callback();
    }
  },
  tearDown: function (callback) {
    if (testconn) {
      altidb.release(testconn, function () {
        callback();
      });
    } else {
      callback();
    }
  },
  testinitialize: function (test) {
    altidb.initialize(function (err) {
      test.expect(1);
      test.equal(err, null);
      test.done();
    });
  },
  testcreatetable: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        var create = "CREATE TABLE blahMax ";
        create += "(id INT, name VARCHAR(10), date DATE)";
        statement.executeUpdate(create, function (err) {
          test.expect(1);
          test.equal(null, err);
          test.done();
        });
      }
    });
  },
  testMultipleInserts: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        asyncjs.times(50, function (n, next) {
            var insert = "INSERT INTO blahMax VALUES " +
              "(" + n + ", 'Jason_" + n + "', SYSDATE)";

            statement.executeUpdate(insert, function (err, result) {
              next(err, result);
            });
          },
          function (err, results) {
            if (err)
              console.log(err);
            else {
              test.expect(3);
              test.equal(null, err);
              test.equal(50, results.length);
              test.ok(results);
              test.done();
            }
          });
      }
    });
  },
  testselect: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeQuery("SELECT * FROM blahMax", function (err, resultset) {
          test.expect(5);
          test.equal(null, err);
          test.ok(resultset);
          resultset.toObjArray(function (err, results) {
            test.equal(results.length, 50);
            test.equal(results[0].NAME, 'Jason_0');
            test.ok(results[0].DATE);
            test.done();
          });
        });
      }
    });
  },
  testselectWithMax10Rows: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.setMaxRows(10, function (err) {
          if (err) {
            console.log(err);
          } else {
            statement.executeQuery("SELECT * FROM blahMax", function (err, resultset) {
              test.expect(4);
              test.equal(null, err);
              test.ok(resultset);
              resultset.toObjArray(function (err, results) {
                test.equal(results.length, 10);
                test.equal(results[0].NAME, 'Jason_0');
                test.done();
              });
            });
          }
        })
      }
    });
  },
  testselectWithMax70Rows: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.setMaxRows(70, function (err) {
          if (err) {
            console.log(err);
          } else {
            statement.executeQuery("SELECT * FROM blahMax", function (err, resultset) {
              test.expect(4);
              test.equal(null, err);
              test.ok(resultset);
              resultset.toObjArray(function (err, results) {
                test.equal(results.length, 50);
                test.equal(results[0].NAME, 'Jason_0');
                test.done();
              });
            });
          }
        })
      }
    });
  },
  testdroptable: function (test) {
    testconn.conn.createStatement(function (err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("DROP TABLE blahMax", function (err) {
          test.expect(1);
          test.equal(null, err);
          test.done();
        });
      }
    });
  }
};
