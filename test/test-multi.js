var nodeunit = require('nodeunit');
var jinst = require('../lib/jinst');
var JDBC = require('../lib/jdbc');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/Altibase.jar',
                        './drivers/Altibase6_5.jar']);
}

var config = {
//drivername: 'Altibase.jdbc.driver.AltibaseDriver',
  url: 'jdbc:Altibase_7.1.6://mmj:20999/mydb?user=sys&password=manager'
};

var config6 = {
  drivername: 'Altibase6_5.jdbc.driver.AltibaseDriver',
  url: 'jdbc:Altibase_7.1.3://mmj:20651/mydb?user=sys&password=manager'
};

var altidb7 = new JDBC(config);
var altidb6 = new JDBC(config6);
var altidb7conn = null;
var altidb6conn = null;

exports.altidb7 = {
  setUp: function(callback) {
    if (altidb7conn === null && altidb7._pool.length > 0) {
      altidb7.reserve(function(err, conn) {
        altidb7conn = conn;
        callback();
      });
    } else {
      callback();
    }
  },
  tearDown: function(callback) {
    if (altidb7conn) {
      altidb7.release(altidb7conn, function(err) {
        callback();
      });
    } else {
      callback();
    }
  },
  testinitialize: function(test) {
    altidb7.initialize(function(err) {
      test.expect(1);
      test.equal(null, err);
      test.done();
    });
  },
  testcreatetable: function(test) {
    altidb7conn.conn.createStatement(function(err, statement) {
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
    altidb7conn.conn.createStatement(function(err, statement) {
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
    altidb7conn.conn.createStatement(function(err, statement) {
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
    altidb7conn.conn.createStatement(function(err, statement) {
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
  testselectbyexecute: function(test) {
    altidb7conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.execute("SELECT * FROM blah;", function(err, resultset) {
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
  testupdatebyexecute: function(test) {
    altidb7conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.execute("UPDATE blah SET id = 2 WHERE name = 'Jason';", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.ok(1, result);
          test.done();
        });
      }
    });
  },
  testdelete: function(test) {
    altidb7conn.conn.createStatement(function(err, statement) {
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
    altidb7conn.conn.createStatement(function(err, statement) {
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
  },
};

exports.altidb6 = {
  setUp: function(callback) {
    if (altidb6conn === null && altidb6._pool.length > 0) {
      altidb6.reserve(function(err, conn) {
        altidb6conn = conn;
        callback();
      });
    } else {
      callback();
    }
  },
  tearDown: function(callback) {
    if (altidb6conn) {
      altidb6.release(altidb6conn, function(err) {
        callback();
      });
    } else {
      callback();
    }
  },
  testinitialize: function(test) {
    altidb6.initialize(function(err) {
      test.expect(1);
      test.equal(null, err);
      test.done();
    });
  },
  testcreatetable: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("CREATE TABLE blah (id INT, name VARCHAR(10), date DATE)", function(err, result) {
          test.expect(1);
          test.equal(null, err);
          test.done();
        });
      }
    });
  },
  testinsert: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("INSERT INTO blah VALUES (1, 'Jason', SYSDATE)", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.ok(result && result == 1);
          test.done();
        });
      }
    });
  },
  testupdate: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("UPDATE blah SET id = 2 WHERE name = 'Jason'", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.ok(result && result == 1);
          test.done();
        });
      }
    });
  },
  testselect: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeQuery("SELECT * FROM blah", function(err, resultset) {
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
  testselectobject: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeQuery("SELECT * FROM blah", function(err, resultset) {
          test.expect(9);
          test.equal(null, err);
          test.ok(resultset);
          resultset.toObject(function(err, results) {
            test.equal(results.rows.length, 1);
            test.equal(results.rows[0].NAME, 'Jason');
            test.ok(results.rows[0].DATE);

            test.equal(results.labels.length, 3);
            test.equal(results.labels[0], 'ID');
            test.equal(results.labels[1], 'NAME');
            test.ok(results.labels[2], 'DATE');

            test.done();
          });
        });
      }
    });
  },
  testselectzero: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeQuery("SELECT * FROM blah WHERE id = 1000", function(err, resultset) {
          test.expect(7);
          test.equal(null, err);
          test.ok(resultset);
          resultset.toObject(function(err, results) {
            test.equal(results.rows.length, 0);
            test.equal(results.labels.length, 3);
            test.equal(results.labels[0], 'ID');
            test.equal(results.labels[1], 'NAME');
            test.ok(results.labels[2], 'DATE');
            test.done();
          });
        });
      }
    });
  },
  testdelete: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("DELETE FROM blah WHERE id = 2", function(err, result) {
          test.expect(2);
          test.equal(null, err);
          test.ok(result && result == 1);
          test.done();
        });
      }
    });
  },
  testdroptable: function(test) {
    altidb6conn.conn.createStatement(function(err, statement) {
      if (err) {
        console.log(err);
      } else {
        statement.executeUpdate("DROP TABLE blah", function(err, result) {
          test.expect(1);
          test.equal(null, err);
          test.done();
        });
      }
    });
  }
};
