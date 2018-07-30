var nodeunit = require('nodeunit');
var jinst = require('../lib/jinst');
var dm = require('../lib/drivermanager.js');
var java = jinst.getInstance();

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/Altibase.jar',
                        './drivers/Altibase6_5.jar']);
}

var config = {
  url: 'jdbc:Altibase://testserver:20999/mydb',
  user : 'sys',
  password: 'manager'
};

java.newInstance('Altibase.jdbc.driver.AltibaseDriver', function(err, driver) {});

module.exports = {
  testgetconnection: function(test) {
    dm.getConnection(config.url + '?user=' + config.user + '&password=' + config.password, function(err, conn) {
      test.expect(2);
      test.equal(null, err);
      test.ok(conn);
      test.done();
    });
  },
  testgetconnectionwithprops: function(test) {
    var Properties = java.import('java.util.Properties');
    var props = new Properties();

    props.putSync('user', config.user);
    props.putSync('password', config.password);

    dm.getConnection(config.url, props, function(err, conn) {
      test.expect(2);
      test.equal(null, err);
      test.ok(conn);
      test.done();
    });
  },
  testgetconnectionwithuserpass: function(test) {
    dm.getConnection(config.url, config.user, config.password, function(err, conn) {
      test.expect(2);
      test.equal(null, err);
      test.ok(conn);
      test.done();
    });
  },
  testsetlogintimeout: function(test) {
    dm.setLoginTimeout(60, function(err) {
      test.expect(1);
      test.equals(null, err);
      test.done();
    });
  },
  testgetlogintimeout: function(test) {
    dm.getLoginTimeout(function(err, seconds) {
      test.expect(2);
      test.ok(seconds);
      test.equal(60, seconds);
      test.done();
    });
  }
};
