var nodeunit = require('nodeunit');
var jinst = require('../lib/jinst');
var JDBC = require('../lib/jdbc');

if (!jinst.isJvmCreated()) {
	jinst.addOption("-Xrs");
	jinst.setupClasspath([
		'./drivers/Altibase.jar',
		'./drivers/Altibase6_5.jar',
	]);
}

var config = {
	url: 'jdbc:Altibase://testserver:20300/mydb',
	user: 'sys',
	password: 'manager'
};

var LARGE_STR = '';
for (var i = 0; i < 1000000; i++)
	LARGE_STR += 'aaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbqweeeeeeeeeeeeeeeeeeeeeeeeeeeeee2222222222222222222222oooooooooooooodddddddddddddddddddkkkkkkkkkkkkkkkkkkfffffffffffffffffffjjjjjjjjjjjjjjjjjjjwwwwwwwwwwwwwwwnnnnnnnnnnnnnnnnnnnxxxxxxxxxxxxxxxxxxxxxkkkkkkkkkkkkkkkkkkkkkk';

let testInsertSelect = (test, pconn, id_in, val_in, val_exp, val_len) => {
	if (val_exp === undefined)
		val_exp = val_in;

	return new Promise((resolve, reject) => {
		pconn.conn.prepareStatement('INSERT INTO nodetest_clob VALUES (?, ?)', function(err, stmt) {
			if (err) return reject(err);

			stmt.setInt(1, id_in, function(err1) {
				if (err) return reject(err);

				let checkRows = function(err7, rows) {
					if (err7) return reject(err7);

					test.equal(1, rows.length);
					var val = rows[0].VAL;
					if (val != null) {
						test.equal(val.toString(), val_exp);
					} else {
						test.equal(val, val_exp);
					}

					resolve();
				}
				let selectCheck = function(err4, stmtSel) {
					if (err4) return reject(err4);

					stmtSel.setInt(1, id_in, function(err5) {
						if (err5) return reject(err5);

						stmtSel.executeQuery(function(err6, rs) {
							if (err6) return reject(err6);

							rs.toObjArray(checkRows);
						});
					});
				}
				let procAfter = function(err2) {
					if (err2) return reject(err2);

					stmt.executeUpdate(function(err3, result) {
						if (err3) return reject(err3);

						test.equals(result, 1);

						pconn.conn.prepareStatement('SELECT val FROM nodetest_clob WHERE id = ?', selectCheck);
					});
				};

				try {
					if (val_len != null)
						stmt.setCharacterStream(2, val_in, val_len, procAfter);
					else
						stmt.setCharacterStream(2, val_in, procAfter);
				} catch (errt) {
					reject(errt);
				}
			});
		});
	});
}

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
	testinit: function(test) {
		altidb.initialize(function(err) {
			test.expect(1);
			test.equal(null, err);
			test.done();
		});
	},
	testcreatetable: function(test) {
		testconn.conn.createStatement(function(err, statement) {
			test.expect(3);
			test.equal(null, err);
			statement.execute("DROP TABLE nodetest_clob", function(err, result){
				statement.executeUpdate("CREATE TABLE nodetest_clob (id INT, val CLOB)", function(err, result) {
					test.equal(null, err);
					test.equal(0, result);
					test.done();
				});
			});
		});
	},
	testsetautocommitfalse: function(test) {
		testconn.conn.setAutoCommit(false, function(err) {
			test.expect(1);
			test.equal(null, err);
			test.done();
		});
	},
	testInsert_a: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 1, 'a');
		test.done();
	},
	testInsert_bb: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 2, 'bb');
		test.done();
	},
	testInsert_k1: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 3, '가나');
		test.done();
	},
	testInsert_k2: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 4, 'a가나');
		test.done();
	},
	testInsert_k3: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 5, '가b나');
		test.done();
	},
	testInsert_k4: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 6, '가나다라마');
		test.done();
	},
	testInsert_k5: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 7, '가나다라마a');
		test.done();
	},
	testInsert_k6: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 8, '가나다라마바');
		test.done();
	},
	testInsert_k7: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 9, '0가a나b다c');
		test.done();
	},
	testInsert_1234567890abcdefghijklmn_10: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 10, '1234567890abcdefghijklmn', '1234567890', 10);
		test.done();
	},
	testInsert_1234567890abcdefghijklmn_20: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 11, '1234567890abcdefghijklmn', '1234567890abcdefghij', 20);
		test.done();
	},
	testInsert_1234567890abcdefghijklmn_100: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 12, '1234567890abcdefghijklmn', '1234567890abcdefghijklmn', 100);
		test.done();
	},
	testInsert_1234567890192837_null: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 13, 1234567890192837, '1234567890192837', null);
		test.done();
	},
	testInsert_1234567890192837_undefined: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 14, 1234567890192837, '1234567890192837', undefined);
		test.done();
	},
	testInsert_1234567890192837: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 15, 1234567890192837);
		test.done();
	},
	testInsert_LARGE_STR: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 16, LARGE_STR);
		test.done();
	},
	testInsert_null: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 17, null);
		test.done();
	},
	testInsert_null_s: async function(test) {
		test.expect(3);
		await testInsertSelect(test, testconn, 18, 'null');
		test.done();
	},
	testdroptable: function(test) {
		testconn.conn.createStatement(function(err, statement) {
			if (err) {
				console.log(err);
			} else {
				statement.executeUpdate("DROP TABLE nodetest_clob;", function(err, result) {
					test.expect(2);
					test.equal(null, err);
					test.equal(0, result);
					test.done();
				});
			}
		});
	},
};

