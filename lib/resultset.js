/* jshint node: true */
"use strict";
var _ = require('lodash');
var jinst = require('./jinst');
var ResultSetMetaData = require('./resultsetmetadata');
var java = jinst.getInstance();
var winston = require('winston');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
}

var _valueTypeMap = null;

function ResultSet(rs) {
  this._rs = rs;
}

ResultSet.TYPE_FORWARD_ONLY        = 1003;
ResultSet.TYPE_SCROLL_INSENSITIVE  = 1004;
ResultSet.TYPE_SCROLL_SENSITIVE    = 1005;
ResultSet.CONCUR_READ_ONLY         = 1007;
ResultSet.CONCUR_UPDATABLE         = 1008;
ResultSet.HOLD_CURSORS_OVER_COMMIT = 1;
ResultSet.CLOSE_CURSORS_AT_COMMIT  = 2;
ResultSet.FETCH_FORWARD            = 1000;
ResultSet.FETCH_REVERSE            = 1001;
ResultSet.FETCH_UNKNOWN            = 1002;

jinst.events.once('initialized', function onInitialized() {
  _valueTypeMap = (function () {
    var typeNames = [];

    typeNames[java.getStaticFieldValue("java.sql.Types", "BIT")] = "Boolean";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TINYINT")] = "Short";
    typeNames[java.getStaticFieldValue("java.sql.Types", "SMALLINT")] = "Short";
    typeNames[java.getStaticFieldValue("java.sql.Types", "INTEGER")] = "Int";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BIGINT")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "FLOAT")] = "Float";
    typeNames[java.getStaticFieldValue("java.sql.Types", "REAL")] = "Float";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DOUBLE")] = "Double";
    typeNames[java.getStaticFieldValue("java.sql.Types", "NUMERIC")] = "BigDecimal";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DECIMAL")] = "BigDecimal";
    typeNames[java.getStaticFieldValue("java.sql.Types", "CHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "VARCHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARCHAR")] = "String";
    typeNames[java.getStaticFieldValue("java.sql.Types", "DATE")] = "Date";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TIME")] = "Time";
    typeNames[java.getStaticFieldValue("java.sql.Types", "TIMESTAMP")] = "Timestamp";
    typeNames[java.getStaticFieldValue("java.sql.Types", "BINARY")] = "Bytes";
    typeNames[java.getStaticFieldValue("java.sql.Types", "VARBINARY")] = "Bytes";
    typeNames[java.getStaticFieldValue("java.sql.Types", "LONGVARBINARY")] = "Bytes";

    var javaVersion = java.callStaticMethodSync('java.lang.System', 'getProperty', 'java.version');
    var jvi = /([0-9]+)\.([0-9]+)\.([0-9]+)(?:_([0-9]+))?(?:-(.+))?/.exec(javaVersion);

    // since 1.2
    if (jvi[1] > 1 || jvi[2] >= 2) {
      typeNames[java.getStaticFieldValue("java.sql.Types", "BLOB")] = "Bytes";
      typeNames[java.getStaticFieldValue("java.sql.Types", "CLOB")] = "String";
    }

    // since 1.4
    if (jvi[1] > 1 || jvi[2] >= 4) {
      typeNames[java.getStaticFieldValue("java.sql.Types", "BOOLEAN")] = "Boolean";
    }

    // since 1.6
    if (jvi[1] > 1 || jvi[2] >= 6) {
      typeNames[java.getStaticFieldValue("java.sql.Types", "NCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "NVARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "LONGNVARCHAR")] = "String";
      typeNames[java.getStaticFieldValue("java.sql.Types", "NCLOB")] = "String";
    }

    return typeNames;
  })();
});

ResultSet.prototype.toObjArray = function (callback) {
  this.toObject(function (err, result) {
    if (err) return callback(err);
    callback(null, result.rows);
  });
};

ResultSet.prototype.toObject = function (callback) {
  this.toObjectIter(function (err, rs) {
    if (err) return callback(err);

    var rowIter = rs.rows;
    var rows = [];
    var row = rowIter.next();

    while (!row.done) {
      rows.push(row.value);
      row = rowIter.next();
    }

    rs.rows = rows;
    return callback(null, rs);
  });
};

ResultSet.prototype.toObjectIter = function (callback) {
  var self = this;

  self.getMetaData(function (err, rsmd) {
    if (err) {
      return callback(err);
    } else {
      var colsmetadata = [];

      rsmd.getColumnCount(function (err, colcount) {

        if (err)
          return callback(err);

        // Get some column metadata.
        _.each(_.range(1, colcount + 1), function (i) {
          colsmetadata.push({
            label: rsmd._rsmd.getColumnLabelSync(i),
            type: rsmd._rsmd.getColumnTypeSync(i)
          });
        });

        callback(null, {
          labels: _.map(colsmetadata, 'label'),
          types: _.map(colsmetadata, 'type'),
          rows: {
            next: function () {
              var nextRow;
              try {
                nextRow = self._rs.nextSync(); // this row can lead to Java RuntimeException - sould be cathced.
              } 
              catch (error) {
                callback(error);
              }
              if (!nextRow) {
                return {
                  done: true
                };
              }

              var result = {};

              // loop through each column
              _.each(_.range(1, colcount + 1), function (i) {
                var cmd = colsmetadata[i - 1];
                var type = _valueTypeMap[cmd.type] || 'String';
                var getter = 'get' + type + 'Sync';

                if (type === 'Date' || type === 'Time' || type === 'Timestamp') {
                  var dateVal = self._rs[getter](i);
                  result[cmd.label] = dateVal ? dateVal.toString() : null;
                } else {
                  // If the column is an integer and is null, set result to null and continue
                  if (type === 'Int' && _.isNull(self._rs.getObjectSync(i))) {
                    result[cmd.label] = null;
                    return;
                  }

                  result[cmd.label] = self._rs[getter](i);
                }
              });

              return {
                value: result,
                done: false
              };
            }
          }
        });
      });
    }
  });
};

ResultSet.prototype.close = function (callback) {
  this._rs.close(function (err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

ResultSet.prototype.getMetaData = function (callback) {
  this._rs.getMetaData(function (err, rsmd) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, new ResultSetMetaData(rsmd));
    }
  });
};

module.exports = ResultSet;
