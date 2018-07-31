/* jshint node: true */
"use strict";
var _ = require('lodash');
var ResultSet = require('./resultset');
var jinst = require('./jinst');
var java = jinst.getInstance();

function Statement(s) {
  this._s = s;
}

Statement.CLOSE_CURRENT_RESULT  = 1;
Statement.KEEP_CURRENT_RESULT   = 2;
Statement.CLOSE_ALL_RESULTS     = 3;
Statement.SUCCESS_NO_INFO       = -2;
Statement.EXECUTE_FAILED        = -3;
Statement.RETURN_GENERATED_KEYS = 1;
Statement.NO_GENERATED_KEYS     = 2;

Statement.prototype.addBatch = function(sql, callback) {
  return callback(new Error("NOT IMPLEMENTED"));
}

Statement.prototype.cancel = function(callback) {
  this._s.cancel(function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
}

Statement.prototype.clearBatch = function(callback) {
  this._s.clearBatch(function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
}

Statement.prototype.close = function(callback) {
  this._s.close(function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

Statement.prototype.executeUpdate = function(sql, arg1, callback) {
  // Get arguments as an array
  var args = Array.prototype.slice.call(arguments);

  // Pull the callback off the end of the arguments
  callback = args.pop();

  // Check arguments for validity, and return error if invalid
  if(! (_.isString(args[0]) && _.isUndefined(args[1]))) {
    return callback(new Error('INVALID ARGUMENTS'));
  }

  // Push a callback handler onto the arguments
  args.push(function(err, count) {
    if (err) {
      return callback(err);
    }
    return callback(null, count);
  });

  // Forward modified arguments to _s.executeUpdate
  this._s.executeUpdate.apply(this._s, args);
};

Statement.prototype.executeQuery = function(sql, callback) {
  if (typeof sql === 'string') {
    this._s.executeQuery(sql, function(err, resultset) {
      if (err) {
        return callback(err);
      } else {
        return callback(null, new ResultSet(resultset));
      }
    });
  } else {
    return callback(new Error('INVALID ARGUMENTS'));
  }
};

Statement.prototype.execute = function(sql, callback) {
  var s = this._s;
  if (typeof sql === 'string') {
    s.execute(sql, function(err, isResultSet) {
      if (err) {
        return callback(err);
      }
      if (isResultSet) {
        s.getResultSet(function(err, resultset) {
          if (err) {
            return callback(err);
          }
          return callback(null, new ResultSet(resultset));
        });
      } else {
        s.getUpdateCount(function(err, count) {
          if (err) {
            return callback(err);
          }
          return callback(null, count);
        });
      }
    });
  } else {
    return callback(new Error('INVALID ARGUMENTS'));
  }
};

Statement.prototype.getFetchSize = function(callback) {
  this._s.getFetchSize(function(err, fetchSize) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, fetchSize);
    }
  });
};

Statement.prototype.setFetchSize = function(rows, callback) {
  this._s.setFetchSize(rows, function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

Statement.prototype.getMaxRows = function(callback) {
  this._s.getMaxRows(function(err, max) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, max);
    }
  });
};

Statement.prototype.setMaxRows = function(max, callback) {
  this._s.setMaxRows(max, function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

Statement.prototype.getQueryTimeout = function(callback) {
  this._s.getQueryTimeout(function(err, queryTimeout) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, queryTimeout);
    }
  });
};

Statement.prototype.setQueryTimeout = function(seconds, callback) {
  this._s.setQueryTimeout(seconds, function(err) {
    if (err) {
      return callback(err);
    } else {
      return callback(null);
    }
  });
};

Statement.prototype.getGeneratedKeys = function(callback) {
  this._s.getGeneratedKeys(function(err, resultset) {
    if(err) {
      return callback(err);
    }
    return callback(null, new ResultSet(resultset));
  });
};

module.exports = Statement;
