/* jshint node: true */
"use strict";

var Types = {};

Types.BIT           = -7;
Types.TINYINT       = -6;
Types.SMALLINT      = 5;
Types.INTEGER       = 4;
Types.BIGINT        = -5;
Types.FLOAT         = 6;
Types.REAL          = 7;
Types.DOUBLE        = 8;
Types.NUMERIC       = 2;
Types.DECIMAL       = 3;
Types.CHAR          = 1;
Types.VARCHAR       = 12;
Types.LONGVARCHAR   = -1;
Types.DATE          = 91;
Types.TIME          = 92;
Types.TIMESTAMP     = 93;
Types.BINARY        = -2;
Types.VARBINARY     = -3;
Types.LONGVARBINARY = -4;
Types.NULL          = 0;
Types.OTHER         = 1111;

// since 1.2
Types.JAVA_OBJECT   = 2000;
Types.DISTINCT      = 2001;
Types.STRUCT        = 2002;
Types.ARRAY         = 2003;
Types.BLOB          = 2004;
Types.CLOB          = 2005;
Types.REF           = 2006;

// since 1.4
Types.DATALINK      = 70;
Types.BOOLEAN       = 16;

// since 1.6
Types.ROWID         = -8;
Types.NCHAR         = -15;
Types.NVARCHAR      = -9;
Types.LONGNVARCHAR  = -16;
Types.NCLOB         = 2011;
Types.SQLXML        = 2009;

// since 1.8
Types.REF_CURSOR              = 2012;
Types.TIME_WITH_TIMEZONE      = 2013;
Types.TIMESTAMP_WITH_TIMEZONE = 2014;

// Altibase specific types
Types.INTERVAL = 10;
Types.VARBIT   = -100;
Types.NUMBER   = 10002;
Types.GEOMETRY = 10003;
Types.BYTE     = 20001;
Types.VARBYTE  = 20003;
Types.NIBBLE   = 20002;

module.exports = Types;
