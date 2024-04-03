--SET log_min_messages  TO DEBUG4;
--SET client_min_messages  TO DEBUG4;
--Testcase 44:
CREATE EXTENSION sqlite_fdw;
--Testcase 45:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER sqlite_fdw
OPTIONS (database '/tmp/sqlite_fdw_test/common.db');

--Testcase 46:
CREATE SERVER sqlite2 FOREIGN DATA WRAPPER sqlite_fdw;

IMPORT FOREIGN SCHEMA public FROM SERVER sqlite_svr INTO public;

--Testcase 16:
SELECT * FROM "type_STRING";

SET log_min_messages  TO DEBUG4;
SET client_min_messages  TO DEBUG4;

UPDATE "type_STRING" SET col = '_' || substr(col, 2) RETURNING *;

--Testcase 16:
SELECT * FROM "type_STRING";
--Testcase 16:
DELETE FROM "type_STRING" RETURNING *;

--Testcase 1:
INSERT INTO "type_STRING"(col) VALUES ('string') ON CONFLICT DO NOTHING RETURNING *;

--Testcase 18:
SELECT * FROM "type_BYTE";
--Testcase 18:
DELETE FROM "type_BYTE" RETURNING *;
--Testcase 19:
SELECT * FROM "type_SINT";
--Testcase 19:
DELETE FROM "type_SINT" RETURNING *;
--Testcase 20:
SELECT * FROM "type_BINT";
--Testcase 20:
DELETE FROM "type_BINT" RETURNING *;
--Testcase 21:
SELECT * FROM "type_INTEGER";
--Testcase 21:
DELETE FROM "type_INTEGER" RETURNING *;
--Testcase 22:
SELECT * FROM "type_FLOAT";
--Testcase 22:
DELETE FROM "type_FLOAT" RETURNING *;
--Testcase 23:
SELECT * FROM "type_DOUBLE";
--Testcase 23:
DELETE FROM "type_DOUBLE" RETURNING *;
--
set datestyle=ISO;
--Testcase 24:
SELECT * FROM "type_TIMESTAMP";
--Testcase 24:
DELETE FROM "type_TIMESTAMP" RETURNING *;
--Testcase 25:
SELECT * FROM "type_BLOB";
--Testcase 25:
DELETE FROM "type_BLOB" RETURNING *;

--Testcase 25:
DELETE FROM "type_UUID" RETURNING *;
--Testcase 25:
DELETE FROM "type_BITSTRING" RETURNING *;

--Testcase 26:
SELECT * FROM typetest;
--Testcase 1:
INSERT INTO "type_STRING"(col) VALUES ('string') ON CONFLICT DO NOTHING RETURNING *;
--Testcase 2:
EXPLAIN VERBOSE
INSERT INTO "type_STRING"(col) VALUES ('string') RETURNING *;
--Testcase 4:
INSERT INTO "type_BYTE"(col) VALUES ('c') RETURNING *;
--Testcase 5:
INSERT INTO "type_SINT"(col) VALUES (32767) RETURNING *;
--Testcase 6:
INSERT INTO "type_SINT"(col) VALUES (-32768) RETURNING *;
--Testcase 7:
INSERT INTO "type_BINT"(col) VALUES (9223372036854775807) RETURNING *;
--Testcase 8:
INSERT INTO "type_BINT"(col) VALUES (-9223372036854775808) RETURNING *;
--Testcase 9:
INSERT INTO "type_INTEGER"(col) VALUES (9223372036854775807) RETURNING *;

--Testcase 10:
INSERT INTO "type_FLOAT"(col) VALUES (3.1415) RETURNING *;
--Testcase 11:
INSERT INTO "type_DOUBLE"(col) VALUES (3.14159265) RETURNING *;
--Testcase 12:
INSERT INTO "type_TIMESTAMP" VALUES ('2017.11.06 12:34:56.789', '2017.11.06') RETURNING *;
--Testcase 13:
INSERT INTO "type_TIMESTAMP" VALUES ('2017.11.06 1:3:0', '2017.11.07') RETURNING *;
--Testcase 14:
INSERT INTO "type_BLOB"(col) VALUES (bytea('\xDEADBEEF')) RETURNING *;
--Testcase 15:
INSERT INTO typetest VALUES(1,'a', 'b', 'c','2017.11.06 12:34:56.789', '2017.11.06 12:34:56.789' )  RETURNING *;

--Testcase 16:
SELECT * FROM "type_STRING";
--Testcase 18:
SELECT * FROM "type_BYTE";
--Testcase 19:
SELECT * FROM "type_SINT";
--Testcase 20:
SELECT * FROM "type_BINT";
--Testcase 21:
SELECT * FROM "type_INTEGER";
--Testcase 22:
SELECT * FROM "type_FLOAT";
--Testcase 23:
SELECT * FROM "type_DOUBLE";
set datestyle=ISO;
--Testcase 24:
SELECT * FROM "type_TIMESTAMP";
--Testcase 25:
SELECT * FROM "type_BLOB";
--Testcase 26:
SELECT * FROM typetest;

--Testcase 27:
insert into "type_STRING" values('TYPE') RETURNING *;
--Testcase 28:
insert into "type_STRING" values('type') RETURNING *;

-- not pushdown
--Testcase 29:
SELECT  *FROM "type_STRING" WHERE col like 'TYP%';
--Testcase 30:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col like 'TYP%';
-- pushdown
--Testcase 31:
SELECT  *FROM "type_STRING" WHERE col ilike 'typ%';
--Testcase 32:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col ilike 'typ%';

--Testcase 33:
SELECT  *FROM "type_STRING" WHERE col ilike 'typ%' and col like 'TYPE';
--Testcase 34:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col ilike 'typ%' and col like 'TYPE';

--Testcase 35:
SELECT * FROM "type_TIMESTAMP";

--Testcase 48:
INSERT INTO "type_DATE"(col) VALUES ('2021.02.23') RETURNING col;
--Testcase 49:
INSERT INTO "type_DATE"(col) VALUES ('2021/03/08') RETURNING col;
--Testcase 50:
INSERT INTO "type_DATE"(col) VALUES ('9999-12-30') RETURNING col;
--Testcase 58:
SELECT * FROM "type_DATE";

--Testcase 51:
INSERT INTO "type_TIME"(col) VALUES ('01:23:45') RETURNING col;
--Testcase 52:
INSERT INTO "type_TIME"(col) VALUES ('01:23:45.6789') RETURNING col;
--Testcase 59:
SELECT * FROM "type_TIME";

--Testcase 60:
EXPLAIN VERBOSE
SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c17, c18, c19, c2, c21, c22, c23, c24 FROM alltypetest;
--Testcase 61:
SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,  c17, c18, c19, c2, c21, c22, c23, c24 FROM alltypetest;

--Testcase 53:
CREATE FOREIGN TABLE type_JSON(col JSON OPTIONS (key 'true')) SERVER sqlite_svr OPTIONS (table 'type_TEXT');
--Testcase 54:
INSERT INTO type_JSON(col) VALUES ('[1, 2, "foo", null]') RETURNING *;
--Testcase 55:
INSERT INTO type_JSON(col) VALUES ('{"bar": "baz", "balance": 7.77, "active": false}'::json) RETURNING *;
--Testcase 56
SELECT * FROM type_JSON;
--Testcase 57
DELETE FROM type_JSON RETURNING *;

-- drop column
--Testcase 62:
DROP FOREIGN TABLE IF EXISTS "type_BOOLEAN";
--Testcase 63:
CREATE FOREIGN TABLE "type_BOOLEAN" (i int, b boolean) SERVER sqlite_svr;
--Testcase 64:
ALTER FOREIGN TABLE "type_BOOLEAN" DROP COLUMN i;
--Testcase 65:
SELECT * FROM "type_BOOLEAN"; -- OK

-- define INTEGER as TEXT column
--Testcase 67:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE text;
--Testcase 68:
SELECT * FROM "type_INTEGER"; -- OK

-- define INTEGER as bpchar
--Testcase 69:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE char(30);
--Testcase 70:
SELECT * FROM "type_INTEGER"; -- OK
-- define INTEGER as varchar
--Testcase 71:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE varchar(30);
--Testcase 72:
SELECT * FROM "type_INTEGER"; -- OK

-- define INTEGER as name
--Testcase 73:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE name;
--Testcase 74:
SELECT * FROM "type_INTEGER"; -- OK

-- define INTEGER as json
--Testcase 75:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE json;
--Testcase 76:
SELECT * FROM "type_INTEGER"; -- OK

-- define INTEGER as time
--Testcase 77:
DELETE FROM "type_INTEGER";
--Testcase 78:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE int;
--Testcase 79:
INSERT INTO "type_INTEGER" VALUES (120506) RETURNING *;
--Testcase 80:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE time;
--Testcase 81:
SELECT * FROM "type_INTEGER"; -- OK

-- define INTEGER as date
--Testcase 82:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE date;
--Testcase 83:
SELECT * FROM "type_INTEGER"; -- OK

--Testcase 84:
ALTER FOREIGN TABLE "type_INTEGER" ALTER COLUMN col TYPE int;

--Testcase 85:
INSERT INTO "type_DOUBLE" VALUES (1.3e-5);
--Testcase 86:
SELECT * FROM "type_DOUBLE";

-- define DOUBLE as TEXT column
--Testcase 87:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE text;
--Testcase 88:
SELECT * FROM "type_DOUBLE"; -- OK

-- define DOUBLE as bpchar
--Testcase 89:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE char(30);
--Testcase 90:
SELECT * FROM "type_DOUBLE"; -- OK
-- define DOUBLE as varchar
--Testcase 91:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE varchar(30);
--Testcase 92:
SELECT * FROM "type_DOUBLE"; -- OK

-- define DOUBLE as name
--Testcase 93:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE name;
--Testcase 94:
SELECT * FROM "type_DOUBLE"; -- OK

-- define DOUBLE as json
--Testcase 95:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE json;
--Testcase 96:
SELECT * FROM "type_DOUBLE"; -- OK

--Testcase 97:
DELETE FROM "type_DOUBLE";
--Testcase 98:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE float8;
--Testcase 99:
INSERT INTO "type_DOUBLE" VALUES (120506.12);

-- define DOUBLE as time
--Testcase 100:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE time;
--Testcase 101:
SELECT * FROM "type_DOUBLE"; -- OK

--Testcase 102:
DELETE FROM "type_DOUBLE";
--Testcase 103:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE float8;
--Testcase 104:
INSERT INTO "type_DOUBLE" VALUES (1999.012);
-- define DOUBLE as date
--Testcase 105:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE date;
--Testcase 106:
SELECT * FROM "type_DOUBLE"; -- OK

--Testcase 107:
ALTER FOREIGN TABLE "type_DOUBLE" ALTER COLUMN col TYPE float8;

--Testcase 47:
DROP EXTENSION sqlite_fdw CASCADE;
