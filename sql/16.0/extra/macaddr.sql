--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
--Testcase 001:
CREATE EXTENSION sqlite_fdw;
--Testcase 002:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER sqlite_fdw
OPTIONS (database '/tmp/sqlite_fdw_test/common.db');

--Testcase 003:
CREATE SERVER sqlite2 FOREIGN DATA WRAPPER sqlite_fdw;

--Testcase 009:
CREATE FOREIGN TABLE "type_MACADDR"( "i" int OPTIONS (key 'true'), "m" macaddr) SERVER sqlite_svr OPTIONS (table 'type_MACADDR');
--Testcase 010:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE text;
--Testcase 011:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (1, '08:00:2b:01:02:03');
--Testcase 012:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (2, '08-00-2b-01-02-03');
--Testcase 013:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (3, '08002b:010203');
--Testcase 014:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (4, '08002b-010203');
--Testcase 015:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (5, '0800.2b01.0203');
--Testcase 016:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (6, '0800-2b01-0203');
--Testcase 017:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (7, '08002b010203');
--Testcase 018:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (8, '08:00:2F:01:02:03');
--Testcase 019:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (9, '08-00-2F-01-02-03');
--Testcase 020:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (10, '08002F:010203');
--Testcase 021:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (11, '08002F-010203');
--Testcase 022:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (12, '0800.2F01.0203');
--Testcase 023:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (13, '0800-2F01-0203');
--Testcase 024:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (14, '08002F010203');
--Testcase 025:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE bytea;
--Testcase 026:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (15, decode('08002F010203', 'hex'));
--Testcase 027:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (16, decode('08002b010203', 'hex'));
--Testcase 028:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE macaddr;
--Testcase 029:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (17, '08:00:2b:01:02:03');
--Testcase 030:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (18, '08-00-2b-01-02-03');
--Testcase 031:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (19, '08002b:010203');
--Testcase 032:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (20, '08002b-010203');
--Testcase 033:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (21, '0800.2b01.0203');
--Testcase 034:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (22, '0800-2b01-0203');
--Testcase 035:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (23, '08002b010203');
--Testcase 036:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (24, '08:00:2F:01:02:03');
--Testcase 037:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (25, '08-00-2F-01-02-03');
--Testcase 038:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (26, '08002F:010203');
--Testcase 039:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (27, '08002F-010203');
--Testcase 040:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (28, '0800.2F01.0203');
--Testcase 041:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (29, '0800-2F01-0203');
--Testcase 042:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (30, '08002F010203');
--Testcase 043:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO "type_MACADDR" ("i", "m") VALUES (30, '08002F010203');
--Testcase 044:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (ADD column_type 'BLOB');
--Testcase 045:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (31, '08:00:2b:01:02:03');
--Testcase 046:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (32, '08-00-2b-01-02-03');
--Testcase 047:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (33, '08002b:010203');
--Testcase 048:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (34, '08002b-010203');
--Testcase 049:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (35, '0800.2b01.0203');
--Testcase 050:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (36, '0800-2b01-0203');
--Testcase 051:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (37, '08002b010203');
--Testcase 052:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (38, '08:00:2F:01:02:03');
--Testcase 053:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (39, '08-00-2F-01-02-03');
--Testcase 054:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (40, '08002F:010203');
--Testcase 055:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (41, '08002F-010203');
--Testcase 056:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (42, '0800.2F01.0203');
--Testcase 057:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (43, '0800-2F01-0203');
--Testcase 058:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (44, '08002F010203');
--Testcase 059:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO "type_MACADDR" ("i", "m") VALUES (44, '08002F010203');
--Testcase 060:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'integer');
--Testcase 061:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (45, '08:00:2b:01:02:03');
--Testcase 062:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (46, '08-00-2b-01-02-03');
--Testcase 063:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (47, '08002b:010203');
--Testcase 064:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (48, '08002b-010203');
--Testcase 065:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (49, '0800.2b01.0203');
--Testcase 066:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (50, '0800-2b01-0203');
--Testcase 067:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (51, '08002b010203');
--Testcase 068:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (52, '08:00:2F:01:02:03');
--Testcase 069:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (53, '08-00-2F-01-02-03');
--Testcase 070:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (54, '08002F:010203');
--Testcase 071:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (55, '08002F-010203');
--Testcase 072:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (56, '0800.2F01.0203');
--Testcase 073:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (57, '0800-2F01-0203');
--Testcase 074:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (58, '08002F010203');
--Testcase 075:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO "type_MACADDR" ("i", "m") VALUES (58, '08002F010203');
--Testcase 076:
CREATE FOREIGN TABLE "type_MACADDR+"( "i" int OPTIONS (key 'true'), "m" macaddr, "t" text, "l" smallint, "tx" varchar(64)) SERVER sqlite_svr OPTIONS (table 'type_MACADDR+');
--Testcase 077:
SELECT * FROM "type_MACADDR+";
--Testcase 078:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'BLOB');
--Testcase 079:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 080:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 081:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'TEXT');
--Testcase 082:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 083:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 084:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2F:01:02:03';
--Testcase 085:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'integer');
--Testcase 086:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 087:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2b:01:02:03';
--Testcase 088:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2F:01:02:03';
--Testcase 089:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'BLOB');
--Testcase 090:
SELECT * FROM "type_MACADDR+" where "m" = '08:00:2F:01:02:03';
--Testcase 091:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 15;
--Testcase 092:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 15;
--Testcase 093:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'TEXT');
--Testcase 094:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 16;
--Testcase 095:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 16;
--Testcase 096:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'integer');
--Testcase 097:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 17; -- 9527026057731
--Testcase 098:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "i" = 17;
--Testcase 099:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'BLOB');

--Testcase 100:
DELETE FROM "type_MACADDR" WHERE "m" = '08:00:2F:01:02:03';
--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM "type_MACADDR" WHERE "m" = '08:00:2F:01:02:03';
--Testcase 102:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'integer');
--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM "type_MACADDR" WHERE "m" = '08:00:2F:01:02:03';
--Testcase 104:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'TEXT');
--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM "type_MACADDR" WHERE "m" = '08:00:2F:01:02:03';
--Testcase 106:
SELECT * FROM "type_MACADDR+";
--Testcase 107:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (59, '08:AA:2F:01:02:04');
--Testcase 108:
SELECT * FROM "type_MACADDR+" WHERE "i" = 59;
--Testcase 109:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:05' WHERE "m" = '08:AA:2F:01:02:04';
--Testcase 110: -- text
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:05' WHERE "m" = '08:AA:2F:01:02:04';
--Testcase 111:
SELECT * FROM "type_MACADDR+";
--Testcase 112:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'BLOB');
--Testcase 113:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "m" = '08:AA:2F:01:02:05';
--Testcase 114: -- BLOB
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:03' WHERE "m" = '08:AA:2F:01:02:05';
--Testcase 115:
SELECT * FROM "type_MACADDR+";
--Testcase 116:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" OPTIONS (SET column_type 'int');
--Testcase 117:
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:02' WHERE "m" = '08:AA:2F:01:02:03';
--Testcase 118: -- BLOB
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE "type_MACADDR" SET "m" = '08:AA:2F:01:02:02' WHERE "m" = '08:AA:2F:01:02:03';
--Testcase 119:
SELECT * FROM "type_MACADDR+";

--Testcase 120:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE bytea;
--Testcase 121:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (60, decode('08002F0102', 'hex'));
--Testcase 122:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (61, decode('08002F01020304', 'hex'));
--Testcase 123:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE macaddr;
--Testcase 124: -- err
SELECT * FROM "type_MACADDR+" WHERE "i" = 60;
--Testcase 125: -- err
SELECT * FROM "type_MACADDR+" WHERE "i" = 61;
--Testcase 126:
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM "type_MACADDR" WHERE "i" IN (60, 61);
--Testcase 127:
DELETE FROM "type_MACADDR" WHERE "i" IN (60, 61);
--Testcase 128:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE bigint;
--Testcase 129:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (62, 0xFFFFFFFFFFFFFF);
--Testcase 130:
ALTER FOREIGN TABLE "type_MACADDR" ALTER COLUMN "m" TYPE macaddr;
--Testcase 131: -- err
SELECT * FROM "type_MACADDR+" WHERE "i" = 62;
--Testcase 132:
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM "type_MACADDR" WHERE "i" IN (62);
--Testcase 133:
DELETE FROM "type_MACADDR" WHERE "i" IN (62);
--Testcase 134:
INSERT INTO "type_MACADDR" ("i", "m") VALUES (63, NULL);
--Testcase 135:
SELECT * FROM "type_MACADDR+";
--Testcase 136:
SELECT * FROM "type_MACADDR+" WHERE "m" IS NULL;
--Testcase 137:
SELECT * FROM "type_MACADDR+" WHERE "m" IS NOT NULL;
--Testcase 138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM "type_MACADDR+" WHERE "m" IS NULL;
--Testcase 139:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM "type_MACADDR+" WHERE "m" IS NOT NULL;

--Testcase 140:
CREATE FOREIGN TABLE "type_MACADDRpk" (col macaddr OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 141:
ALTER FOREIGN TABLE "type_MACADDRpk" ALTER COLUMN "m" OPTIONS (SET column_type 'BLOB');
--Testcase 142:
INSERT INTO "type_MACADDRpk" VALUES ('01:02:03:04:05:06');
--Testcase 143:
ALTER FOREIGN TABLE "type_MACADDRpk" ALTER COLUMN "m" OPTIONS (SET column_type 'int');
--Testcase 144:
INSERT INTO "type_MACADDRpk" VALUES ('01:02:03:04:05:06');
--Testcase 145:
ALTER FOREIGN TABLE "type_MACADDRpk" ALTER COLUMN "m" OPTIONS (SET column_type 'text');
--Testcase 146: ERR - primary key
INSERT INTO "type_MACADDRpk" VALUES ('01:02:03:04:05:06');
--Testcase 147:
ALTER FOREIGN TABLE "type_MACADDRpk" ALTER COLUMN col OPTIONS (ADD column_type 'BLOB');
--Testcase 148: NO ERR, but the same semantics!
INSERT INTO "type_MACADDRpk" VALUES ('01-02-03-04-05-06');
--Testcase 149:
DELETE FROM "type_MACADDRpk";

--Testcase 200:
DROP EXTENSION sqlite_fdw CASCADE;
