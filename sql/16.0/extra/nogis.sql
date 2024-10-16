--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
--Testcase 001:
CREATE EXTENSION sqlite_fdw;
--Testcase 002:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER sqlite_fdw
OPTIONS (database '/tmp/sqlite_fdw_test/common.db');

-- --Testcase 01:
CREATE DOMAIN geometry AS bytea;
-- --Testcase 02:
CREATE DOMAIN geography AS bytea;
-- --Testcase 03:
CREATE DOMAIN addbandarg AS bytea;
-- --Testcase 04:
CREATE DOMAIN box2d AS bytea;
-- --Testcase 05:
CREATE DOMAIN box3d AS bytea;
-- --Testcase 06:
CREATE DOMAIN geometry_dump AS bytea;
-- --Testcase 07:
CREATE DOMAIN geomval AS bytea;
-- --Testcase 08:
CREATE DOMAIN getfaceedges_returntype AS bytea;
-- --Testcase 09:
CREATE DOMAIN rastbandarg AS bytea;
--Testcase 10:
CREATE DOMAIN raster AS bytea;
-- --Testcase 11:
CREATE DOMAIN reclassarg AS bytea;
-- --Testcase 12:
CREATE DOMAIN summarystats AS bytea;
-- --Testcase 13:
CREATE DOMAIN topoelement AS bytea;
-- --Testcase 14:
CREATE DOMAIN topoelementarray AS bytea;
-- --Testcase 15:
CREATE DOMAIN topogeometry AS bytea;
-- --Testcase 16:
CREATE DOMAIN unionarg AS bytea;
-- --Testcase 17:
CREATE DOMAIN validatetopology_returntype AS bytea;

--Testcase 30:
CREATE FOREIGN TABLE "types_PostGIS"( "i" int OPTIONS (key 'true'), gm geometry, gg geography, r raster, t text) SERVER sqlite_svr;

--Testcase 31: ERR - geometry
INSERT INTO "types_PostGIS" ( "i", gm, gg, r, t ) VALUES (1, decode('0101000020e6100000fd5aa846f9733e406c054d4bacd74d40', 'hex'),  decode('0101000020e6100000fd5aa846f9733e406c054d4bacd74d40', 'hex'),  decode('1223456890', 'hex'), '{"genus": "Rhododendron", "taxon": "Rhododendron ledebourii", "natural": "shrub", "genus:ru": "Рододендрон", "taxon:ru": "Рододендрон Ледебура", "source:taxon": "board"}');
--Testcase 32:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gm" TYPE bytea;
--Testcase 33:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gg" TYPE bytea;

-- Insert SpatiaLite BLOB, read PostGOS/GEOS BLOB
--Testcase 34: OK
INSERT INTO "types_PostGIS" ( "i", gm, gg, t ) VALUES (1, decode('0001e6100000bf72ce99fe763e40ed4960730ed84d40bf72ce99fe763e40ed4960730ed84d407c01000000bf72ce99fe763e40ed4960730ed84d40fe', 'hex'),  decode('0001e6100000bf72ce99fe763e40ed4960730ed84d40bf72ce99fe763e40ed4960730ed84d407c01000000bf72ce99fe763e40ed4960730ed84d40fe', 'hex'), '{"genus": "Rhododendron", "taxon": "Rhododendron ledebourii", "natural": "shrub", "genus:ru": "Рододендрон", "taxon:ru": "Рододендрон Ледебура", "source:taxon": "board"}');
--Testcase 35:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gm" TYPE geometry;
--Testcase 36:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gg" TYPE geography;
--Testcase 37: ERR - no GIS data support
SELECT "i", gm, gg, t FROM "types_PostGIS";
--Testcase 38:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT "i", gm, gg, t FROM "types_PostGIS";
--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT "i", gm, gg, t FROM "types_PostGIS" WHERE gm = '0101000020e6100000bf72ce99fe763e40ed4960730ed84d40'::geometry;
--Testcase 40: ERR - no GIS data support
SELECT "i", gm, gg, t FROM "types_PostGIS" WHERE gm = '0101000020e6100000bf72ce99fe763e40ed4960730ed84d40'::geometry;

-- Insert PostGIS/GEOS BLOB, read SpatiaLite BLOB
--Testcase 41: ERR - no GIS data support
INSERT INTO "types_PostGIS" ( "i", gm, gg, t ) VALUES (2, decode('0101000020e6100000bf72ce99fe763e40ed4960730ed84d40', 'hex'),  decode('0101000020e6100000bf72ce99fe763e40ed4960730ed84d40', 'hex'), '{"genus": "Rhododendron", "taxon": "Rhododendron ledebourii"}');
--Testcase 42:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gm" TYPE bytea;
--Testcase 43:
ALTER FOREIGN TABLE "types_PostGIS" ALTER COLUMN "gg" TYPE bytea;
--Testcase 44: OK
SELECT "i", gm, gg, t FROM "types_PostGIS";

--Testcase 45:
CREATE FOREIGN TABLE "♂" (
	id int4 OPTIONS (key 'true'),
	"UAI" varchar(254),
	"⌖" geometry,
	geom geometry,
	"t₀" date,
	"class" text,
	"URL" varchar(80)
) SERVER sqlite_svr;

--Testcase 47:
ALTER FOREIGN TABLE "♂" ALTER COLUMN "⌖" TYPE bytea;
--Testcase 48:
ALTER FOREIGN TABLE "♂" ALTER COLUMN "geom" TYPE bytea;
--Testcase 49:
SELECT * FROM "♂";
--Testcase 50:
ALTER FOREIGN TABLE "♂" ALTER COLUMN "⌖" TYPE geometry;
--Testcase 51:
ALTER FOREIGN TABLE "♂" ALTER COLUMN "geom" TYPE geometry;
--Testcase 52:
SELECT * FROM "♂";

--Testcase 53:
CREATE FOREIGN TABLE "♁ FDW"(
	geom geometry NOT NULL,
	osm_type varchar(16) OPTIONS (key 'true') NOT NULL ,
	osm_id int OPTIONS (key 'true') NOT NULL,
	ver int NOT NULL,
	arr text,
	t jsonb
) SERVER sqlite_svr
OPTIONS (table '♁');

--Testcase 57: empty data set, no problems
SELECT * FROM "♁ FDW";

--Testcase 004:
DROP EXTENSION sqlite_fdw CASCADE;
