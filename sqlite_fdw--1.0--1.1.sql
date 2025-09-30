/* contrib/sqlite_fdw/sqlite_fdw--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION sqlite_fdw UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION sqlite_fdw_get_connections (OUT server_name text,
    OUT valid boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION sqlite_fdw_disconnect (name)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

COMMENT ON FUNCTION sqlite_fdw_disconnect(name)
IS 'closes a SQLite connection by name of FOREIGN SERVER';

CREATE FUNCTION sqlite_fdw_disconnect_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

COMMENT ON FUNCTION sqlite_fdw_disconnect_all()
IS 'closes all opened SQLite connections';

COMMENT ON FUNCTION sqlite_fdw_handler()
IS 'SQLite foreign data wrapper handler';

COMMENT ON FUNCTION sqlite_fdw_validator(text[], oid)
IS 'SQLite foreign data wrapper options validator';

COMMENT ON FOREIGN DATA WRAPPER sqlite_fdw
IS 'SQLite foreign data wrapper';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_attach("server" name, sqlite_db_string text, sqlite_db_alias name, integrity_check_mode text default 'full')
RETURNS int STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
COMMENT ON FUNCTION sqlite_fdw_db_attach(name, text, name, text)
IS 'Attach SQLite database file as additional remote schema for PosrgreSQL foreign server';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_detach("server" name, sqlite_db_alias name)
RETURNS void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
COMMENT ON FUNCTION sqlite_fdw_db_detach(name, name)
IS 'Detach SQLite database file as additional remote schema from PosrgreSQL foreign server';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_list("server" name)
RETURNS table(sqlite_id int, alias name, file text, readonly bool, txn varchar(5))
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_list("server" name)
IS 'List all avaliable SQLite databases for PosrgreSQL foreign server';

CREATE OR REPLACE FUNCTION sqlite_fdw_mem()
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_mem()
IS 'Returns SQLite outstanding memory in bytes';

CREATE OR REPLACE FUNCTION sqlite_fdw_sqlite_version()
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_sqlite_version()
IS 'Returns used SQLite code version';

CREATE OR REPLACE FUNCTION sqlite_fdw_sqlite_code_source()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_sqlite_code_source()
IS 'Returns used SQLite code source with commit point';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_encoding("server" name)
RETURNS varchar(8)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_encoding("server" name)
IS 'Returns encoding of the SQLite database and all of attaced databases';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_journal_mode("server" name, "schema" name default 'main')
RETURNS varchar(16)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_journal_mode("server" name, "schema" name)
IS 'Returns journal mode of the SQLite database';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_secure_delete("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_secure_delete("server" name, "schema" name)
IS 'Returns secure delete mode of the SQLite database';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_page_size("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_page_size("server" name, "schema" name)
IS 'Returns page size of the SQLite database in bytes';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_exclusive_locking_mode("server" name, "schema" name default 'main')
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_exclusive_locking_mode("server" name, "schema" name)
IS 'Returns true is case of exclusive locking mode or false in case of normal';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_auto_vacuum("server" name, "schema" name default 'main')
RETURNS varchar(16)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_auto_vacuum("server" name, "schema" name)
IS 'Returns auto vacuum mode of the SQLite database';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_temp_store("server" name, "schema" name default 'main')
RETURNS varchar(16)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_temp_store("server" name, "schema" name)
IS 'Returns temporary storage mode of the SQLite database.';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_synchronous_mode("server" name, "schema" name default 'main')
RETURNS varchar(16)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_synchronous_mode("server" name, "schema" name)
IS 'Returns synchronous mode of the SQLite database.';

CREATE OR REPLACE FUNCTION sqlite_fdw_table_info("server" name, "table" name, "schema" name default 'main')
RETURNS table("sqlite_id" int, "column" name, "type" text, "notnull" bool, "default_value" text, "pk" bool, "column_mode" varchar(16))
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_table_info("server" name, "table" name, "schema" name)
IS 'Returns formal table info from SQLite';

CREATE OR REPLACE FUNCTION sqlite_fdw_rel_list("server" name, "schema" name default 'main')
RETURNS table("schema" name, "table" name, "type" text, "ncol" int, "no_rowid" bool, "strict" bool)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_rel_list("server" name, "schema" name)
IS 'Returns list of relations in a SQLite database';

CREATE OR REPLACE FUNCTION sqlite_fdw_index_list("server" name, "table" name, "schema" name default 'main')
RETURNS table("sqlite_id" int, "index_name" name, "unique" bool, "source" varchar(2), "partial" bool)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_index_list("server" name, "table" name, "schema" name)
IS 'Returns index metadata from SQLite';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_application_id("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_application_id("server" name, "schema" name)
IS 'Returns application id code - integer file subtype of database could means like OGC GeoPackage file, MBTiles tileset, TeXnicard file etc.';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_cache_spill("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_cache_spill("server" name, "schema" name)
IS 'Returns cache spill in pages';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_max_page_count("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_max_page_count("server" name, "schema" name)
IS 'Returns maximal page count in database in pages';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_journal_size_limit("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_journal_size_limit("server" name, "schema" name)
IS 'Returns journal size limit in bytes.';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_cache_size("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_cache_size("server" name, "schema" name)
IS 'Returns cache size in pages';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_tmp_directory("server" name, "schema" name default 'main')
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_tmp_directory("server" name, "schema" name)
IS 'Returns temporary storage directory if not default';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_schema_version("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_schema_version("server" name, "schema" name)
IS 'Returns schema version - number of changes in database, something like LSN';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_user_version("server" name, "schema" name default 'main')
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_user_version("server" name, "schema" name)
IS 'Returns user version value of database - integer for outer usage';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_wal_checkpoint("server" name, "schema" name default 'main')
RETURNS table("blocked" bool, "mod_pages_in_wal" int, "pages_suc_wr_to_db" int)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_wal_checkpoint("server" name, "schema" name)
IS 'Blocked from end WAL, number of modified pages written to WAL file, pages in WAL successfully moved into DB';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_fkeys("server" name, "schema" name default 'main')
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_fkeys("server" name, "schema" name)
IS 'Returns true if foreign keys is on';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_fkeys_list("server" name, "table" name, "schema" name default 'main')
RETURNS table(i int, j int, "source_table" name, "column_name" name, "constraint" name, onupd text, ondel text, f8 text)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_fkeys_list("server" name, "table" name, "schema" name)
IS '';

CREATE OR REPLACE FUNCTION sqlite_fdw_db_fkeys_check("server" name, "table" name, "schema" name default 'main')
RETURNS table(source_table name, "rowid" bigint, fk_from_table name, i int)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

COMMENT ON FUNCTION sqlite_fdw_db_fkeys_check("server" name, "table" name, "schema" name)
IS 'Return information about data which is not compatible to declared foreign keys in a table';
