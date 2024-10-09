--
-- Schema usage, database attach test
--
--Testcase 01:
CREATE EXTENSION sqlite_fdw;
--Testcase 02:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER sqlite_fdw
OPTIONS (database '/tmp/sqlite_fdw_test/core.db');

--Testcase 03:
CREATE FOREIGN TABLE aggtest (
  a       int2,
  b     float4
) SERVER sqlite_svr;

--Testcase 04:
CREATE FOREIGN TABLE student (
  name    text,
  age     int4,
  location  point,
  gpa     float8
) SERVER sqlite_svr;

--Testcase 05:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 06:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', 'cdb');
--Testcase 07:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 08: -- ERR
SELECT sqlite_fdw_db_attach('sqlite_svr_non_exist', '/tmp/sqlite_fdw_test/common.db', 'scdb');
--Testcase 09: -- ERR
SELECT * FROM sqlite_fdw_db_list('sqlite_svr_non_exist');
--Testcase 10:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/non_exist.db', 'ndb');
--Testcase 11:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');

--Testcase 12:
CREATE FOREIGN TABLE "Unicode data"(i text OPTIONS (key 'true'), t text) SERVER sqlite_svr OPTIONS (schema_name 'cdb');
--Testcase 13:
SELECT * FROM "Unicode data";

--Testcase 14:
SELECT sqlite_fdw_db_detach('sqlite_svr', 'cdb');
--Testcase 15: -- ERR
SELECT * FROM "Unicode data";
--Testcase 16: -- ERR
SELECT sqlite_fdw_db_detach('sqlite_svr_non_exist', 'scdb');
--Testcase 17: -- ERR
SELECT sqlite_fdw_db_detach('sqlite_svr', 'ndb');
--Testcase 18:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');

--Testcase 19:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', 'cm');
--Testcase 20:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 21:
CREATE SCHEMA import1;
--Testcase 22:
IMPORT FOREIGN SCHEMA main FROM SERVER sqlite_svr INTO import1;
--Testcase 23:
\det+ import1.*
--Testcase 24:
CREATE SCHEMA import2;
--Testcase 25:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 26: -- fails, db was detached
IMPORT FOREIGN SCHEMA cm FROM SERVER sqlite_svr INTO import2;
--Testcase 27:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', 'cm');
--Testcase 28:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 29:
IMPORT FOREIGN SCHEMA cm FROM SERVER sqlite_svr INTO import2;
--Testcase 30:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 31:
\det+ import2.*

--Testcase 32:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 ');
--Testcase 33:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 34: -- normal, other alias
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 1');
--Testcase 35: -- ERR
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 1');

--Testcase 36:
SELECT sqlite_fdw_db_journal_mode('sqlite_svr', '試験 ') jm;
--Testcase 37:
SELECT sqlite_fdw_db_secure_delete('sqlite_svr', '試験 ') sd;
--Testcase 38:
SELECT sqlite_fdw_db_page_size('sqlite_svr', '試験 ') ps_b;
--Testcase 39:
SELECT sqlite_fdw_db_exclusive_locking_mode('sqlite_svr', '試験 ') elm;
--Testcase 40:
SELECT sqlite_fdw_db_auto_vacuum('sqlite_svr', '試験 ') av;
--Testcase 41:
SELECT sqlite_fdw_db_temp_store('sqlite_svr', '試験 ') tsm;
--Testcase 42:
SELECT sqlite_fdw_db_synchronous_mode('sqlite_svr', '試験 ') sm;

--Testcase 43:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'Unicode data', '試験 ');
--Testcase 44:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'RO_RW_test', '試験 ');
--Testcase 45:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'type_UUID+', '試験 ');
--Testcase 46:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'type_UUID', '試験 ');
--Testcase 47:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'BitT', '試験 ');
--Testcase 48:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'strict', '試験 ');
--Testcase 49:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'norowid', '試験 ');

--Testcase 50:
SELECT * FROM sqlite_fdw_rel_list('sqlite_svr', '試験 ');
--Testcase 51:
SELECT * FROM sqlite_fdw_index_list('sqlite_svr', 'Unicode data', '試験 ');
--Testcase 52:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'fts_table', '試験 ');
--Testcase 53:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'grem1_1', '試験 ');
--Testcase 54:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'grem1_2', '試験 ');
--Testcase 55:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'grem1_3', '試験 ');

--Testcase 56:
SELECT sqlite_fdw_db_application_id('sqlite_svr', '試験 ') ai;
--Testcase 57:
SELECT sqlite_fdw_db_cache_spill('sqlite_svr', '試験 ') dcs;
--Testcase 58:
SELECT sqlite_fdw_db_max_page_count('sqlite_svr', '試験 ') mpc;
--Testcase 59:
SELECT sqlite_fdw_db_journal_size_limit('sqlite_svr', '試験 ') jsl;
--Testcase 60:
SELECT sqlite_fdw_db_cache_size('sqlite_svr', '試験 ') cs;
--Testcase 61:
SELECT sqlite_fdw_db_tmp_directory('sqlite_svr', '試験 ') td;

--Testcase 62:
SELECT sqlite_fdw_db_user_version('sqlite_svr', '試験 ') uv;
--Testcase 63:
SELECT sqlite_fdw_db_schema_version('sqlite_svr', '試験 ') sv;
--Testcase 64:
SELECT * FROM sqlite_fdw_db_wal_checkpoint('sqlite_svr', '試験 ') walcp;

--Testcase 65:
SELECT sqlite_fdw_db_fkeys('sqlite_svr', '試験 ') fk;
--Testcase 66:
SELECT * FROM sqlite_fdw_db_fkeys_list('sqlite_svr', 'fk_d_track', '試験 ');
--Testcase 67:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 2', 'none');
--Testcase 68:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 69:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 3', 'quick');
--Testcase 70:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 72:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 4', 'full');
--Testcase 72:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 73:
SELECT sqlite_fdw_db_attach('sqlite_svr', '/tmp/sqlite_fdw_test/common.db', '試験 5', 'fkeys');
--Testcase 74:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');

--Testcase 75:
SELECT * FROM sqlite_fdw_db_fkeys_check('sqlite_svr', 'fk_d_track', '試験 4');

-- NON EXIST SERVER TESTS
--Testcase 150:
SELECT sqlite_fdw_db_attach('nonexist_svr', '/tmp/sqlite_fdw_test/common.db', '試験 ');
--Testcase 151:
SELECT * FROM sqlite_fdw_db_list('nonexist_svr');
--Testcase 152:
SELECT sqlite_fdw_db_attach('nonexist_svr', '/tmp/sqlite_fdw_test/common.db', '試験 1');

--Testcase 154:
SELECT sqlite_fdw_db_journal_mode('nonexist_svr', '試験 ') jm;
--Testcase 155:
SELECT sqlite_fdw_db_secure_delete('nonexist_svr', '試験 ') sd;
--Testcase 156:
SELECT sqlite_fdw_db_page_size('nonexist_svr', '試験 ') ps_b;
--Testcase 157:
SELECT sqlite_fdw_db_exclusive_locking_mode('nonexist_svr', '試験 ') elm;
--Testcase 158:
SELECT sqlite_fdw_db_auto_vacuum('nonexist_svr', '試験 ') av;
--Testcase 159:
SELECT sqlite_fdw_db_temp_store('nonexist_svr', '試験 ') tsm;
--Testcase 160:
SELECT sqlite_fdw_db_synchronous_mode('nonexist_svr', '試験 ') sm;

--Testcase 161:
SELECT * FROM sqlite_fdw_table_info('nonexist_svr', 'Unicode data', '試験 ');
--Testcase 162:
SELECT * FROM sqlite_fdw_rel_list('nonexist_svr', '試験 ');
--Testcase 163:
SELECT * FROM sqlite_fdw_index_list('nonexist_svr', 'Unicode data', '試験 ');
--Testcase 164:
SELECT * FROM sqlite_fdw_table_info('nonexist_svr', 'fts_table', '試験 ');
--Testcase 165:
SELECT * FROM sqlite_fdw_table_info('nonexist_svr', 'grem1_1', '試験 ');
--Testcase 166:
SELECT * FROM sqlite_fdw_table_info('nonexist_svr', 'grem1_2', '試験 ');
--Testcase 167:
SELECT * FROM sqlite_fdw_table_info('nonexist_svr', 'grem1_3', '試験 ');

--Testcase 168:
SELECT sqlite_fdw_db_application_id('nonexist_svr', '試験 ') ai;
--Testcase 169:
SELECT sqlite_fdw_db_cache_spill('nonexist_svr', '試験 ') dcs;
--Testcase 170:
SELECT sqlite_fdw_db_max_page_count('nonexist_svr', '試験 ') mpc;
--Testcase 171:
SELECT sqlite_fdw_db_journal_size_limit('nonexist_svr', '試験 ') jsl;
--Testcase 172:
SELECT sqlite_fdw_db_cache_size('nonexist_svr', '試験 ') cs;
--Testcase 173:
SELECT sqlite_fdw_db_tmp_directory('nonexist_svr', '試験 ') td;

--Testcase 174:
SELECT sqlite_fdw_db_user_version('nonexist_svr', '試験 ') uv;
--Testcase 175:
SELECT sqlite_fdw_db_schema_version('nonexist_svr', '試験 ') sv;
--Testcase 176:
SELECT * FROM sqlite_fdw_db_wal_checkpoint('nonexist_svr', '試験 ') walcp;

--Testcase 177:
SELECT sqlite_fdw_db_fkeys('nonexist_svr', '試験 ') fk;
--Testcase 178:
SELECT * FROM sqlite_fdw_db_fkeys_list('nonexist_svr', 'fk_d_track', '試験 ');
--Testcase 179:
SELECT sqlite_fdw_db_attach('nonexist_svr', '/tmp/sqlite_fdw_test/common.db', '試験 2', 'none');

--Testcase 180:
SELECT * FROM sqlite_fdw_db_fkeys_check('nonexist_svr', 'fk_d_track', '試験 4');

-- NON EXIST TABLE TEST
--Testcase 200:
SELECT sqlite_fdw_db_attach('sqlite_svr', 'notexist.db', 'notexist');
--Testcase 201:
SELECT * FROM sqlite_fdw_db_list('sqlite_svr');
--Testcase 202:
SELECT sqlite_fdw_db_journal_mode('sqlite_svr', 'notexist') jm;
--Testcase 203:
SELECT sqlite_fdw_db_secure_delete('sqlite_svr', 'notexist') sd;
--Testcase 204:
SELECT sqlite_fdw_db_page_size('sqlite_svr', 'notexist') ps_b;
--Testcase 205:
SELECT sqlite_fdw_db_exclusive_locking_mode('sqlite_svr', 'notexist') elm;
--Testcase 206:
SELECT sqlite_fdw_db_auto_vacuum('sqlite_svr', 'notexist') av;
--Testcase 207:
SELECT sqlite_fdw_db_temp_store('sqlite_svr', 'notexist') tsm;
--Testcase 208:
SELECT sqlite_fdw_db_synchronous_mode('sqlite_svr', 'notexist') sm;

--Testcase 209:
SELECT * FROM sqlite_fdw_table_info('sqlite_svr', 'Unicode data', 'notexist');

--Testcase 210:
SELECT * FROM sqlite_fdw_rel_list('sqlite_svr', 'notexist');
--Testcase 211:
SELECT * FROM sqlite_fdw_index_list('sqlite_svr', 'Unicode data', 'notexist ');
--Testcase 212:
SELECT sqlite_fdw_db_application_id('sqlite_svr', 'notexist') ai;
--Testcase 213:
SELECT sqlite_fdw_db_cache_spill('sqlite_svr', 'notexist') dcs;
--Testcase 214:
SELECT sqlite_fdw_db_max_page_count('sqlite_svr', 'notexist') mpc;
--Testcase 215:
SELECT sqlite_fdw_db_journal_size_limit('sqlite_svr', 'notexist') jsl;
--Testcase 216:
SELECT sqlite_fdw_db_cache_size('sqlite_svr', 'notexist') cs;
--Testcase 217:
SELECT sqlite_fdw_db_tmp_directory('sqlite_svr', 'notexist') td;

--Testcase 218:
SELECT sqlite_fdw_db_user_version('sqlite_svr', 'notexist') uv;
--Testcase 219:
SELECT sqlite_fdw_db_schema_version('sqlite_svr', 'notexist') sv;
--Testcase 220:
SELECT * FROM sqlite_fdw_db_wal_checkpoint('sqlite_svr', 'notexist') walcp;

--Testcase 221:
SELECT sqlite_fdw_db_fkeys('sqlite_svr', 'notexist') fk;
--Testcase 222:
SELECT * FROM sqlite_fdw_db_fkeys_list('sqlite_svr', 'fk_d_track', 'notexist');
--Testcase 223:
SELECT * FROM sqlite_fdw_db_fkeys_check('sqlite_svr', 'fk_d_track', 'notexist');

--Testcase 100:
DROP SERVER sqlite_svr CASCADE;
--Testcase 101:
DROP EXTENSION sqlite_fdw CASCADE;
