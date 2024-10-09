/*-------------------------------------------------------------------------
 *
 * SQLite Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "sqlite_fdw.h"
#include "limits.h"

#include "access/xact.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"


/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID
 */
typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	sqlite3	   *conn;			/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open, 2 =
								 * one level of subxact open, etc */
	char	   *dbpath;			/* Address of SQLite file in a file system*/
	bool		keep_connections;	/* setting value of keep_connections
									 * server option */
	bool		truncatable;	/* check table can truncate or not */
	bool		readonly;		/* option force_readonly, readonly SQLite file mode */
	char	   *integrity_check_mode;/* one of modes of database checks before opening a new connection */
	char	   *temp_store_directory; /* non-default directory address for temporary files like WAL etc. */
	bool		foreign_keys;	/* true if FDW should not destroy internal SQLite foreign keys during DML operations */
	bool		invalidated;	/* true if reconnect is pending */
	Oid			serverid;		/* foreign server OID used to get server name */
	List	   *stmtList;		/* list stmt associated with conn */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* tracks whether any work is needed in callback functions */
static volatile bool xact_got_connection = false;

/* connection management functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_get_connections);
PG_FUNCTION_INFO_V1(sqlite_fdw_disconnect);
PG_FUNCTION_INFO_V1(sqlite_fdw_disconnect_all);
/* schemas support */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_attach);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_detach);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_list);
/* SQLite database file metadata functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_encoding);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_schema_version);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_user_version);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_application_id);
/* metadata */
PG_FUNCTION_INFO_V1(sqlite_fdw_rel_list);
PG_FUNCTION_INFO_V1(sqlite_fdw_table_info);
PG_FUNCTION_INFO_V1(sqlite_fdw_index_list);
/* journal functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_journal_mode);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_journal_size_limit);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_synchronous_mode);
/* SQLite foreign keys functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_fkeys);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_fkeys_list);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_fkeys_check);
/* SQLite page functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_page_size);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_cache_spill);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_max_page_count);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_cache_size);
/* WAL and tmp storage functions */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_tmp_directory);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_temp_store);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_wal_checkpoint);

/* Other diagnostic PRAGMAs */
PG_FUNCTION_INFO_V1(sqlite_fdw_db_secure_delete);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_exclusive_locking_mode);
PG_FUNCTION_INFO_V1(sqlite_fdw_db_auto_vacuum);


static sqlite3 *sqlite_open_db(ConnCacheEntry *entry);
static void sqlite_make_new_connection(ConnCacheEntry *entry, ForeignServer *server);
static ConnCacheEntry * sqlite_get_conn_cache_entry(ForeignServer *server, bool truncatable);
void		sqlite_do_sql_command(sqlite3 * conn, const char *sql, int level, List **busy_connection);
static void sqlite_begin_remote_xact(ConnCacheEntry *entry);
static void sqlitefdw_xact_callback(XactEvent event, void *arg);
static void sqlitefdw_reset_xact_state(ConnCacheEntry *entry, bool toplevel);
static void sqlitefdw_subxact_callback(SubXactEvent event,
									   SubTransactionId mySubid,
									   SubTransactionId parentSubid,
									   void *arg);
static void sqlitefdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static void sqlitefdw_abort_cleanup(ConnCacheEntry *entry, bool toplevel, List **busy_connection);
#if PG_VERSION_NUM >= 140000
static bool sqlite_disconnect_cached_connections(Oid serverid);
#endif
static void sqlite_finalize_list_stmt(List **list);
static List *sqlite_append_stmt_to_list(List *list, sqlite3_stmt * stmt);

static int attach_sqlite_db (ForeignServer* server, sqlite3* conn, const char * dbadr, const Name alias);
static void detach_sqlite_db (ForeignServer* server, sqlite3* conn, const Name alias);

typedef struct BusyHandlerArg
{
	sqlite3	   *conn;
	const char *sql;
	int			level;
} BusyHandlerArg;

/*
 * one_string_value:
 *
 * Returns char* value from SQL commands, 1st column and 1st row. Uses for PRAGMA.
 */
static char*
one_string_value (sqlite3* db, char* query){
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	int				rc = SQLITE_ERROR;
	char			*reschar = NULL;

	sqlite_prepare_wrapper(NULL, db, query, (sqlite3_stmt * *) & pragma_stmt, NULL, false);
	for (;;)
	{
		rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc == SQLITE_ROW)
		{
			sqlite3_value  *resval = sqlite3_column_value(pragma_stmt, 0);
			int				vt = sqlite3_value_type(resval);
			int				vb = sqlite3_value_bytes(resval);
			if ( vt != SQLITE3_TEXT)
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"%s\" returns not a text, but %s (%d bytes). Please check your SQLite configuration",
					 		query,
					 		sqlite_datatype(vt),
					 		vb)));
			}
			else
			{
				char* t = (char*)sqlite3_value_text(resval);

				reschar = (char*)palloc(vb);
				strcpy(reschar, t);
			}
		}
		else
		{
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
	return reschar;
}

/*
 * one_integer_value:
 *
 * Returns integer value from SQL commands, 1st column and 1st row. Uses for PRAGMA.
 */
static int
one_integer_value (sqlite3* db, char* query){
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	int				rc = SQLITE_ERROR;
	int				resint = -1;

	sqlite_prepare_wrapper(NULL, db, query, (sqlite3_stmt * *) & pragma_stmt, NULL, false);
	for (;;)
	{
		rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc == SQLITE_ROW)
		{
			sqlite3_value  *resval = sqlite3_column_value(pragma_stmt, 0);
			int				vt = sqlite3_value_type(resval);
			int				vb = sqlite3_value_bytes(resval);
			if ( vt != SQLITE_INTEGER)
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"%s\" returns not integer, but %s (%d bytes). Please check your SQLite configuration",
					 		query,
					 		sqlite_datatype(vt),
					 		vb)));
			}
			else
			{
				resint = sqlite3_value_int(resval);
			}
		}
		else
		{
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
	return resint;
}

/*
 * CStringGetNameDatum:
 *
 * Make Name Datum from C string
 */
static Datum
CStringGetNameDatum (char * s)
{
	int			len = strlen(s);
	Name		n;
	/* Truncate oversize input */
	if (len >= NAMEDATALEN)
		len = pg_mbcliplen(s, len, NAMEDATALEN - 1);
	/* We use palloc0 here to ensure result is zero-padded */
	n = (Name) palloc0(NAMEDATALEN);
	memcpy(NameStr(*n), s, len);
	return NameGetDatum(n);
}

/*
 * sqlite_get_conn_cache_entry:
 * Get a connection cahce entry which can be used to ensure some options or get
 * connection of the remote SQLite server with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
ConnCacheEntry *
sqlite_get_conn_cache_entry(ForeignServer *server, bool truncatable)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);

		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("sqlite_fdw connections", 8,
									 &ctl,
#if (PG_VERSION_NUM >= 140000)
									 HASH_ELEM | HASH_BLOBS);
#else
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
#endif

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(sqlitefdw_xact_callback, NULL);
		RegisterSubXactCallback(sqlitefdw_subxact_callback, NULL);
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  sqlitefdw_inval_callback, (Datum) 0);
	}

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;

	key = server->serverid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* If can not find any cached entry => initialize new hashtable entry */
		entry->conn = NULL;
	}

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->conn != NULL && entry->invalidated && entry->xact_depth == 0)
	{
		int			rc = sqlite3_close(entry->conn);

		elog(DEBUG1, "closing connection %p for option changes to take effect. sqlite3_close=%d",
			 entry->conn, rc);
		entry->conn = NULL;
	}

	/*
	 * If cache entry doesn't have a connection, we have to establish a new
	 * connection.  (If sqlite_open_db has an error, the cache entry will
	 * remain in a valid empty state, ie conn == NULL.)
	 */
	if (entry->conn == NULL)
		sqlite_make_new_connection(entry, server);

	entry->truncatable = truncatable;

	/*
	 * SQLite FDW support TRUNCATE command by executing DELETE statement
	 * without WHERE clause. In order to delete records in parent and child
	 * table subsequently, SQLite FDW executes "PRAGMA foreign_keys = ON"
	 * before executing DELETE statement. But "PRAGMA foreign_keys = ON"
	 * command does not have any affect when using within transaction.
	 * Therefore, do not create transaction when executing TRUNCATE.
	 */
	if (!entry->truncatable)

		/*
		 * Start a new transaction or subtransaction if needed.
		 */
		sqlite_begin_remote_xact(entry);

	return entry;
}

/*
 * sqlite_get_connection:
 * Get a connection which can be used to execute queries on
 * the remote SQLite server with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
sqlite3 *
sqlite_get_connection(ForeignServer *server, bool truncatable)
{
	ConnCacheEntry *entry = sqlite_get_conn_cache_entry(server, truncatable);
	return entry->conn;
}

/* OPEN DATABASE GROUP */

/*
 * sqlite_check:
 *
 * Do SQLite integrity check
 */
static void
sqlite_check(sqlite3 *conn, const char *dbpath, const char *mode, const char *schema)
{
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	char		   *check_res = NULL;

	sprintf(stmt, "PRAGMA \"%s\".%s_check;", schema, mode);
	check_res = one_string_value (conn, stmt);
	if (strcmp(check_res, "ok") != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("%s check failed to for SQLite DB, file '%s': %s", mode, dbpath, check_res)));
	}
}

/*
 * integrity_checks:
 *
 * Validate integrity check mode string and runs requested integrity checks
 */
static void
integrity_checks(sqlite3 *conn, const char *dbadr, const char *integrity_check_mode,const char * schema)
{
	if (integrity_check_mode == NULL)
		return;
	validate_integrity_check_mode (integrity_check_mode);

	if (strcmp(integrity_check_mode, "quick") == 0)
		sqlite_check(conn, dbadr, "quick", schema);
	if (strcmp(integrity_check_mode, "full") == 0)
		sqlite_check(conn, dbadr, "integrity", schema);
	if (strcmp(integrity_check_mode, "fkeys") == 0)
	{
		sqlite_check(conn, dbadr, "integrity", schema);
	//	sqlite_check(conn, dbadr, "foreign_keys", schema);
	}
}

static void
open_db_fail (sqlite3 *conn, int rc, char *err, const char *dbpath, int step)
{
	char	   *perr = pstrdup(err);
	sqlite3_free(err);
	sqlite3_close(conn);
	conn = NULL;
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("Failed to open SQLite DB, file '%s', SQLite error '%s', result code %d, step %d", dbpath, perr, rc, step)));
}

/*
 * Open remote sqlite database using specified database path
 * and flags of opened file descriptor mode.
 */
static sqlite3 *
sqlite_open_db(ConnCacheEntry *entry)
{
	sqlite3	   *conn = NULL;
	int			rc;
	char	   *err;
	const char *zVfs = NULL;
	int			flags = 0;

	if (entry->dbpath == NULL)
		return NULL;
	flags = flags | (entry->readonly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE);
	rc = sqlite3_open_v2(entry->dbpath, &conn, flags, zVfs);
	if (rc != SQLITE_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("Failed to open SQLite DB, file '%s', result code %d", entry->dbpath, rc)));

	/* Do not check intergity in case of readonly mode */
	if (! entry->readonly)
		integrity_checks(conn, entry->dbpath, entry->integrity_check_mode, "main");
	/* Set not default temporary store directory */
	if (entry->temp_store_directory != NULL)
	{
		char		   *stmt = (char*)palloc0(PATH_MAX + 40);
		sprintf(stmt, "pragma temp_store_directory='%s';", entry->temp_store_directory);
		rc = sqlite3_exec(conn, stmt,
					  NULL, NULL, &err);
		pfree(stmt);
		if (rc != SQLITE_OK)
			open_db_fail(conn, rc, err, entry->dbpath, 3);
	}
	/* Enable SQLite internal foreign keys support */
	if (! entry->readonly && entry->foreign_keys)
	{
		rc = sqlite3_exec(conn, "pragma foreign_keys=1",
						  NULL, NULL, &err);
		if (rc != SQLITE_OK)
			open_db_fail(conn, rc, err, entry->dbpath, 4);
	}
	/* make 'LIKE' of SQLite case sensitive like PostgreSQL */
	rc = sqlite3_exec(conn, "pragma case_sensitive_like=1",
					  NULL, NULL, &err);
	if (rc != SQLITE_OK)
		open_db_fail(conn, rc, err, entry->dbpath, 5);
	/* add included inner SQLite functions from separate c file
	 * for using in data unifying during deparsing
	 */
	sqlite_fdw_data_norm_functs_init(conn);
	return conn;
}

/*
 * Reset all transient state fields in the cached connection entry and
 * establish new connection to the remote server.
 */
static void
sqlite_make_new_connection(ConnCacheEntry *entry, ForeignServer *server)
{
	ListCell   *lc;

	Assert(entry->conn == NULL);

	entry->serverid = server->serverid;
	entry->xact_depth = 0;
	entry->invalidated = false;
	entry->stmtList = NULL;
	entry->dbpath = NULL;
	entry->keep_connections = true;
	entry->readonly = false;
	entry->integrity_check_mode = NULL;
	entry->temp_store_directory = NULL;
	entry->foreign_keys = true;
	entry->server_hashvalue =
		GetSysCacheHashValue1(FOREIGNSERVEROID,
							  ObjectIdGetDatum(server->serverid));
	foreach(lc, server->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
			entry->dbpath = defGetString(def);
		else if (strcmp(def->defname, "keep_connections") == 0)
			entry->keep_connections = defGetBoolean(def);
		else if (strcmp(def->defname, "force_readonly") == 0)
			entry->readonly = defGetBoolean(def);
		else if (strcmp(def->defname, "integrity_check") == 0)
			entry->integrity_check_mode = defGetString(def);
		else if (strcmp(def->defname, "temp_store_directory") == 0)
			entry->temp_store_directory = defGetString(def);
		else if (strcmp(def->defname, "foreign_keys") == 0)
			entry->foreign_keys = defGetBoolean(def);
	}

	/* Try to make the connection */
	entry->conn = sqlite_open_db(entry);
}

/* End of open database group */

/*
 * cleanup_connection:
 * Delete all the cache entries on backend exists.
 */
void
sqlite_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	int			rc;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		sqlite_finalize_list_stmt(&entry->stmtList);

		elog(DEBUG1, "disconnecting sqlite_fdw connection %p", entry->conn);
		rc = sqlite3_close(entry->conn);
		entry->conn = NULL;
		if (rc != SQLITE_OK)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("Failed to close SQLite DB"),
					 errhint("SQLite error '%s', SQLite result code %d", sqlite3_errmsg(entry->conn), rc)
					));
		}
	}
}

/*
 * Convenience subroutine to issue a non-data-returning SQL command to remote
 */
void
sqlite_do_sql_command(sqlite3 * conn, const char *sql, int level, List **busy_connection)
{
	char	   *err = NULL;
	int			rc;

	elog(DEBUG3, "sqlite_fdw do_sql_command %s", sql);

	rc = sqlite3_exec(conn, sql, NULL, NULL, &err);

	if (busy_connection && rc == SQLITE_BUSY)
	{
		/* Busy case will be handled later, not here */
		BusyHandlerArg *arg = palloc0(sizeof(BusyHandlerArg));

		arg->conn = conn;
		arg->sql = sql;
		arg->level = level;
		*busy_connection = lappend(*busy_connection, arg);

		return;
	}

	if (rc != SQLITE_OK)
	{
		char	   *perr = NULL;

		if (err)
		{
			perr = pstrdup(err);
			sqlite3_free(err);

			if (perr)
			{
				ereport(level,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("SQLite failed to execute a query"),
						 errcontext("SQL query: %s", sql),
						 errhint("SQLite error '%s'", perr)));
				pfree(perr);
			}
		}
		else
			ereport(level,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("SQLite failed to execute a query"),
					 errcontext("SQL query: %s", sql)
					 ));
	}
}

/*
 * Start remote transaction or subtransaction, if needed.
 */
static void
sqlite_begin_remote_xact(ConnCacheEntry *entry)
{
	int			curlevel = GetCurrentTransactionNestLevel();

	/* Start main transaction if we haven't yet */
	if (entry->xact_depth <= 0)
	{
		const char *sql;

		elog(DEBUG3, "starting remote transaction on connection %p",
			 entry->conn);

		sql = "BEGIN";

		sqlite_do_sql_command(entry->conn, sql, ERROR, NULL);
		entry->xact_depth = 1;
	}

	/*
	 * If we're in a subtransaction, stack up savepoints to match our level.
	 * This ensures we can rollback just the desired effects when a
	 * subtransaction aborts.
	 */
	while (entry->xact_depth < curlevel)
	{
		char		sql[64];

		snprintf(sql, sizeof(sql), "SAVEPOINT s%d", entry->xact_depth + 1);
		sqlite_do_sql_command(entry->conn, sql, ERROR, NULL);
		entry->xact_depth++;
	}
}

/*
 * Report an SQLite execution error
 */
void
sqlitefdw_report_error(int elevel, sqlite3_stmt * stmt, sqlite3 * conn,
					   const char *sql, int rc)
{
	const char *message = sqlite3_errmsg(conn);
	int			sqlstate = ERRCODE_FDW_ERROR;

	/* copy sql before callling another SQLite API */
	if (message)
		message = pstrdup(message);

	if (!sql && stmt)
	{
		sql = sqlite3_sql(stmt);
		if (sql)
			sql = pstrdup(sqlite3_sql(stmt));
	}
	ereport(ERROR,
			(errcode(sqlstate),
			 errmsg("Failed to execute remote SQL"),
			 errcontext("SQL query: %s", sql ? sql : ""),
			 errhint("SQLite error '%s', SQLite result code %d", message ? message : "", rc)));
}

/* XACT GROUP */

/*
 * sqlitefdw_xact_callback --- cleanup at main-transaction end.
 */
static void
sqlitefdw_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	ListCell *lc;
	List *busy_connection = NIL;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	elog(DEBUG1, "sqlite_fdw xact_callback %d", event);

	/*
	 * Scan all connection cache entries to find open remote transactions, and
	 * close them.
	 */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry->conn == NULL)
			continue;

		/* If it has an open remote transaction, try to close it */
		if (entry->xact_depth > 0)
		{
			elog(DEBUG3, "closing remote transaction on connection %p",
				 entry->conn);

			switch (event)
			{
				case XACT_EVENT_PARALLEL_PRE_COMMIT:
				case XACT_EVENT_PRE_COMMIT:

					/* Commit all remote transactions during pre-commit */
					if (!sqlite3_get_autocommit(entry->conn))
						sqlite_do_sql_command(entry->conn, "COMMIT", ERROR, &busy_connection);
					/* Finalize all prepared statements */
					sqlite_finalize_list_stmt(&entry->stmtList);
					break;
				case XACT_EVENT_PRE_PREPARE:

					/*
					 * We disallow remote transactions that modified anything,
					 * since it's not very reasonable to hold them open until
					 * the prepared transaction is committed.  For the moment,
					 * throw error unconditionally; later we might allow
					 * read-only cases.  Note that the error will cause us to
					 * come right back here with event == XACT_EVENT_ABORT, so
					 * we'll clean up the connection state at that point.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot prepare a transaction that modified remote tables")));
					break;
				case XACT_EVENT_PARALLEL_COMMIT:
				case XACT_EVENT_COMMIT:
				case XACT_EVENT_PREPARE:
					/* Pre-commit should have closed the open transaction */
					elog(ERROR, "missed cleaning up connection during pre-commit");
					break;
				case XACT_EVENT_PARALLEL_ABORT:
				case XACT_EVENT_ABORT:
					{
						sqlitefdw_abort_cleanup(entry, true, &busy_connection);
						break;
					}
			}
		}

		/* Reset state to show we're out of a transaction */
		sqlitefdw_reset_xact_state(entry, true);
	}

	/* Execute again the query after server is available */
	foreach(lc, busy_connection)
	{
		BusyHandlerArg *arg = lfirst(lc);

		/*
		 * If there is still error, we can not do anything more, just raise it.
		 * requireBusyHandler is set to false, and NULL busy_connection list.
		 */
		sqlite_do_sql_command(arg->conn, arg->sql, arg->level, NULL);
	}

	list_free(busy_connection);

	/*
	 * Regardless of the event type, we can now mark ourselves as out of the
	 * transaction.  (Note: if we are here during PRE_COMMIT or PRE_PREPARE,
	 * this saves a useless scan of the hashtable during COMMIT or PREPARE.)
	 */
	xact_got_connection = false;
}

/*
 * sqlitefdw_reset_xact_state --- Reset state to show we're out of a (sub)transaction
 */
static void
sqlitefdw_reset_xact_state(ConnCacheEntry *entry, bool toplevel) {
	if (toplevel) {
		/* Reset state to show we're out of a transaction */
		entry->xact_depth = 0;

		/*
		 * If the connection isn't in a good idle state, it is marked as
		 * invalid or keep_connections option of its server is disabled, then
		 * discard it to recover. Next GetConnection will open a new
		 * connection.
		 */
		if (entry->invalidated ||
			!entry->keep_connections)
		{
			elog(DEBUG3, "discarding sqlite_fdw connection %p", entry->conn);
			sqlite3_close(entry->conn);
			entry->conn = NULL;
		}
	} else {
		/* Reset state to show we're out of a subtransaction */
		entry->xact_depth--;
	}
}
/*
 * sqlitefdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
sqlitefdw_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	int			curlevel;
	ListCell 	   *lc;
	List *busy_connection = NIL;

	/* Nothing to do at subxact start, nor after commit. */
	if (!(event == SUBXACT_EVENT_PRE_COMMIT_SUB ||
		  event == SUBXACT_EVENT_ABORT_SUB))
		return;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	/*
	 * Scan all connection cache entries to find open remote subtransactions
	 * of the current level, and close them.
	 */
	curlevel = GetCurrentTransactionNestLevel();
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		char		sql[100];

		/*
		 * We only care about connections with open remote subtransactions of
		 * the current level.
		 */
		if (entry->conn == NULL || entry->xact_depth < curlevel)
			continue;

		if (entry->truncatable)
			continue;

		if (entry->xact_depth > curlevel)
			elog(ERROR, "missed cleaning up remote subtransaction at level %d",
				 entry->xact_depth);

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			/* Commit all remote subtransactions during pre-commit */
			snprintf(sql, sizeof(sql), "RELEASE SAVEPOINT s%d", curlevel);
			sqlite_do_sql_command(entry->conn, sql, ERROR, &busy_connection);

		}
		else if (in_error_recursion_trouble())
		{
			/*
			 * Don't try to clean up the connection if we're already in error
			 * recursion trouble.
			 */
		}
		else
		{
			/* Rollback all remote subtransactions during abort */
			sqlitefdw_abort_cleanup(entry, false, &busy_connection);
		}

		/* OK, we're outta that level of subtransaction */
		sqlitefdw_reset_xact_state(entry, false);
	}

	/* Execute again the query after server is available */
	foreach(lc, busy_connection)
	{
		BusyHandlerArg *arg = lfirst(lc);

		/*
		 * If there is still error, we can not do anything more, just raise it.
		 * requireBusyHandler is set to false, and NULL busy_connection list.
		 */
		sqlite_do_sql_command(arg->conn, arg->sql, arg->level, NULL);
	}

	list_free(busy_connection);
}

/* End of xact group */

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade.
 * We can't immediately destroy them, since they might be in the midst of
 * a transaction, but we'll remake them at the next opportunity.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
sqlitefdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue))
		{
			/*
			 * Close the connection immediately if it's not used yet in this
			 * transaction. Otherwise mark it as invalid so that
			 * sqlitefdw_xact_callback() can close it at the end of this
			 * transaction.
			 */
			if (entry->xact_depth == 0)
			{
				elog(DEBUG3, "discarding sqlite_fdw connection %p", entry->conn);
				sqlite3_close(entry->conn);
				entry->conn = NULL;
			}
			else
				entry->invalidated = true;
		}
	}
}

/* CONNECTION MANAGEMENT GROUP */

/*
 * List active foreign server connections.
 *
 * This function takes no input parameter and returns setof record made of
 * following values:
 * - server_name - server name of active connection. In case the foreign server
 *   is dropped but still the connection is active, then the server name will
 *   be NULL in output.
 * - valid - true/false representing whether the connection is valid or not.
 * 	 Note that the connections can get invalidated in sqlitefdw_inval_callback.
 *
 * No records are returned when there are no cached connections at all.
 */
Datum
sqlite_fdw_get_connections(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function %s does not support in Postgres version %s", __func__, PG_VERSION)
			 ));
#else
#define SQLITE_FDW_GET_CONNECTIONS_COLS	2
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
#if PG_VERSION_NUM < 150000
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
#endif

#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	/* If cache doesn't exist, we return no records */
	if (!ConnectionHash)
	{
#if PG_VERSION_NUM < 150000
		/* clean up and return the tuplestore */
		tuplestore_donestoring(tupstore);
#endif

		PG_RETURN_VOID();
	}

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		ForeignServer *server;
		Datum		values[SQLITE_FDW_GET_CONNECTIONS_COLS] = {0};
		bool		nulls[SQLITE_FDW_GET_CONNECTIONS_COLS] = {0};

		/* We only look for open remote connections */
		if (!entry->conn)
			continue;

		server = GetForeignServerExtended(entry->serverid, FSV_MISSING_OK);

		/*
		 * The foreign server may have been dropped in current explicit
		 * transaction. It is not possible to drop the server from another
		 * session when the connection associated with it is in use in the
		 * current transaction, if tried so, the drop query in another session
		 * blocks until the current transaction finishes.
		 *
		 * Even though the server is dropped in the current transaction, the
		 * cache can still have associated active connection entry, say we
		 * call such connections dangling. Since we can not fetch the server
		 * name from system catalogs for dangling connections, instead we show
		 * NULL value for server name in output.
		 *
		 * We could have done better by storing the server name in the cache
		 * entry instead of server oid so that it could be used in the output.
		 * But the server name in each cache entry requires 64 bytes of
		 * memory, which is huge, when there are many cached connections and
		 * the use case i.e. dropping the foreign server within the explicit
		 * current transaction seems rare. So, we chose to show NULL value for
		 * server name in output.
		 *
		 * Such dangling connections get closed either in next use or at the
		 * end of current explicit transaction in sqlitefdw_xact_callback.
		 */
		if (!server)
		{
			/*
			 * If the server has been dropped in the current explicit
			 * transaction, then this entry would have been invalidated in
			 * sqlitefdw_inval_callback at the end of drop server command.
			 * Note that this connection would not have been closed in
			 * sqlitefdw_inval_callback because it is still being used in the
			 * current explicit transaction. So, assert that here.
			 */
			Assert(entry->conn && entry->xact_depth > 0 && entry->invalidated);

			/* Show null, if no server name was found */
			nulls[0] = true;
		}
		else
			values[0] = CStringGetTextDatum(server->servername);

		values[1] = BoolGetDatum(!entry->invalidated);
#if PG_VERSION_NUM >= 150000
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
	}

#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif

	PG_RETURN_VOID();
#endif
}

/*
 * Disconnect the specified cached connections.
 *
 * This function discards the open connections that are established by
 * sqlite_fdw from the local session to the foreign server with
 * the given name. Note that there can be multiple connections to
 * the given server using different user mappings. If the connections
 * are used in the current local transaction, they are not disconnected
 * and warning messages are reported. This function returns true
 * if it disconnects at least one connection, otherwise false. If no
 * foreign server with the given name is found, an error is reported.
 */
Datum
sqlite_fdw_disconnect(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function %s does not support in Postgres version %s", __func__, PG_VERSION)
			 ));
#else
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);

	PG_RETURN_BOOL(sqlite_disconnect_cached_connections(server->serverid));
#endif
}

/*
 * Disconnect all the cached connections.
 *
 * This function discards all the open connections that are established by
 * sqlite_fdw from the local session to the foreign servers.
 * If the connections are used in the current local transaction, they are
 * not disconnected and warning messages are reported. This function
 * returns true if it disconnects at least one connection, otherwise false.
 */
Datum
sqlite_fdw_disconnect_all(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 140000
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Function %s does not support in Postgres version %s", __func__, PG_VERSION)
			 ));
#else
	PG_RETURN_BOOL(sqlite_disconnect_cached_connections(InvalidOid));
#endif
}

/*
 * Abort remote transaction or subtransaction.
 *
 * "toplevel" should be set to true if toplevel (main) transaction is
 * rollbacked, false otherwise.
 */
static void
sqlitefdw_abort_cleanup(ConnCacheEntry *entry, bool toplevel, List **busy_connection)
{
	if (toplevel)
	{
		elog(DEBUG3, "abort transaction");

		/* Finalize all prepared statements */
		sqlite_finalize_list_stmt(&entry->stmtList);

		/*
		* rollback if in transaction because SQLite may
		* already rollback
		*/
		if (!sqlite3_get_autocommit(entry->conn))
			sqlite_do_sql_command(entry->conn, "ROLLBACK", WARNING, busy_connection);
	}
	else
	{
		char		sql[100];
		int			curlevel = GetCurrentTransactionNestLevel();
		snprintf(sql,
				 sizeof(sql),
				 "ROLLBACK TO SAVEPOINT s%d; RELEASE SAVEPOINT s%d",
				 curlevel,
				 curlevel
				);
		if (!sqlite3_get_autocommit(entry->conn))
			sqlite_do_sql_command(entry->conn, sql, ERROR, busy_connection);
	}
}

#if PG_VERSION_NUM >= 140000
/*
 * Workhorse to disconnect cached connections.
 *
 * This function scans all the connection cache entries and disconnects
 * the open connections whose foreign server OID matches with
 * the specified one. If InvalidOid is specified, it disconnects all
 * the cached connections.
 *
 * This function emits a warning for each connection that's used in
 * the current transaction and doesn't close it. It returns true if
 * it disconnects at least one connection, otherwise false.
 *
 * Note that this function disconnects even the connections that are
 * established by other users in the same local session using different
 * user mappings. This leads even non-superuser to be able to close
 * the connections established by superusers in the same local session.
 *
 * XXX As of now we don't see any security risk doing this. But we should
 * set some restrictions on that, for example, prevent non-superuser
 * from closing the connections established by superusers even
 * in the same session?
 */
static bool
sqlite_disconnect_cached_connections(Oid serverid)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	bool		all = !OidIsValid(serverid);
	bool		result = false;

	/*
	 * Connection cache hashtable has not been initialized yet in this
	 * session, so return false.
	 */
	if (!ConnectionHash)
		return false;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now. */
		if (!entry->conn)
			continue;

		if (all || entry->serverid == serverid)
		{
			/*
			 * Emit a warning because the connection to close is used in the
			 * current transaction and cannot be disconnected right now.
			 */
			if (entry->xact_depth > 0)
			{
				ForeignServer *server;

				server = GetForeignServerExtended(entry->serverid,
												  FSV_MISSING_OK);

				if (!server)
				{
					/*
					 * If the foreign server was dropped while its connection
					 * was used in the current transaction, the connection
					 * must have been marked as invalid by
					 * sqlitefdw_inval_callback at the end of DROP SERVER
					 * command.
					 */
					Assert(entry->invalidated);

					ereport(WARNING,
							(errmsg("cannot close dropped server connection because it is still in use")));
				}
				else
					ereport(WARNING,
							(errmsg("cannot close connection for server \"%s\" because it is still in use",
									server->servername)));
			}
			else
			{
				elog(DEBUG3, "discarding sqlite_fdw connection %p", entry->conn);
				sqlite_finalize_list_stmt(&entry->stmtList);
				sqlite3_close(entry->conn);
				entry->conn = NULL;
				result = true;
			}
		}
	}
	return result;
}
#endif

/* End of connection management group */

/*
 * cache sqlite3 statement to finalize at the end of transaction
 */
void
sqlite_cache_stmt(ForeignServer *server, sqlite3_stmt * *stmt)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key = server->serverid;

	/*
	 * Find cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);

	/* We must always have found the entry */
	Assert(found);

	entry->stmtList = sqlite_append_stmt_to_list(entry->stmtList, *stmt);
}

/*
 * finalize all sqlite statement
 */
static void
sqlite_finalize_list_stmt(List **list)
{
	ListCell   *lc;

	foreach(lc, *list)
	{
		sqlite3_stmt *stmt = (sqlite3_stmt *) lfirst(lc);

		elog(DEBUG1, "sqlite_fdw: finalize : %s", sqlite3_sql(stmt));
		sqlite3_finalize(stmt);
	}

	list_free(*list);
	*list = NULL;
}

/*
 * append sqlite3 stmt to the head of linked list
 */
static List *
sqlite_append_stmt_to_list(List *list, sqlite3_stmt * stmt)
{
	/*
	 * CurrentMemoryContext is released before cleanup transaction (when the
	 * list is called), so, use TopMemoryContext instead.
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	list = lappend(list, stmt);
	MemoryContextSwitchTo(oldcontext);
	return list;
}

/* ATTACHED DATABASES (SCHEMAS) GROUP */

/*
 * attach_sqlite_db
 *
 */
int
attach_sqlite_db (ForeignServer* server, sqlite3* conn, const char * dbadr, const Name alias)
{
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
	int				sqlite_id = -1;
	int				rc = SQLITE_OK;
	char		   *err;

	sprintf(stmt, "attach database '%s' as \"%.*s\"", dbadr, (int)sizeof(alias->data), alias->data);
	rc = sqlite3_exec(conn, stmt, NULL, NULL, &err);
	pfree(stmt);
	if (rc != SQLITE_OK)
	{
		char	   *perr = pstrdup(err);
		sqlite3_free(err);
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("Failed to attach SQLite DB, file '%s', SQLite error '%s', result code %d", dbadr, perr, rc)));
	}

	/* get sqlite_id, control internal SQLite success */
	sqlite_prepare_wrapper(server, conn, "PRAGMA database_list;", (sqlite3_stmt * *) & pragma_stmt, NULL, false);
	for (;;)
	{
		rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, conn, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
			sqlite3_value *val_db_id = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value *val_alias = sqlite3_column_value(pragma_stmt, 1);
			char		  *db_alias = NULL;

			if (sqlite3_value_type(val_db_id) == SQLITE_NULL ||
				sqlite3_value_type(val_alias) == SQLITE_NULL)
			{
				sqlite3_finalize(pragma_stmt);
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"PRAGMA database_list;\" returns NULL in a field, please check your SQLite configuration")));
			}
			db_alias = (char *)sqlite3_value_text(val_alias);
			if (strcmp(alias->data, db_alias) == 0)
			{
				sqlite_id = sqlite3_value_int(val_db_id);
				break;
			}
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
	return sqlite_id;
}

/*
 * sqlite_fdw_db_attach:
 * 		Attach outer database.
 */
Datum
sqlite_fdw_db_attach(PG_FUNCTION_ARGS)
{
	ConnCacheEntry *cce;
	ForeignServer  *server;
	Name			srvname = PG_GETARG_NAME(0);
	char		   *dbadr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Name			alias = PG_GETARG_NAME(2);
	Oid				userid = GetUserId();
	int				sqlite_id = -1;
	char		   *integrity_check_mode = text_to_cstring(PG_GETARG_TEXT_PP(3));

	elog(DEBUG1, "sqlite_fdw : %s attach db %s as \"%.*s\"", __func__, dbadr, (int)sizeof(alias->data), alias->data);
	server = GetForeignServerByName(srvname->data, false);

	/* Check if current user executing the function is foreign server owner or superuser */
	if (!superuser_arg(userid) && userid != server->owner)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("Only supersuser or owner of the foreign server can attach a schema!")));
	}

	cce = sqlite_get_conn_cache_entry(server, false);
	sqlite_id = attach_sqlite_db (server, cce->conn, dbadr, alias);
	integrity_checks(cce->conn, dbadr, integrity_check_mode, alias->data);
	PG_RETURN_INT32(sqlite_id);
}

/*
 * detach_sqlite_db
 *
 */
void
detach_sqlite_db (ForeignServer* server, sqlite3* conn, const Name alias)
{
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
	int				rc = SQLITE_OK;
	char		   *err;
	sprintf(stmt, "detach database \"%.*s\"", (int)sizeof(alias->data), alias->data);
	rc = sqlite3_exec(conn, stmt, NULL, NULL, &err);
	pfree(stmt);
	if (rc != SQLITE_OK)
	{
		char	   *perr = pstrdup(err);
		sqlite3_free(err);
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("Failed to detach SQLite DB %.*s, SQLite error '%s', result code %d", (int)sizeof(alias->data), alias->data, perr, rc)));
	}
}

/*
 * sqlite_fdw_db_detach:
 * 		Detach outer database.
 */
Datum
sqlite_fdw_db_detach(PG_FUNCTION_ARGS)
{
	ConnCacheEntry *cce;
	ForeignServer  *server;
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	Oid				userid = GetUserId();

	elog(DEBUG1, "sqlite_fdw : %s detach db as \"%.*s\"", __func__, (int)sizeof(alias->data), alias->data);
	server = GetForeignServerByName(srvname->data, false);

	/* Check if current user executing the function is foreign server owner or superuser */
	if (!superuser_arg(userid) && userid != server->owner)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("Only supersuser or owner of the foreign server can detach a schema!")));
	}

	cce = sqlite_get_conn_cache_entry(server, false);
	detach_sqlite_db (server, cce->conn, alias);
	PG_RETURN_VOID();
}

/*
 * sqlite_fdw_db_list:
 *
 * List all attached SQLite databases
 *
 */
Datum
sqlite_fdw_db_list(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_LIST_DB_COLS	5
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	int				rc = SQLITE_OK;
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);

#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sqlite_prepare_wrapper(server, db, "PRAGMA database_list;", (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum		values[SQLITE_FDW_LIST_DB_COLS] = {0};
		bool		nulls[SQLITE_FDW_LIST_DB_COLS] = {0};

		rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
			char		   *db_alias = NULL;
			char		   *fname = NULL;
			int				bRdonly;
			int				eTxn;
			sqlite3_value  *val_db_id = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_alias = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_file_addr = sqlite3_column_value(pragma_stmt, 2);

			if (sqlite3_value_type(val_db_id) == SQLITE_NULL ||
				sqlite3_value_type(val_alias) == SQLITE_NULL ||
				sqlite3_value_type(val_file_addr) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"PRAGMA database_list;\" returns NULL in a field, please check your SQLite configuration")));
			}

			/* sqlite_id int */
			values[0] = Int32GetDatum(sqlite3_value_int(val_db_id));

			/* alias name */
			db_alias = (char *) sqlite3_value_text(val_alias);
			values[1] = CStringGetNameDatum(db_alias);

			/* file text */
			fname = (char *)sqlite3_value_text(val_file_addr);
			values[2] = CStringGetTextDatum(fname);

			/* readonly bool */
			bRdonly = sqlite3_db_readonly(db, db_alias);
			if (bRdonly == -1)
			{
				nulls[3] = true;
			}
			else
			{
				values[3] = BoolGetDatum(bRdonly);
			}

			/*  txn varchar(5) */
			eTxn = sqlite3_txn_state(db, db_alias);
			values[4] = CStringGetTextDatum(
				eTxn == SQLITE_TXN_NONE ? "none" :
				eTxn == SQLITE_TXN_READ ? "read" :
				"write");
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/* End of attached databases (schemas) group */

/* GENERAL SQLITE DATABASE FILE METADATA GROUP */

/*
 * sqlite_fdw_db_encoding:
 *
 * Gets encoding of the SQLite database and all of attaced databases
 *
 */
Datum
sqlite_fdw_db_encoding(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *enc = one_string_value (db, "PRAGMA encoding;");

	if ( enc == NULL )
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(cstring_to_text(enc));
}

/*
 * sqlite_fdw_db_schema_version:
 *
 * Returns schema version - number of changes in database, something like LSN.
 *
 */
Datum
sqlite_fdw_db_schema_version(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".schema_version;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_user_version:
 *
 * Returns user version - integer for outer usage.
 *
 */
Datum
sqlite_fdw_db_user_version(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".user_version;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_application_id:
 *
 * Returns application id code - integer file subtype of database could means like OGC GeoPackage file, MBTiles tileset, TeXnicard file etc.
 *
 */
Datum
sqlite_fdw_db_application_id(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".application_id;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}
/* End of SQLite database file metadata group */

/* JOURNAL GROUP */

/*
 * sqlite_fdw_db_journal_mode:
 *
 * Returns journal mode of the SQLite database.
 *
 */
Datum
sqlite_fdw_db_journal_mode(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	char		   *res = NULL;

	sprintf(stmt, "PRAGMA \"%s\".journal_mode;", schema);
	res = one_string_value (db, stmt);
	if ( res == NULL )
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(cstring_to_text(res));
}

/*
 * sqlite_fdw_db_journal_size_limit:
 *
 * Returns journal size limit in bytes.
 *
 */
Datum
sqlite_fdw_db_journal_size_limit(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".journal_size_limit;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_synchronous_mode:
 *
 * Returns synchronous mode of the SQLite database.
 *
 */
Datum
sqlite_fdw_db_synchronous_mode(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".synchronous;", schema);
	res = one_integer_value (db, stmt);
	if ( res == 0 )
		PG_RETURN_TEXT_P(cstring_to_text("off"));
	if ( res == 1 )
		PG_RETURN_TEXT_P(cstring_to_text("on"));
	if ( res == 2 )
		PG_RETURN_TEXT_P(cstring_to_text("normal"));
	if ( res == 3 )
		PG_RETURN_TEXT_P(cstring_to_text("full"));
	if ( res == 4 )
		PG_RETURN_TEXT_P(cstring_to_text("extra"));
	PG_RETURN_NULL();
}

/* End of journal group */

/* METADATA INFO GROUP */

/*
 * sqlite_fdw_table_info:
 *
 * Returns formal table info from SQLite
 *
 */
Datum
sqlite_fdw_table_info(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_TABLE_INFO_COLS	7
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			table = PG_GETARG_NAME(1);
	Name			schema = PG_GETARG_NAME(2);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".table_xinfo (\"%s\");", schema->data, table->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_TABLE_INFO_COLS] = {0};
		bool			nulls[SQLITE_FDW_TABLE_INFO_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
/*	Comment from SQLite code:
 *	** cid:        Column id (numbered from left to right, starting at 0)
 *  ** name:       Column name
 *  ** type:       Column declaration type.
 *	** notnull:    True if 'NOT NULL' is part of column declaration
 *	** dflt_value: The default value for the column, if any.
 *	** pk:         Non-zero for PK fields.
 * +++ col_mode:  column implementation type, from xinfo */
			char		   *name = NULL;
			char		   *type = NULL;
			int				nn_int = -1;
			int				pk_int = -1;
			int				col_mode = -1;
			sqlite3_value  *val_cid = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_name = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_type = sqlite3_column_value(pragma_stmt, 2);
			sqlite3_value  *val_notnull = sqlite3_column_value(pragma_stmt, 3);
			sqlite3_value  *val_dflt_value = sqlite3_column_value(pragma_stmt, 4);
			sqlite3_value  *val_pk = sqlite3_column_value(pragma_stmt, 5);
			sqlite3_value  *val_col_mode = sqlite3_column_value(pragma_stmt, 6);

			if (sqlite3_value_type(val_cid) == SQLITE_NULL ||
				sqlite3_value_type(val_name) == SQLITE_NULL ||
				sqlite3_value_type(val_type) == SQLITE_NULL ||
				sqlite3_value_type(val_notnull) == SQLITE_NULL ||
				sqlite3_value_type(val_pk) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns NULL in a field, please check your SQLite configuration", stmt)));
			}

			/* cid:        Column id (numbered from left to right, starting at 0) */
			values[0] = Int32GetDatum(sqlite3_value_int(val_cid));
			/* name:       Column name */
			name = sqlite_text_value_to_pg_db_encoding(val_name);
			values[1] = CStringGetNameDatum(name);
			/* type:       Column declaration type. */
			type = sqlite_text_value_to_pg_db_encoding(val_type);
			if (strcmp(type, "") == 0)
				nulls[2] = true;
			else
				values[2] = CStringGetTextDatum(type);
			/* notnull:    True if 'NOT NULL' is part of column declaration */
			nn_int = sqlite3_value_int(val_notnull);
			values[3] = BoolGetDatum(nn_int == 1);
			/* dflt_value: The default value for the column, if any. */
			if (sqlite3_value_type(val_dflt_value) == SQLITE_NULL)
				nulls[4] = true;
			else
			{
				char * def = sqlite_text_value_to_pg_db_encoding(val_dflt_value);
				values[4] = CStringGetTextDatum(def);
			}
			/* pk:         Non-zero for PK fields. */
			pk_int = sqlite3_value_int(val_pk);
			values[5] = BoolGetDatum(pk_int == 1);
			/* normal column (0),
			 * a dynamic or stored generated column (2 or 3),
			 * or a hidden column in a virtual table (1).
			 */
			col_mode = sqlite3_value_int(val_col_mode);
			if (col_mode == 0)
				values[6] = CStringGetTextDatum("normal");
			else if (col_mode == 1)
				values[6] = CStringGetTextDatum("hidden+vtbl");
			else if (col_mode == 2)
				values[6] = CStringGetTextDatum("gen dynamic");
			else if (col_mode == 3)
				values[6] = CStringGetTextDatum("gen stored");
			else
				nulls[6] = true;
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/*
 * sqlite_fdw_rel_list:
 *
 * Returns list of relations in a SQLite database
 *
 */
Datum
sqlite_fdw_rel_list(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_REL_LIST_COLS	6
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			schema = PG_GETARG_NAME(1);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".table_list;", schema->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_REL_LIST_COLS] = {0};
		bool			nulls[SQLITE_FDW_REL_LIST_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
/*	Comment from SQLite code:
 * ** schema:     Name of attached database hold this table
 * ** name:       Name of the table itself
 * ** type:       "table", "view", "virtual", "shadow"
 * ** ncol:       Number of columns
 * ** wr:         True for a WITHOUT ROWID table
 * ** strict:     True for a STRICT table
 */
			char		   *schema_out = NULL;
			char		   *name = NULL;
			char		   *type = NULL;
			int				ncol = 0;
			int				wr_int = -1;
			int				strict_int = -1;
			sqlite3_value  *val_schema = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_name = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_type = sqlite3_column_value(pragma_stmt, 2);
			sqlite3_value  *val_ncol = sqlite3_column_value(pragma_stmt, 3);
			sqlite3_value  *val_wr = sqlite3_column_value(pragma_stmt, 4);
			sqlite3_value  *val_strict = sqlite3_column_value(pragma_stmt, 5);

			if (sqlite3_value_type(val_schema) == SQLITE_NULL ||
				sqlite3_value_type(val_name) == SQLITE_NULL ||
				sqlite3_value_type(val_type) == SQLITE_NULL ||
				sqlite3_value_type(val_ncol) == SQLITE_NULL ||
				sqlite3_value_type(val_wr) == SQLITE_NULL ||
				sqlite3_value_type(val_strict) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns NULL in a field, please check your SQLite configuration", stmt)));
			}

			/* schema:     Name of attached database hold this table */
			schema_out = sqlite_text_value_to_pg_db_encoding(val_schema);
			values[0] = CStringGetNameDatum(schema_out);
			/* name:       Name of the table itselfname: */
			name = sqlite_text_value_to_pg_db_encoding(val_name);
			values[1] = CStringGetNameDatum(name);
			/* type:       "table", "view", "virtual", "shadow" */
			type = sqlite_text_value_to_pg_db_encoding(val_type);
			if (strcmp(type, "") == 0)
				nulls[2] = true;
			else
				values[2] = CStringGetTextDatum(type);
			/* ncol:       Number of columns */
			ncol = sqlite3_value_int(val_ncol);
			values[3] = Int32GetDatum(ncol);
			/* wr:         True for a WITHOUT ROWID table */
			wr_int = sqlite3_value_int(val_wr);
			values[4] = BoolGetDatum(wr_int == 1);
			/* strict:     True for a STRICT table type: */
			strict_int = sqlite3_value_int(val_strict);
			values[5] = BoolGetDatum(strict_int == 1);
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/*
 * sqlite_fdw_index_list:
 *
 * Returns list of indexes in a SQLite database
 *
 */
Datum
sqlite_fdw_index_list(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_IDX_LIST_COLS	5
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			table = PG_GETARG_NAME(1);
	Name			schema = PG_GETARG_NAME(2);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".index_list (\"%s\");", schema->data, table->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_IDX_LIST_COLS] = {0};
		bool			nulls[SQLITE_FDW_IDX_LIST_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
/*	Comment from SQLite code:
 *		   i,
 *         pIdx->zName,
 *         IsUniqueIndex(pIdx),
 *         azOrigin[pIdx->idxType],
 *         pIdx->pPartIdxWhere!=0);
 */
			char		   *name = NULL;
			char		   *source = NULL;
			int				sqlite_id = -1;
			int				unique_int = -1;
			int				partial_int = -1;
			sqlite3_value  *val_sqlite_id = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_name = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_unique = sqlite3_column_value(pragma_stmt, 2);
			sqlite3_value  *val_source = sqlite3_column_value(pragma_stmt, 3);
			sqlite3_value  *val_partial = sqlite3_column_value(pragma_stmt, 4);

			if (sqlite3_value_type(val_sqlite_id) == SQLITE_NULL ||
				sqlite3_value_type(val_name) == SQLITE_NULL ||
				sqlite3_value_type(val_unique) == SQLITE_NULL ||
				sqlite3_value_type(val_source) == SQLITE_NULL ||
				sqlite3_value_type(val_partial) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns NULL in a field, please check your SQLite configuration", stmt)));
			}

			/* A sequence number assigned to each index for internal tracking purposes. */
			sqlite_id = sqlite3_value_int(val_sqlite_id);
			values[0] = Int32GetDatum(sqlite_id);
			/* The name of the index. */
			name = sqlite_text_value_to_pg_db_encoding(val_name);
			values[1] = CStringGetNameDatum(name);
			/*     "1" if the index is UNIQUE and "0" if not. */
			unique_int = sqlite3_value_int(val_unique);
			values[2] = BoolGetDatum(unique_int == 1);
			/* "c" if the index was created by a CREATE INDEX statement,
			 * "u" if the index was created by a UNIQUE constraint, or
			 * "pk" if the index was created by a PRIMARY KEY constraint.
			 */
			source = sqlite_text_value_to_pg_db_encoding(val_source);
			values[3] = CStringGetTextDatum(source);
			/*     "1" if the index is a partial index and "0" if not. */
			partial_int = sqlite3_value_int(val_partial);
			values[4] = BoolGetDatum(partial_int == 1);
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/* End of metadata info group */

/* SQLITE FOREIGN KEYS GROUP */

/*
 * sqlite_fdw_db_fk:
 *
 * Returns true if foreign keys is on.
 *
 */
Datum
sqlite_fdw_db_fkeys(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".foreign_keys;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_BOOL(res == 1);
}

/*
 * sqlite_fdw_foreign_key_check:
 *
 * Returns records not passing foreign key constaint
 *
 */
Datum
sqlite_fdw_db_fkeys_check(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_FK_CHECK_COLS	8
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			table = PG_GETARG_NAME(1);
	Name			schema = PG_GETARG_NAME(2);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".foreign_key_check(\"%s\");", schema->data, table->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_FK_CHECK_COLS] = {0};
		bool			nulls[SQLITE_FDW_FK_CHECK_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
			char		   *to = NULL;
			char		   *from = NULL;
			sqlite3_value  *val_table_to = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_rowid = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_table_from = sqlite3_column_value(pragma_stmt, 2);
			sqlite3_value  *val_i = sqlite3_column_value(pragma_stmt, 3);

			if (sqlite3_value_type(val_table_to) == SQLITE_NULL ||
				sqlite3_value_type(val_table_from) == SQLITE_NULL ||
				sqlite3_value_type(val_i) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns NULL in a field, please check your SQLite configuration", stmt)));
			}

			/* The first column is the name of the table that contains the REFERENCES clause. */
			to = sqlite_text_value_to_pg_db_encoding(val_table_to);
			values[0] = CStringGetNameDatum(to);
			/* The second column is the rowid of the row that contains the invalid REFERENCES clause, or NULL if the child table is a WITHOUT ROWID table. */
			if (sqlite3_value_type(val_rowid) == SQLITE_NULL)
				nulls[1] = true;
			else
				values[1] = Int64GetDatum(sqlite3_value_int64(val_rowid));
			/* The third column is the name of the table that is referred to. */
			from = sqlite_text_value_to_pg_db_encoding(val_table_from);
			values[2] = CStringGetNameDatum(from);
			/* The fourth column is the index of the specific foreign key constraint that failed. */
			/* The fourth column in the output of the foreign_key_check pragma is the same integer as the first column in the output of the foreign_key_list pragma. */
			values[3] = Int32GetDatum(sqlite3_value_int(val_i));
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/*
 * sqlite_fdw_db_fkeys_list:
 *
 * Returns description of foreign keys
 *
 */
Datum
sqlite_fdw_db_fkeys_list(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_FK_LIST_COLS	8
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN * 2 + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			table = PG_GETARG_NAME(1);
	Name			schema = PG_GETARG_NAME(2);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".foreign_key_list(\"%s\");", schema->data, table->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_FK_LIST_COLS] = {0};
		bool			nulls[SQLITE_FDW_FK_LIST_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
			char		   *to = NULL;
			char		   *col = NULL;
			char		   *constrt = NULL;
			char		   *onupd = NULL;
			char		   *ondel = NULL;
			char		   *f8 = NULL;
			sqlite3_value  *val_i = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_j = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_to = sqlite3_column_value(pragma_stmt, 2);
			sqlite3_value  *val_col = sqlite3_column_value(pragma_stmt, 3);
			sqlite3_value  *val_constrt = sqlite3_column_value(pragma_stmt, 4);
			sqlite3_value  *val_onupd = sqlite3_column_value(pragma_stmt, 5);
			sqlite3_value  *val_ondel = sqlite3_column_value(pragma_stmt, 6);
			sqlite3_value  *val_8 = sqlite3_column_value(pragma_stmt, 7);

			if (sqlite3_value_type(val_i) == SQLITE_NULL ||
				sqlite3_value_type(val_j) == SQLITE_NULL ||
				sqlite3_value_type(val_to) == SQLITE_NULL ||
				sqlite3_value_type(val_col) == SQLITE_NULL ||
				sqlite3_value_type(val_constrt) == SQLITE_NULL ||
				sqlite3_value_type(val_onupd) == SQLITE_NULL ||
				sqlite3_value_type(val_ondel) == SQLITE_NULL ||
				sqlite3_value_type(val_8) == SQLITE_NULL)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns NULL in a field, please check your SQLite configuration", stmt)));
			}

			values[0] = Int32GetDatum(sqlite3_value_int(val_i));
			values[1] = Int32GetDatum(sqlite3_value_int(val_j));

			to = sqlite_text_value_to_pg_db_encoding(val_to);
			values[2] = CStringGetNameDatum(to);

			col = sqlite_text_value_to_pg_db_encoding(val_col);
			values[3] = CStringGetNameDatum(col);

			constrt = sqlite_text_value_to_pg_db_encoding(val_constrt);
			values[4] = CStringGetNameDatum(constrt);

			onupd = sqlite_text_value_to_pg_db_encoding(val_onupd);
			values[5] = CStringGetTextDatum(onupd);

			ondel = sqlite_text_value_to_pg_db_encoding(val_ondel);
			values[6] = CStringGetTextDatum(ondel);

			f8 = sqlite_text_value_to_pg_db_encoding(val_8);
			values[7] = CStringGetTextDatum(f8);
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/* End of SQLite foreign keys group */

/* SQLITE PAGE BLOCK */

/*
 * sqlite_fdw_db_page_size:
 *
 * Returns page size of the SQLite database in bytes.
 *
 */
Datum
sqlite_fdw_db_page_size(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".page_size;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_cache_spill:
 *
 * Returns cache spill in pages.
 *
 */
Datum
sqlite_fdw_db_cache_spill(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".cache_spill;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_max_page_count:
 *
 * Returns maximal page count in database in pages.
 *
 */
Datum
sqlite_fdw_db_max_page_count(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".max_page_count;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_cache_size:
 *
 * Returns cache size in pages.
 *
 */
Datum
sqlite_fdw_db_cache_size(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".cache_size;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/* End of SQLite page group */

/* WAL AND TEMPORARY STORAGE GROUP */

/*
 * sqlite_fdw_db_temp_store:
 *
 * Returns temp storage mode of the SQLite database.
 *
 */
Datum
sqlite_fdw_db_temp_store(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".temp_store;", schema);
	res = one_integer_value (db, stmt);
	if ( res == 0 )
		PG_RETURN_TEXT_P(cstring_to_text("default"));
	if ( res == 1 )
		PG_RETURN_TEXT_P(cstring_to_text("memory"));
	if ( res == 2 )
		PG_RETURN_TEXT_P(cstring_to_text("file"));
	PG_RETURN_NULL();
}

/*
 * sqlite_fdw_db_tmp_directory:
 *
 * Returns temporary storage directory if not default.
 *
 */
Datum
sqlite_fdw_db_tmp_directory(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	char		   *res = NULL;

	sprintf(stmt, "PRAGMA \"%s\".temp_store_directory;", schema);
	res = one_string_value (db, stmt);
	if ( res == NULL )
		PG_RETURN_NULL();
	else
	{
		PG_RETURN_TEXT_P(res);
	}
}

/*
 * sqlite_fdw_db_wal_checkpoint:
 *
 * Returns WAL checkpoint mode
 *
 */
Datum
sqlite_fdw_db_wal_checkpoint(PG_FUNCTION_ARGS)
{
#define SQLITE_FDW_WALCP_COLS	3
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	sqlite3_stmt   *volatile pragma_stmt = NULL;
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
#if PG_VERSION_NUM < 150000
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
#endif
	Name			srvname = PG_GETARG_NAME(0);
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	Name			schema = PG_GETARG_NAME(1);
#if PG_VERSION_NUM >= 160000
	InitMaterializedSRF(fcinfo, 0);
#elif PG_VERSION_NUM >= 150000
	SetSingleFuncCall(fcinfo, 0);
#else
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);
#endif

	sprintf(stmt, "PRAGMA \"%s\".wal_checkpoint;", schema->data);
	sqlite_prepare_wrapper(server, db, stmt, (sqlite3_stmt * *) & pragma_stmt, NULL, false);

	for (;;)
	{
		Datum			values[SQLITE_FDW_WALCP_COLS] = {0};
		bool			nulls[SQLITE_FDW_WALCP_COLS] = {0};
		int				rc = sqlite3_step(pragma_stmt);
		if (rc == SQLITE_DONE)
			break;
		else if (rc != SQLITE_ROW)
		{
			/* Not pass sql_stmt because it is finalized in PG_CATCH */
			sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
		}
		else
		{
			sqlite3_value  *val_blk_flg = sqlite3_column_value(pragma_stmt, 0);
			sqlite3_value  *val_mpw = sqlite3_column_value(pragma_stmt, 1);
			sqlite3_value  *val_npsmbdbf = sqlite3_column_value(pragma_stmt, 2);
			int				blk_flg = -1;
			int				mpw = -1;
			int				npsmbdbf = -1;

			if (sqlite3_value_type(val_blk_flg) != SQLITE_INTEGER ||
				sqlite3_value_type(val_mpw) != SQLITE_INTEGER ||
				sqlite3_value_type(val_npsmbdbf) != SQLITE_INTEGER)
			{
				ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" returns not integer field, please check your SQLite configuration", stmt)));
			}

			/* 0 but will be 1 if a RESTART or FULL or TRUNCATE checkpoint was blocked from completing, for example because another thread or process was actively using the database. In other words, the first column is 0 if the equivalent call to sqlite3_wal_checkpoint_v2() would have returned SQLITE_OK or 1 if the equivalent call would have returned SQLITE_BUSY. */
			blk_flg = sqlite3_value_int(val_blk_flg);
			values[0] = BoolGetDatum(blk_flg == 1);
			/* The second column is the number of modified pages that have been written to the write-ahead log file. */
			mpw = sqlite3_value_int(val_mpw);
			if ( mpw == -1 )
				nulls[1] = true;
			else
				values[1] = Int32GetDatum(mpw);
			/* The third column is the number of pages in the write-ahead log file that have been successfully moved back into the database file at the conclusion of the checkpoint. */
			npsmbdbf = sqlite3_value_int(val_npsmbdbf);
			if ( npsmbdbf == -1 )
				nulls[2] = true;
			else
				values[2] = Int32GetDatum(npsmbdbf);
#if PG_VERSION_NUM >= 150000
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
		}
	}
	sqlite3_finalize(pragma_stmt);
	pragma_stmt = NULL;
#if PG_VERSION_NUM < 150000
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
#endif
	PG_RETURN_VOID();
}

/* End of WAL and temporary storage group */

/*
 * sqlite_fdw_db_secure_delete:
 *
 * Returns secure delete  mode of the SQLite database.
 *
 */
Datum
sqlite_fdw_db_secure_delete(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".secure_delete;", schema);
	res = one_integer_value (db, stmt);
	if ( res == -1 )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(res);
}

/*
 * sqlite_fdw_db_exclusive_locking_mode:
 *
 * Returns true is case of exclusive locking mode or false in case of normal.
 *
 */
Datum
sqlite_fdw_db_exclusive_locking_mode(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	char		   *res = NULL;

	sprintf(stmt, "PRAGMA \"%s\".locking_mode;", schema);
	res = one_string_value (db, stmt);
	if ( res == NULL )
		PG_RETURN_NULL();
	else
	{
		bool exc = strcmp(res, "exclusive") == 0;
		pfree(res);
		PG_RETURN_BOOL(exc);
	}
}

/*
 * sqlite_fdw_db_auto_vacuum:
 *
 * Returns auto vacuum mode of the SQLite database.
 *
 */
Datum
sqlite_fdw_db_auto_vacuum(PG_FUNCTION_ARGS)
{
	Name			srvname = PG_GETARG_NAME(0);
	Name			alias = PG_GETARG_NAME(1);
	char*			schema = alias->data;
	ForeignServer  *server = GetForeignServerByName(srvname->data, false);
	sqlite3		   *db = sqlite_get_connection(server, false);
	char		   *stmt = (char*)palloc0(NAMEDATALEN + 32);
	int		  	 	res = -1;

	sprintf(stmt, "PRAGMA \"%s\".auto_vacuum;", schema);
	res = one_integer_value (db, stmt);
	if ( res == 0 )
		PG_RETURN_TEXT_P(cstring_to_text("none"));
	if ( res == 1 )
		PG_RETURN_TEXT_P(cstring_to_text("full"));
	if ( res == 2 )
		PG_RETURN_TEXT_P(cstring_to_text("incremental"));
	PG_RETURN_NULL();
}

