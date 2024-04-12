/*-------------------------------------------------------------------------
 *
 * SQLite Foreign Data Wrapper for PostgreSQL
 *
 * SQLite functions for data normalization
 * This function is useful for mixed affinity inputs for PostgreSQL
 * data column. Also some UUID functions are implemented here according
 * the uuid SQLite exension, Public Domain
 * https://www.sqlite.org/src/file/ext/misc/uuid.c
 *
 * IDENTIFICATION
 * 		sqlite_data_norm.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * This SQLite extension implements functions that handling RFC-4122 UUIDs
 * Three SQL functions are implemented:
 *
 *	 gen_random_uuid() - generate a version 4 UUID as a string
 *	 uuid_str(X)	   - convert a UUID X into a well-formed UUID string
 *	 uuid_blob(X)	  - convert a UUID X into a 16-byte blob
 *
 * The output from gen_random_uuid() and uuid_str(X) are always well-formed
 * RFC-4122 UUID strings in this format:
 *
 *		xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
 *
 * All of the 'x', 'M', and 'N' values are lower-case hexadecimal digits.
 * The M digit indicates the "version".  For uuid4()-generated UUIDs, the
 * version is always "4" (a random UUID).  The upper three bits of N digit
 * are the "variant".  This library only supports variant 1 (indicated
 * by values of N between '8' and 'b') as those are overwhelming the most
 * common.  Other variants are for legacy compatibility only.
 *
 * The output of uuid_blob(X) is always a 16-byte blob. The UUID input
 * string is converted in network byte order (big-endian) in accordance
 * with RFC-4122 specifications for variant-1 UUIDs.  Note that network
 * byte order is *always* used, even if the input self-identifies as a
 * variant-2 UUID.
 *
 * The input X to the uuid_str() and uuid_blob() functions can be either
 * a string or a BLOB. If it is a BLOB it must be exactly 16 bytes in
 * length or else a NULL is returned.  If the input is a string it must
 * consist of 32 hexadecimal digits, upper or lower case, optionally
 * surrounded by {...} and with optional "-" characters interposed in the
 * middle.  The flexibility of input is inspired by the PostgreSQL
 * implementation of UUID functions that accept in all of the following
 * formats:
 *
 *	 A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11
 *	 {a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}
 *	 a0eebc999c0b4ef8bb6d6bb9bd380a11
 *	 a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11
 *	 {a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}
 *
 * If any of the above inputs are passed into uuid_str(), the output will
 * always be in the canonical RFC-4122 format:
 *
 *	 a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
 *
 * If the X input string has too few or too many digits or contains
 * stray characters other than {, }, or -, then NULL is returned.
 */
#include <assert.h>
#include <ctype.h>
#include <string.h>

#include "sqlite3.h"
#include "postgres.h"
#include "sqlite_fdw.h"
#include "utils/uuid.h"
#include "utils/inet.h"

static void error_helper(sqlite3* db, int rc);

#if !defined(SQLITE_ASCII) && !defined(SQLITE_EBCDIC)
#define SQLITE_ASCII 1
#endif

/*
 * Translate a single byte of Hex into an integer.
 * This routine only works if h really is a valid hexadecimal
 * character:  0..9a..fA..F
 */
static unsigned char
sqlite_fdw_data_norm_UuidHexToInt(int h)
{
	assert((h >= '0' && h <= '9') || (h >= 'a' && h <= 'f') || (h >= 'A' && h <= 'F'));
#ifdef SQLITE_ASCII
	h += 9 * (1 & (h >> 6));
#endif
#ifdef SQLITE_EBCDIC
	h += 9 * (1 & ~(h >> 4));
#endif
	return (unsigned char)(h & 0xf);
}

/*
 * Attempt to parse a zero-terminated input string zs into a binary
 * UUID.  Return 1 on success, or 0 if the input string is not
 * parsable.
 */
static int
sqlite_fdw_uuid_blob (const unsigned char* s0, unsigned char* Blob)
{
	int i;
	unsigned char* s = (unsigned char*)s0;
	if (s[0] == '{')
		s++;
	for (i = 0; i < 16; i++)
	{
		if (s[0] == '-')
			s++;
		if (isxdigit(s[0]) && isxdigit(s[1]))
		{
			Blob[i] = (sqlite_fdw_data_norm_UuidHexToInt(s[0]) << 4) + sqlite_fdw_data_norm_UuidHexToInt(s[1]);
			s += 2;
		}
		else
		{
			return 0;
		}
	}
	if (s[0] == '}')
		s++;
	return s[0] == 0;
}

/*
 * uuid_generate generates a version 4 UUID as a string
 *
 *static void uuid_generate(sqlite3_context* context, int argc, sqlite3_value** argv)
 *{
 *   unsigned char aBlob[16];
 *   unsigned char zs[37];
 *   sqlite3_randomness(16, aBlob);
 *   aBlob[6] = (aBlob[6] & 0x0f) + 0x40;
 *   aBlob[8] = (aBlob[8] & 0x3f) + 0x80;
 *   sqlite_fdw_data_norm_uuid_blob_to_str(aBlob, zs);
 *   sqlite3_result_text(context, (char*)zs, 36, SQLITE_TRANSIENT);
 *}
 */

/*
 * aBlob to RFC UUID string with 36 characters
 */

static void
sqlite3UuidBlobToStr( const unsigned char *aBlob, unsigned char *zs)
{
	static const char hex_dig[] = "0123456789abcdef";
	int i, k;
	unsigned char x;
	k = 0;
	for(i=0, k=0x550; i<UUID_LEN; i++, k=k>>1)
	{
		if( k&1 )
		{
			zs[0] = '-';
			zs++;
		}
		x = aBlob[i];
		zs[0] = hex_dig[x>>4];
		zs[1] = hex_dig[x&0xf];
		zs += 2;
	}
	*zs = 0;
}

/*
 * Converts argument BLOB-UUID into a well-formed UUID string.
 * X can be either a string or a blob.
 */
static void
sqlite_fdw_uuid_str(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	unsigned char aBlob[UUID_LEN];
	const unsigned char* pBlob;
	unsigned char zs[UUID_LEN * 2 + 1];
	sqlite3_value* arg = argv[0];
	int t = sqlite3_value_type(arg);

	if (t == SQLITE_BLOB)
	{
		pBlob = sqlite3_value_blob(arg);
	}
	if (t == SQLITE3_TEXT)
	{
		const unsigned char* txt = sqlite3_value_text(arg);
		if (sqlite_fdw_uuid_blob(txt, aBlob))
			pBlob = aBlob;
		else
		{
			sqlite3_result_null(context);
			return;
		}
	}
	if (t != SQLITE_BLOB)
	{
		sqlite3_result_null(context);
		return;
	}

	sqlite3UuidBlobToStr(pBlob, zs);
	sqlite3_result_text(context, (char*)zs, 36, SQLITE_TRANSIENT);
}

/*
 * sqlite_fdw_data_norm_uuidnormalize text or blob UUID argv[0] into a 16-byte blob.
 */
static void
sqlite_fdw_data_norm_uuid(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	unsigned char aBlob[16];
	sqlite3_value* arg = argv[0];

	if (sqlite3_value_type(argv[0]) == SQLITE3_TEXT)
	{
		const unsigned char* txt = sqlite3_value_text(arg);
		if (sqlite_fdw_uuid_blob(txt, aBlob))
		{
			sqlite3_result_blob(context, aBlob, 16, SQLITE_TRANSIENT);
			return;
		}
	}
	sqlite3_result_value(context, arg);
}

/*
 * ISO:SQL valid boolean values with text affinity such as Y, no, f, t, oN etc.
 * will be treated as boolean like in PostgreSQL console input
 */
static void
sqlite_fdw_data_norm_bool(sqlite3_context* context, int argc, sqlite3_value** argv)
{

	sqlite3_value* arg = argv[0];
	int dt = sqlite3_value_type(arg);
	const char* t;
	int l;

	if (dt == SQLITE_INTEGER)
	{
		/* The fastest call because expected very often */
		sqlite3_result_value(context, arg);
		return;
	}
	if (dt != SQLITE3_TEXT && dt != SQLITE_BLOB )
	{
		/* NULL, FLOAT */
		sqlite3_result_value(context, arg);
		return;
	}
	l = sqlite3_value_bytes(arg);
	if (l > 5)
	{
		sqlite3_result_value(context, arg);
		return;
	}

	t = (const char*)sqlite3_value_text(arg);

	if ( l == 1 )
	{
		if (strcasecmp(t, "t") == 0)
		{
			sqlite3_result_int(context, 1);
			return;
		}
		if (strcasecmp(t, "f") == 0)
		{
			sqlite3_result_int(context, 0);
			return;
		}
		if (strcasecmp(t, "y") == 0)
		{
			sqlite3_result_int(context, 1);
			return;
		}
		if (strcasecmp(t, "n") == 0)
		{
			sqlite3_result_int(context, 0);
			return;
		}
		/* rare but possible cases */
		if (strcasecmp(t, "1") == 0)
		{
			sqlite3_result_int(context, 1);
			return;
		}
		if (strcasecmp(t, "0") == 0)
		{
			sqlite3_result_int(context, 0);
			return;
		}
	}
	else if ( l == 2 )
	{
		if (strcasecmp(t, "on") == 0)
		{
			sqlite3_result_int(context, 1);
			return;
		}
		if (strcasecmp(t, "no") == 0)
		{
			sqlite3_result_int(context, 0);
			return;
		}
	}
	else if ( l == 3 )
	{
		if (strcasecmp(t, "yes") == 0)
		{
			sqlite3_result_int(context, 1);
			return;
		}
		if (strcasecmp(t, "off") == 0)
		{
			sqlite3_result_int(context, 0);
			return;
		}
	}
	else if ( l == 4 && strcasecmp(t, "true") == 0)
	{
		sqlite3_result_int(context, 1);
		return;
	}
	else if ( l == 5 && strcasecmp(t, "false") == 0)
	{
		sqlite3_result_int(context, 0);
		return;
	}
	sqlite3_result_value(context, arg);
}

/*
 * Attempt to parse a zero-terminated input string zs into a binary
 * MAC address.  Return 1 on success, or 0 if the input string is not
 * parsable.
 */
static int
sqlite_fdw_macaddr_blob (const unsigned char* s, unsigned char* Blob)
{
	int		 a,	b, c, d, e,	f;
	char	 junk[2];
	int		 count;
	const char* str = (const char*)s;

	/* %1s matches iff there is trailing non-whitespace garbage */

	count = sscanf(str, "%x:%x:%x:%x:%x:%x%1s",
				   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%x-%x-%x-%x-%x-%x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%2x%2x%2x:%2x%2x%2x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%2x%2x%2x-%2x%2x%2x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%2x%2x.%2x%2x.%2x%2x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%2x%2x-%2x%2x-%2x%2x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		count = sscanf(str, "%2x%2x%2x%2x%2x%2x%1s",
					   &a, &b, &c, &d, &e, &f, junk);
	if (count != 6)
		return false;

	if ((a < 0) || (a > 255) || (b < 0) || (b > 255) ||
		(c < 0) || (c > 255) || (d < 0) || (d > 255) ||
		(e < 0) || (e > 255) || (f < 0) || (f > 255))
		return false;

	Blob[0] = a;
	Blob[1] = b;
	Blob[2] = c;
	Blob[3] = d;
	Blob[4] = e;
	Blob[5] = f;
	return true;
}

/*
 * sqlite_fdw_data_norm_macaddr normalize text or ineger or blob macaddr argv[0] into 6-byte blob.
 */
static void
sqlite_fdw_data_norm_macaddr(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	unsigned char aBlob[6];
	sqlite3_value* arg = argv[0];
	sqlite3_value* len_arg = argv[1];
	int vt = sqlite3_value_type(arg);
	int len = 0;
	if (vt == SQLITE_BLOB)
	{
		/* the fastest call for typical case */
		sqlite3_result_value(context, arg);
		return;
	}
	if (sqlite3_value_type(len_arg) != SQLITE_INTEGER)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			 errmsg("no mac address length argument in BLOB creating function %s", __func__)));
	}
	len = sqlite3_value_int(len_arg);
	
	if (vt == SQLITE3_TEXT)
	{
		const unsigned char* txt = sqlite3_value_text(arg);
		if (sqlite_fdw_macaddr_blob(txt, aBlob))
		{
			sqlite3_result_blob(context, aBlob, len, SQLITE_TRANSIENT);
			return;
		}
	}
	if (vt == SQLITE_INTEGER)
	{
		const sqlite3_int64 v = sqlite3_value_int64(arg);
		int					i = len - 1;
		if ( v > ((sqlite3_int64)1 << (len * CHAR_BIT)))
		{
			ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			 errmsg("integer for mac address greather than byte length of MAC address"),
			 errhint("%lld", v)));
		}

		elog(DEBUG5, "sqlite_fdw : int for macaddr %lld", v);
		for (;i >=0; i--)
		{
			int s = CHAR_BIT*i;
			aBlob[len-i-1] = (v >> s) & 0xff;
		}
		sqlite3_result_blob(context, aBlob, len, SQLITE_TRANSIENT);
		return;
	}
	sqlite3_result_value(context, arg);
}

/*
 * Converts argument BLOB-MAC address into a well-formed MAC address string.
 */
static void
sqlite_fdw_macaddr_str(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	sqlite3_value* arg = argv[0];
	int t = sqlite3_value_type(arg);
	int	l = sqlite3_value_bytes(arg);
	if (t != SQLITE_BLOB || l !=6 )
	{
		ereport(ERROR,
		(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
		 errmsg("internal deparse error, SQLite input have not 'BLOB' affinity")));
		return;
	}
	{
		const unsigned char* pBlob;
		char	   *result = (char *) palloc(MACADDR_LEN * 4);
		pBlob = sqlite3_value_blob(arg);
		snprintf(result, MACADDR_LEN * 4, "%02x:%02x:%02x:%02x:%02x:%02x",
			  pBlob[0], pBlob[1], pBlob[2], pBlob[3], pBlob[4], pBlob[5]);
  		sqlite3_result_text(context, (char*)result, MACADDR_LEN * 4, SQLITE_TRANSIENT);
	}
}

/*
 * Converts argument BLOB-MAC8 address into a well-formed 8 byte MAC address string.
 */
static void
sqlite_fdw_macaddr8_str(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	sqlite3_value* arg = argv[0];
	int t = sqlite3_value_type(arg);
	int	l = sqlite3_value_bytes(arg);
	if (t != SQLITE_BLOB || l !=8 )
	{
		ereport(ERROR,
		(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
		 errmsg("internal deparse error, SQLite input have not 'BLOB' affinity")));
		return;
	}
	{
		const unsigned char* pBlob;
		char	   *result = (char *) palloc(MACADDR8_LEN * 4);
		pBlob = sqlite3_value_blob(arg);
		snprintf(result, MACADDR8_LEN * 4, "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
			pBlob[0], pBlob[1], pBlob[2], pBlob[3],
			pBlob[4], pBlob[5], pBlob[6], pBlob[7]);
  		sqlite3_result_text(context, (char*)result, MACADDR8_LEN * 4, SQLITE_TRANSIENT);
	}
}

/*
 * Converts argument BLOB-MAC address (both 6 or 8 bytes) into a well-formed MAC address integer.
 */
static void
sqlite_fdw_macaddr_int(sqlite3_context* context, int argc, sqlite3_value** argv)
{
	sqlite3_value* arg = argv[0];
	int t = sqlite3_value_type(arg);
	int	l = sqlite3_value_bytes(arg);
	if (t != SQLITE_BLOB || (l !=6 && l !=8 ))
	{
		ereport(ERROR,
		(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
		 errmsg("internal deparse error, SQLite input have not 'BLOB' affinity with 6 bytes length")));
		return;
	}
	if (l == 6 )	
	{
		const unsigned char*	sqlite_blob = sqlite3_value_blob(arg);
		sqlite_int64			i = (((sqlite_int64)(sqlite_blob[0]))<<40) +
									(((sqlite_int64)(sqlite_blob[1]))<<32) +
									((sqlite_blob[2])<<24) +
									((sqlite_blob[3])<<16) +
									((sqlite_blob[4])<<8) +
									(sqlite_blob[5]);
		sqlite3_result_int64(context, i);
	}
	if (l == 8 )	
	{
		const unsigned char*	sqlite_blob = sqlite3_value_blob(arg);
		sqlite_int64			i = (((sqlite_int64)(sqlite_blob[0]))<<56) +
									(((sqlite_int64)(sqlite_blob[1]))<<48) +
									(((sqlite_int64)(sqlite_blob[2]))<<40) +
									(((sqlite_int64)(sqlite_blob[3]))<<32) +
									((sqlite_blob[4])<<24) +
									((sqlite_blob[5])<<16) +
									((sqlite_blob[6])<<8) +
									(sqlite_blob[7]);
		sqlite3_result_int64(context, i);
	}	
	
}

/*
 * Makes pg error from SQLite error.
 * Interrupts normal executing, no need return after place of calling
 */
static void
error_helper(sqlite3* db, int rc)
{
	const char * err = sqlite3_errmsg(db);
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			 errmsg("failed to create data unifying functions for SQLite DB"),
			 errhint("%s \n SQLite code %d", err, rc)));
}

void
sqlite_fdw_data_norm_functs_init(sqlite3* db)
{
	static const int det_flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;

	int rc = sqlite3_create_function(db, "sqlite_fdw_uuid_blob", 1, det_flags, 0, sqlite_fdw_data_norm_uuid, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_bool", 1, det_flags, 0, sqlite_fdw_data_norm_bool, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_uuid_str", 1, det_flags, 0, sqlite_fdw_uuid_str, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_macaddr_blob", 2, det_flags, 0, sqlite_fdw_data_norm_macaddr, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_macaddr_str", 1, det_flags, 0, sqlite_fdw_macaddr_str, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_macaddr8_str", 1, det_flags, 0, sqlite_fdw_macaddr8_str, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);
	rc = sqlite3_create_function(db, "sqlite_fdw_macaddr_int", 1, det_flags, 0, sqlite_fdw_macaddr_int, 0, 0);
	if (rc != SQLITE_OK)
		error_helper(db, rc);

	/* no rc because in future SQLite releases it can be added UUID generation function
	 * PostgreSQL 13+, no gen_random_uuid() before
	 *	static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS;
	 *	sqlite3_create_function(db, "uuid_generate_v4", 0, flags, 0, uuid_generate, 0, 0);
	 *	sqlite3_create_function(db, "gen_random_uuid", 1, flags, 0, uuid_generate, 0, 0);
	 */
}
