/*
 * pg_object_store.h - the ABI a dependent extension resolves.
 *
 * Keep this in sync with src/vtable.rs, which is the definition of record; this header exists
 * so that a dependent written in C, or a dependent that would rather not re-declare the struct
 * in Rust, has one place to include.
 *
 * Usage:
 *
 *   void **slot = find_rendezvous_variable("pg_object_store.v1");
 *   PgObjectStoreApiV1 *api = *slot;
 *
 *   if (api == NULL)
 *   {
 *       load_file("$libdir/pg_object_store", false);
 *       api = *slot;
 *   }
 *
 *   if (api == NULL ||
 *       api->magic != PG_OBJECT_STORE_API_MAGIC ||
 *       api->version != PG_OBJECT_STORE_API_VERSION)
 *       ereport(ERROR, ...);
 *
 * Contract:
 *
 * - Every call blocks. There is no async entry point; the tokio runtime stays inside
 *   pg_object_store.
 * - No call ereports. A failure is a return code plus a palloc'd message in *error, so the
 *   caller chooses the elevel and the error code, and a caller that wants to recover can.
 * - Returned buffers and strings are palloc'd in the caller's CurrentMemoryContext. There is
 *   no free function: switch contexts before calling if a buffer must outlive the statement.
 * - No Rust panic crosses the boundary. A Postgres ereport longjmp still can, exactly as it
 *   can between any two loaded modules.
 * - A streaming write is a handle, not a buffer. put() takes a whole object; put_begin,
 *   put_part, put_finish and put_abort are for a COPY TO that produces bytes as it goes, and
 *   they occupy four of the six slots v1 reserved. Adding a field in place of a reserved slot
 *   does not change the layout and so does not bump the version, which means the only thing
 *   that tells a caller whether the copy it resolved has them is a NULL check on the pointer.
 *   Check, every time.
 * - Every handle ends in put_finish or put_abort. One left open at transaction end is swept,
 *   and a swept upload is an aborted one: the file is not written.
 * - reserved[] is NULL. Check before calling.
 */
#ifndef PG_OBJECT_STORE_H
#define PG_OBJECT_STORE_H

#include "postgres.h"

#define PG_OBJECT_STORE_API_MAGIC 0x504f5331 /* "POS1" */
#define PG_OBJECT_STORE_API_VERSION 1
#define PG_OBJECT_STORE_RENDEZVOUS_KEY "pg_object_store.v1"

#define PGOS_OK 0
#define PGOS_NOT_FOUND 1
#define PGOS_ERROR (-1)

typedef struct PgObjectStoreApiV1
{
	uint32		magic;
	uint32		version;

	/* whether the current user may reach uri; copy_from true means reading */
	int			(*check_privilege) (const char *uri, bool copy_from,
									bool *allowed, char **error);

	/* size of the object at uri, or PGOS_NOT_FOUND */
	int			(*head) (const char *uri, int64 *size, char **error);

	/* one byte range; *data is palloc'd, *len may be short at end of object */
	int			(*get_range) (const char *uri, int64 offset, int64 length,
							  uint8 **data, int64 *len, char **error);

	/*
	 * several ranges of one object, fetched concurrently. data and lens are
	 * caller-supplied arrays of n_ranges elements; each data[i] is set to a
	 * palloc'd buffer. All or nothing.
	 */
	int			(*get_ranges) (const char *uri, int32 n_ranges,
							   const int64 *offsets, const int64 *lengths,
							   uint8 **data, int64 *lens, char **error);

	/* write the object at uri, replacing anything already there */
	int			(*put) (const char *uri, const uint8 *data, int64 len,
						char **error);

	/* objects under uri as a prefix; arrays and strings are palloc'd */
	int			(*list) (const char *uri, int32 *n_objects, char ***locations,
						 int64 **sizes, char **error);

	/*
	 * open a streaming write to uri and set *handle to an opaque, non-NULL
	 * handle. *handle is NULL whenever the call did not return PGOS_OK. May be
	 * NULL: see the contract above.
	 */
	int			(*put_begin) (const char *uri, void **handle, char **error);

	/*
	 * append len bytes. Anything other than PGOS_OK closes the handle and
	 * discards the upload: a multipart upload cannot resume past a missing
	 * part. May be NULL.
	 */
	int			(*put_part) (void *handle, const uint8 *data, int64 len,
							 char **error);

	/*
	 * complete the upload and close the handle. The object becomes visible to a
	 * reader here and nowhere earlier. May be NULL.
	 */
	int			(*put_finish) (void *handle, char **error);

	/* discard the upload and close the handle. May be NULL. */
	int			(*put_abort) (void *handle, char **error);

	/* two spares, for the same reason the first four existed. NULL. */
	const void *reserved[2];
}			PgObjectStoreApiV1;

#endif							/* PG_OBJECT_STORE_H */
