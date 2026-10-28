/*
 * pg_arrow_base.h -- registering a file format provider with pg_arrow_base.
 *
 * A provider is an extension that can turn the bytes of a file into Arrow and back. It fills
 * in a PgArrowProviderV1, resolves pg_arrow_base's registry from its own _PG_init, and
 * registers. From then on `COPY ... WITH (format '<name>')` and the arrow.* functions reach it,
 * and it never sees a uri, a credential or a Postgres internal -- pg_arrow_base does all the
 * I/O, through pg_object_store.
 *
 * The data path is the Arrow C Data Interface (ArrowSchema / ArrowArray / ArrowArrayStream),
 * which means a provider and pg_arrow_base may be built against different Arrow versions.
 * Include arrow's abi.h, or any header that declares those three structs, before this one.
 *
 * Contract, in full:
 *
 *   - Nothing may ereport and nothing may longjmp out of a provider function. Report a failure
 *     by returning PGARROW_ERROR and setting *error.
 *   - Return PGARROW_UNSUPPORTED from kv_metadata or file_info for a question the format cannot
 *     answer at all. The base renders NULL rather than failing the query.
 *   - Everything a provider returns through an out-parameter is palloc'd in the caller's
 *     CurrentMemoryContext: error messages, the kv_metadata arrays and their strings, the
 *     finished file from finish_writer. There is no free function; the caller's context
 *     teardown is the free.
 *   - `name`, `file_extensions` and PgArrowFileInfoV1.split_label are static strings owned by
 *     the provider.
 *   - An ArrowSchema or ArrowArray passed *in* is consumed: the provider owns it and must
 *     release it. One filled in as an out-parameter is handed to the base on the same terms.
 *   - The buffer passed to schema / kv_metadata / file_info must not be retained after the
 *     call. The one passed to open_reader is kept alive by the base until the stream is
 *     released, and must not be retained past that.
 *   - Exactly one terminator runs for a writer handle: finish_writer for a buffered writer,
 *     finish_streaming_writer for a streaming one, or close_writer for either if the statement
 *     failed first. A finish_ call closes the handle whether it succeeded or not and the base
 *     forgets it at that point, so a provider may free its allocation there; close_writer is
 *     only ever called on a handle that was never finished. All must tolerate a NULL handle.
 *   - A callback the base installs -- PgArrowByteSourceV1.get_ranges, PgArrowByteSinkV1.write --
 *     returns only a code, with no error out-parameter. The base keeps the message on its own
 *     side and reports that one, because a provider has no allocator the base may use and a
 *     message copied across the boundary would be the second-hand version of it.
 *
 * Whole files, and the two ways around them
 * -----------------------------------------
 *
 * The baseline path is whole-buffer: schema / kv_metadata / file_info / open_reader get the
 * bytes of the file, and open_writer / write_batch / finish_writer hand a finished file back.
 * That is correct for a small file and wrong for a large one, so v2 adds a way out at each end,
 * and a provider may implement either, both or neither:
 *
 *   - open_ranged_reader. The provider is handed a PgArrowByteSourceV1 instead of a buffer and
 *     fetches what it needs -- a parquet footer, then the row groups a scan touches. The
 *     split_begin / split_end arguments are what gives one worker of a parallel COPY FROM one
 *     split range; a negative split_begin means the whole file. A provider that cannot restrict
 *     a read to splits must leave this NULL rather than ignoring the range, since reading
 *     everything in every worker would multiply the rows.
 *   - open_streaming_writer. The provider is handed a PgArrowByteSinkV1 and writes the file
 *     into it as it goes, so a COPY TO of any size never materializes the output. Batches still
 *     arrive through write_batch; the handle ends in finish_streaming_writer.
 *
 * Leaving a slot NULL is a supported answer, not a degraded one: the base asks before it does
 * any I/O and falls back to the whole-buffer path for that statement.
 *
 * Registering looks like this:
 *
 *   #include "pg_arrow_base.h"
 *
 *   static const PgArrowProviderV1 my_provider = {
 *       .magic = PG_ARROW_PROVIDER_MAGIC,
 *       .version = PG_ARROW_PROVIDER_VERSION,
 *       .name = "myformat",
 *       .file_extensions = "myf,myformat",
 *       .validate_option = my_validate_option,
 *       ...
 *   };
 *
 *   void
 *   _PG_init(void)
 *   {
 *       void **slot = find_rendezvous_variable(PG_ARROW_REGISTRY_RENDEZVOUS_KEY);
 *       PgArrowRegistryApiV1 *registry = *slot;
 *       char *error = NULL;
 *
 *       if (registry == NULL)
 *       {
 *           load_file("$libdir/pg_arrow_base", false);   // lazy load
 *           registry = *slot;
 *       }
 *
 *       if (registry == NULL ||
 *           registry->magic != PG_ARROW_REGISTRY_MAGIC ||
 *           registry->version != PG_ARROW_REGISTRY_VERSION)
 *           ereport(ERROR, (errmsg("pg_arrow_base v1 is not available")));
 *
 *       if (registry->register_provider(&my_provider, &error) != PGARROW_OK)
 *           ereport(ERROR, (errmsg("could not register the myformat provider: %s", error)));
 *   }
 *
 * The version check has to happen on both sides. An ABI skew between two loaded libraries is a
 * crash, not a link error.
 */

#ifndef PG_ARROW_BASE_H
#define PG_ARROW_BASE_H

#include <stdbool.h>
#include <stdint.h>

/* "PAB1" and "PAP1" */
#define PG_ARROW_REGISTRY_MAGIC 0x50414231
#define PG_ARROW_PROVIDER_MAGIC 0x50415031

#define PG_ARROW_REGISTRY_VERSION 1
#define PG_ARROW_PROVIDER_VERSION 2

#define PG_ARROW_REGISTRY_RENDEZVOUS_KEY "pg_arrow_base.registry.v1"

#define PGARROW_OK 0
#define PGARROW_UNSUPPORTED 1
#define PGARROW_ERROR (-1)

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

/*
 * What the file says about itself. The has_ flags exist because 0 rows and "unknown rows" are
 * different answers, and a file with 0 rows is a real thing.
 */
typedef struct PgArrowFileInfoV1
{
	bool		has_num_rows;
	int64_t		num_rows;
	bool		has_num_splits;
	int64_t		num_splits;
	/* what one split is called, singular and lower case: "row group", "record batch" */
	const char *split_label;
	char	   *compression;
	char	   *format_version;
} PgArrowFileInfoV1;

/*
 * Random access to a file the base holds. Installed by the base, called by a provider.
 *
 * get_ranges fills caller-supplied arrays: data and lens have n_ranges elements, and data[i] is
 * set to a buffer the *base* owns. Those buffers are valid only until the next call on the same
 * source, and must not be freed by the provider; a reader that holds bytes for longer copies
 * them. The plural form is the point -- the base fetches them concurrently, and a loop over a
 * singular form would serialize a read that is parallel underneath.
 *
 * A non-PGARROW_OK return means the read failed and the message is already on the base's side.
 * Stop and propagate; do not retry.
 */
typedef struct PgArrowByteSourceV1
{
	void	   *ctx;

	int64_t		(*size) (void *ctx);

	int			(*get_ranges) (void *ctx, int32_t n_ranges,
							   const int64_t *offsets, const int64_t *lengths,
							   uint8_t **data, int64_t *lens);
} PgArrowByteSourceV1;

/*
 * Where a streaming writer's bytes go. Installed by the base, called by a provider.
 *
 * A non-PGARROW_OK return means the upload failed. Stop and propagate: the base has the real
 * message and will report that rather than whatever the provider says about it.
 */
typedef struct PgArrowByteSinkV1
{
	void	   *ctx;

	int			(*write) (void *ctx, const uint8_t *data, int64_t len);
} PgArrowByteSinkV1;

typedef struct PgArrowProviderV1
{
	uint32_t	magic;
	uint32_t	version;

	/* the name in COPY ... WITH (format 'x'), lower case, unique in a cluster */
	const char *name;
	/* comma separated, no dots, lower case; may be empty but not NULL */
	const char *file_extensions;

	int			(*validate_option) (const char *name, const char *value, char **error);

	int			(*schema) (const uint8_t *data, int64_t len,
						   struct ArrowSchema *out, char **error);

	/* a NULL (*values)[i] is a key stored without a value, not an empty one */
	int			(*kv_metadata) (const uint8_t *data, int64_t len,
								int32_t *n, char ***keys, char ***values, char **error);

	int			(*file_info) (const uint8_t *data, int64_t len,
							  PgArrowFileInfoV1 *out, char **error);

	int			(*open_reader) (const uint8_t *data, int64_t len,
								int32_t n_options,
								const char *const *option_names,
								const char *const *option_values,
								struct ArrowArrayStream *out, char **error);

	int			(*open_writer) (struct ArrowSchema *schema,
								int32_t n_options,
								const char *const *option_names,
								const char *const *option_values,
								void **handle, char **error);

	int			(*write_batch) (void *handle, struct ArrowArray *array,
								struct ArrowSchema *schema, char **error);

	int			(*finish_writer) (void *handle, uint8_t **data, int64_t *len, char **error);

	void		(*close_writer) (void *handle);

	/*
	 * Reads the file in pieces instead of taking it whole, restricted to splits
	 * [split_begin, split_end); a negative split_begin means all of them. `source` is valid
	 * until the stream is released. NULL means this format has to be read whole.
	 */
	int			(*open_ranged_reader) (const PgArrowByteSourceV1 *source,
									   int64_t split_begin, int64_t split_end,
									   int32_t n_options,
									   const char *const *option_names,
									   const char *const *option_values,
									   struct ArrowArrayStream *out, char **error);

	/*
	 * Opens a writer that writes into `sink` as it goes rather than buffering the file.
	 * `sink` is valid until the handle is closed. Batches arrive through write_batch, the same
	 * as for a buffered writer. NULL means this format has to be buffered whole.
	 */
	int			(*open_streaming_writer) (struct ArrowSchema *schema,
										  int32_t n_options,
										  const char *const *option_names,
										  const char *const *option_values,
										  const PgArrowByteSinkV1 *sink,
										  void **handle, char **error);

	/* writes the footer through the sink and closes the handle; there is no buffer to return */
	int			(*finish_streaming_writer) (void *handle, char **error);

	/*
	 * Reserved, and NULL in v2. The intended use, in order: [0] a validate_options taking the
	 * whole list at once, for options that are only invalid in combination; [1] a parts-based
	 * streaming writer for a parallel COPY TO of a single file, where several processes upload
	 * parts and one stitches the footer. A provider must leave them NULL; the base must check
	 * before using one.
	 */
	const void *reserved[4];
} PgArrowProviderV1;

typedef struct PgArrowRegistryApiV1
{
	uint32_t	magic;
	uint32_t	version;

	/*
	 * Registers a provider for the life of the backend. The base keeps the pointer, so it must
	 * point at something with static lifetime in the provider's library. Fails if the format
	 * name is taken: silently replacing would make which implementation ran depend on library
	 * load order.
	 */
	int			(*register_provider) (const PgArrowProviderV1 *provider, char **error);

	bool		(*unregister_provider) (const char *name);

	const void *reserved[4];
} PgArrowRegistryApiV1;

#endif							/* PG_ARROW_BASE_H */
