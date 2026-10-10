# pz-connector-clickhouse

ClickHouse source and sink for [PipelineZ](https://pipelinez.dev) (`pz`). One connection is one
ClickHouse endpoint plus credentials (and optionally a cluster name); an entity is a table named the
way ClickHouse names it (`table` or `database.table`).

The connector talks to ClickHouse's HTTP interface with `FORMAT ArrowStream` in both directions:
a read is one streaming `SELECT` whose response pz ingests as Arrow, a write streams Arrow batches
into a staging table and then runs one commit step. It supports a single self-managed node, ClickHouse
Cloud, and self-managed clusters. It runs out of process and has no third-party dependency besides
Apache.Arrow.

## Install

Add the package under `connectors:` in `project.yml`, then restore:

```yaml
connectors:
  - package: Pz.Connector.ClickHouse
    version: 0.1.0
```

```bash
pz restore
```

The package ships Native AOT binaries for linux-x64, linux-arm64, win-x64 and osx-arm64; there is no
.NET runtime requirement on the machine.

## Connection

```yaml
ch:
  connector: clickhouse
  url: https://abc.eu-west-1.aws.clickhouse.cloud:8443   # required; http(s)://host:port, no path
  user: default                                          # default "default"
  password: ${CLICKHOUSE_PASSWORD}                       # optional
  database: analytics                                    # default "default"
  cluster: prod_cluster                                  # optional; self-managed clusters only
  tls_ca_file: certs/ca.pem                              # optional; relative to the project directory
  settings: { max_execution_time: 600 }                  # optional
```

| Key | Meaning |
|---|---|
| `url` | Required. `http://` or `https://`, host and port, no path, query or fragment. |
| `user` | ClickHouse user. Default `default`. |
| `password` | Sent in the `X-ClickHouse-Key` header, never in the URL, and redacted from every error. |
| `database` | Database for entity names that do not carry one. Default `default`. |
| `cluster` | A cluster name from `system.clusters`. Self-managed clusters only; it is rejected on ClickHouse Cloud (PZCH0105). See [Clusters](#clusters). |
| `tls_ca_file` | A PEM root certificate to trust for an `https` URL (the chain is built against that CA only). Relative paths resolve against the project directory. Without it the OS trust store applies. An untrusted certificate fails permanently (PZCH0401) with a hint to set this key. |
| `settings` | ClickHouse settings sent as URL parameters on every request. Values are strings, numbers or booleans. Read and write options can add or override settings per entity. |

Unknown keys are refused. An entity name is `table` or `database.table`; a name with a backtick or more
than two parts is refused (PZCH0104). Every generated identifier is backtick-quoted.

## Reading

```yaml
entities:
  events:
    read: { entity: analytics.events }        # optional; defaults to the pz entity name
  eu_events:
    read:
      query: "select * from events where region = 'EU'"
      settings: { max_threads: 8 }            # merged over the connection's settings
```

- `entity` reads a table or view. `query` runs your SQL; giving both is an error (PZCH0201).
- `settings` are merged over the connection's settings for that read.
- Column pruning and predicate pushdown apply to `entity` reads: the projection, the pushed predicate
  and a limit become part of the `SELECT`.
- **`query:` reads apply watermark and window bounds** (the query is wrapped and the bound added), but
  **never push ReadHints**: the SQL runs as you wrote it.
- **Rejected predicates.** The pushed predicate is DuckDB-dialect SQL, and ClickHouse may reject a
  function name. When ClickHouse rejects the predicate (and accepts the query without it), the read
  drops the predicate and logs a warning in pz's log (an out-of-process connector cannot raise a
  notice). Nothing is lost: DuckDB re-applies your pipeline's own SQL over whatever landed.

### Incremental reads

Watermark and window bounds are bound as typed query parameters, never spliced into the SQL. Supported
cursor column types: integers, `Decimal`, `Date`, `Date32`, `DateTime`, `DateTime64` and `String`. Any
other type, or a cursor missing from the table, is refused before any data is read (PZCH0202).

**Timestamp bounds are instants.** For `DateTime` and `DateTime64` cursors the bound is compared as an
instant (Unix microseconds through `fromUnixTimestamp64Micro`), independent of any timezone. A bound
without an offset is read as UTC.

### Type mapping, ClickHouse to Arrow

| ClickHouse | Arrow |
|---|---|
| `Bool` | boolean |
| `(U)Int8/16/32/64` | (u)int8/16/32/64 |
| `Float32/64` | float / double |
| `Decimal(p, s)` up to precision 38 | decimal128(p, s) |
| `Decimal(p, s)` above precision 38 | decimal256(p, s) |
| `String` | utf8 |
| `FixedString(n)` | fixed_size_binary(n) |
| `Date`, `Date32` | date32 (read through `toDate32`) |
| `DateTime[('tz')]` | timestamp(s), with the column timezone or the server timezone |
| `DateTime64(p[, 'tz'])` | timestamp(unit for p), with the column timezone or the server timezone |
| `Nullable(T)` | T, nullable |
| `LowCardinality(T)` | T (no dictionary) |
| `Enum8/16`, `UUID`, `IPv4`, `IPv6`, `JSON`, `Dynamic`, `Variant`, `Object` | utf8 (`toString`) |
| `(U)Int128`, `(U)Int256` | utf8 (`toString`) |
| `Array(T)`, `Tuple(...)`, `Map(K, V)` | list, struct, map (utf8 when an element needs a cast) |
| `Interval*`, `Time`, `Time64`, `AggregateFunction`, `SimpleAggregateFunction`, `Nothing` | refused, naming the column (PZCH0204) |

## Writing

```yaml
entities:
  events_agg:
    write:
      entity: analytics.events_agg
      strategy: merge                  # append | replace | merge
      keys: [event_id]                 # merge only
      merge_strategy: replacing        # replacing (default) | delete_insert
      engine: "ReplacingMergeTree(updated_at)"
      order_by: [event_id]
      partition_by: "toYYYYMM(event_date)"
      settings: { insert_deduplicate: 0 }
```

Batches land in a staging table `_pz_stg_<tag>` in the target's database, and one commit step then
touches the target. A crash before commit leaves the target untouched. `append` is at-least-once;
`replace` and both `merge` strategies are idempotent on retry. The sink does not claim transactions:
ClickHouse has none across statements.

- **`append`** inserts the staged rows into the target, creating it if missing.
- **`replace`** builds a shadow table (`<target>__pz_new_<tag>`, same engine, `ORDER BY`, partitioning
  and settings as the target), fills it, and swaps it in with `EXCHANGE TABLES`, so a reader sees the old
  or the new table. The database must be `Atomic`, `Replicated` or `Shared` (PZCH0306 otherwise).
- **`merge`** needs `keys:` and deduplicates the staged rows by key (the last row in a write wins).
  - `replacing` (default) inserts into a `ReplacingMergeTree`, creating it with a `_pz_version` column
    if missing. An existing target must be a `ReplacingMergeTree` family engine (PZCH0302).
  - `delete_insert` deletes the keys that are being written, then inserts the new rows. It is not
    atomic: readers can briefly see the merged keys missing, and a retry converges.
- For `merge_strategy: replacing`, ClickHouse deduplicates by the table's sorting key, so an `order_by:`
  override must cover the merge `keys:` (the default `order_by` is the keys).
- `engine`, `order_by` (a list of columns), `partition_by` and `settings` are used **only when the sink
  creates the table**. Against an existing table they are ignored.

### Created-table types, Arrow to ClickHouse

| Arrow | ClickHouse |
|---|---|
| boolean | `Bool` |
| (u)int8/16/32/64 | `(U)Int8/16/32/64` |
| float, double (half-float is widened) | `Float32`, `Float64` |
| decimal128(p, s), decimal256(p, s) | `Decimal(p, s)` |
| utf8, large utf8, binary, large binary | `String` |
| fixed_size_binary(n) | `FixedString(n)` |
| date32, date64 | `Date32` |
| timestamp(unit, tz) | `DateTime64(0/3/6/9, 'tz')`; without a timezone, `DateTime64(p)` in the server timezone |
| list, struct, map | `Array(T)`, `Tuple(name T, ...)`, `Map(K, V)` |
| nullable field | `Nullable(T)` |
| dictionary, union, duration, interval, null, time32, time64 | refused (PZCH0303) |

Nullability is kept except where ClickHouse forbids it (map keys, and the containers themselves, see
below). Every write column must exist on an existing target (PZCH0304; names compare case-sensitively).
The names `_pz_seq` and `_pz_version` are reserved (PZCH0308).

### Caveats

- **Tables the sink creates force merge keys and `order_by` columns non-Nullable**, because ClickHouse
  sorting keys reject `Nullable`. A null in such a column fails the insert (PZCH0305).
- **`replacing` merges need `FINAL`, or `argMax`, for exact reads.** Until background merges run, a
  table can hold several versions of a key.
- **`replacing` stamps `_pz_version` from the wall clock at commit.** It assumes the clock moves
  forward across runs. An NTP step backwards, or clock skew between machines running pz, can make a
  later run lose to an earlier one.
- **`delete_insert` is not atomic.** A reader can briefly see the deleted keys missing, and a retry
  converges. NULL keys never match `IN`, so rows with NULL keys are re-inserted, not replaced. On a
  cluster the key `DELETE` runs with `allow_nondeterministic_mutations=1`; this is safe because staging
  is synced to every replica first and dropped only after the delete completes everywhere. If a replica
  is down during the delete, its queued mutation can fail after staging is dropped: check
  `system.mutations`.
- **A `Distributed` target deduplicates only within a shard.** Its sharding key should include the
  merge keys.
- **On a cluster, replace swaps each node in turn.** For an instant, a read through the `Distributed`
  table can mix old and new shards.
- **On a cluster, staging data is copied to every node.** Large writes pay that replication cost.
- **A null list, map or struct lands as empty or default**, because ClickHouse forbids `Nullable`
  arrays, maps and tuples. Nullability inside the container is kept.
- **Time, interval and dictionary columns are refused** (PZCH0303).
- **Append is at-least-once.** A retried write can land its rows twice; use `merge` for effectively-once.

## Clusters

Set `cluster:` to a name from `system.clusters`; it is checked when the connection opens (PZCH0103).

- All DDL (`CREATE`, `EXCHANGE`, `RENAME`, `DROP`, the key delete) runs `ON CLUSTER`.
- Tables the sink creates are `Replicated*MergeTree` with the server's default `{shard}`/`{replica}`
  macros. The sink creates only the local table, never a `Distributed` front; create one yourself and
  point the entity at it if you want one.
- **`{replica}` must be unique per node** across the cluster (the usual per-host macro). The connector
  checks this and refuses otherwise (PZCH0106).
- Writing to a `Distributed` target goes through the `Distributed` table, while DDL and deletes address
  the local table it names.
- Staging is one replicated table with a replica on every node, and is synced before the commit
  reads it, so a commit statement sees every staged row on whichever node a load balancer picks.
- Replace on a replicated table with a static Keeper path gives the shadow table a suffixed Keeper path
  (`.../pz_new_<tag>`) so it does not collide with the table it shadows.

## ClickHouse Cloud

ClickHouse Cloud is supported by design but has not been tested against a live Cloud service.
Use the `https://...:8443` URL. Cloud is detected automatically and tables the sink creates use plain
engine names, which Cloud maps to `Shared*` engines. `cluster:` is rejected on Cloud (PZCH0105).

## Errors

Every message has the form `clickhouse: PZCH####: <text>`. Passwords and auth headers are redacted.
Messages carry the context, the `query_id` and the server's one-line error text (credentials redacted),
which may quote part of the statement, for example a syntax error in a `query:` read.

| Code | Meaning |
|---|---|
| PZCH0101 | `url` is not a valid `http(s)://host:port` with no path, query or fragment. |
| PZCH0102 | `tls_ca_file` cannot be read. |
| PZCH0103 | `cluster` is not in `system.clusters`. |
| PZCH0104 | Entity name has a backtick or more than two parts. |
| PZCH0105 | `cluster:` is set on a ClickHouse Cloud connection. |
| PZCH0106 | The `{replica}` macro is not unique across the cluster. |
| PZCH0107 | Invalid connection configuration (unknown key, empty value, bad `settings`, `tls_ca_file` on an `http` URL). |
| PZCH0201 | A read sets both `entity` and `query`. |
| PZCH0202 | Unsupported or missing incremental cursor column, or an unparseable timestamp bound. |
| PZCH0203 | The read query failed, including a failure after the response started. |
| PZCH0204 | The table has a ClickHouse type that cannot be read (names the type and column). |
| PZCH0205 | Invalid read option. |
| PZCH0301 | Unsupported `strategy`, or `merge_strategy` set outside `strategy: merge`. |
| PZCH0302 | `merge_strategy: replacing` against an existing table that is not a `ReplacingMergeTree` family engine. |
| PZCH0303 | An Arrow type that cannot be written (dictionary, union, duration, interval, null, time). |
| PZCH0304 | A write column is missing on the existing target. |
| PZCH0305 | A commit statement failed. |
| PZCH0306 | The database engine has no `EXCHANGE TABLES`, so `replace` cannot run. |
| PZCH0307 | Invalid write option (unknown key, bad `merge_strategy` or `order_by`). |
| PZCH0308 | Merge keys: none given, not in the write schema, or a reserved column name. |
| PZCH0401 | Permanent authentication, authorization or TLS failure (ClickHouse codes 516, 192, 194, HTTP 401/403, an untrusted certificate). An untrusted certificate carries a `tls_ca_file` hint. |
| PZCH0402 | Transient failure that pz retries: HTTP 429/502/503/504, and ClickHouse codes 159 (timeout exceeded), 209 (socket timeout), 210 (network error), 242 (table is read-only), 252 (too many parts), 202 (too many simultaneous queries) and 999 (Keeper exception). Honors `Retry-After`. |

## Leftovers

A crashed process can leave helper tables behind in the target's database. All of them are safe to
drop:

- `_pz_stg_*`: staging tables.
- `*__pz_new_*`: replace shadow tables.
- `_pz_front_*`: transient `Distributed` fronts used by replace on a `Distributed` target.

The connection check that pz runs under `pz validate --connect` warns when `_pz_stg_*` tables exist in the connection's database.
