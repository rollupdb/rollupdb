# Streaming imports and local HTTP

The CLI streaming input, the HTTP service and Prometheus remote write are adapters over the embedded engine. Databases are revision 5; later stream versions add definition rows without changing the format. All adapters use the same engine validation, finest-tier grace, counters, quantile settings, explicit retention and commit semantics. They do not create a raw-event store. Retries are recognized only through batch IDs (see docs/API.md).

## NDJSON command

```
rollupdb ingest-ndjson FILE STREAM INPUT [--batch-size N] [--batch-id-prefix P]
```

INPUT is a UTF-8 file, or `-` for stdin. Each physical line contains exactly one Observation object with explicit nonnull time/value and the exact declared dimensions. LF and CRLF are accepted, as is a final line without a newline. Empty lines, a BOM, null, unknown fields, malformed JSON, invalid UTF-8 and trailing objects are errors. JSON duplicate keys follow encoding/json's last-value behavior; this is not duplicate-key detection. As with JSON batches, a structurally valid observation can be rejected individually by the engine for timestamp, dimensions, numeric overflow, lateness or group capacity.

Records are limited to 256 KiB excluding LF/optional CR. Batch size defaults to 1000 records and must be in [1,10000]. A batch is also flushed before buffered serialized records would exceed 8 MiB. Those are input bounds, not an overall process memory promise: decoded maps, engine caches and sketches require additional memory. The importer does not buffer the entire file. Parsing the next record happens before a byte-capacity flush, so encountering malformed input does not unexpectedly commit the preceding pending batch.

Every successful engine batch produces one JSON-line receipt with type=batch, from_line, through_line and the complete BatchResult, including per-observation outcomes. A final JSON line has type=complete or error and reports committed_records, committed_batches, accepted/rejection totals, next_line, failed_line where applicable, discarded_records, unacknowledged_records and an error message. The exit status is nonzero on failure. committed_records includes rejected observations whose outcomes/counters committed, not just accepted data. next_line is one past the last successfully committed batch, not the scanner's read-ahead position.

An invalid line or input read error discards the pending not-yet-submitted observations and preserves earlier commits. The bad record itself is identified by failed_line; discarded_records counts parsed records that were never submitted. An engine error reports the submitted batch as unacknowledged_records. A commit-level I/O failure can be ambiguous, and a receipt/output failure may happen after a real commit. Without `--batch-id-prefix`, do not blindly retry the whole input: resubmission is new input and can duplicate observations. Keep receipts and resolve an ambiguous write before choosing a replay point.

With `--batch-id-prefix P` each batch is sent with the ID `P:N`, where N is the number of the batch's first record (the same as its line number, since blank lines are refused). Running the same input again with the same prefix and batch size replays the batches that committed (their receipts have `"replayed": true` and the summary counts them in replayed_batches) and commits the rest, so after any failure the whole input can simply be run again. Batch boundaries depend on the batch size and the 8 MiB byte flush, so keep both unchanged between runs. If a boundary has moved, a batch reuses an ID with other lines and the run stops with `batch_conflict` before writing it, rather than counting anything twice. A prefix is at most 107 bytes of printable ASCII. IDs are kept for seven days after their commit by default. An output failure may also prevent delivery of the final summary.

The command owns its file/stdin reader and closes it on cancellation to unblock reads. The reusable transport.ImportNDJSON does not own arbitrary readers: callers must close a blocked reader if they want cancellation to interrupt I/O. Waiting for the engine handle mutex is also not context-aware.

For a current-time smoke import, create a new database and the supplied stream, then generate input immediately before ingestion:

```sh
rollupdb init latency.rollupdb
rollupdb create-stream latency.rollupdb examples/api_latency_quantiles.json
python3 - <<'PY' > observations.ndjson
import datetime, json
at = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1)).isoformat()
for i in range(10000):
    print(json.dumps({"time": at, "value": i % 1000, "dimensions": {"service": "web"}}))
PY
rollupdb ingest-ndjson latency.rollupdb api_latency observations.ndjson --batch-size 1000
rollupdb stats latency.rollupdb api_latency
```

The sample definition starts October 17, 2026 and rejects events before its effective date. This example uses the host clock, so do not leave the generated data waiting beyond its grace window. Historical replay belongs in an embedding with an explicit clock, as in the examples.

## Stream versions and SQL commands

```
rollupdb update-stream FILE DEFINITION.json
rollupdb sql FILE STATEMENT
```

`update-stream` adds the stream's next version (`"version"` one higher than the current one) through `UpdateStream`; a change a version can't make fails with `definition_change` and writes nothing. HTTP has the same as `POST /v1/streams/NAME/versions`, and `GET` on that path lists every version. `sql` runs one read-only statement and prints `{"columns", "rows", "truncated"}`; STATEMENT `-` reads it from stdin. HTTP has the same as `POST /v1/sql` with `{"sql": "..."}`. docs/SQL.md has the views, aggregates and limits.

## HTTP service

```
rollupdb serve FILE [--listen 127.0.0.1:7070] [--advance-every 1m] [--remote-write MAP.json]
```

The file must already exist in the current format. The command never initializes or migrates it. While it runs it rolls up every stream once a minute (`--advance-every` changes the interval; `0` turns it off) and writes one JSON line per stream that did work, or per failure. Its first JSON line reports the actual listening address and the interval, including when port 0 chooses a free port. Only literal loopback IPs are allowed: 127.0.0.1 or [::1], for example. Wildcards, LAN IPs and hostnames are refused. Keep this as the one owning process for the file; use HTTP operations while it is running rather than opening another CLI writer.

There is no authentication or TLS. This is a trusted local-process interface, not a multi-user security boundary. Host values must be localhost or a literal loopback address; requests with any Origin header are refused and CORS is not enabled. These checks help keep browser requests out; they do not authenticate a local client. The exported transport.NewHandler must also be placed behind a loopback listener by its owner. The CLI cannot opt into public binding.

| Method | Path | Body / result |
| --- | --- | --- |
| GET | /v1/info | Engine Info |
| POST | /v1/streams | Definition; 201 on creation |
| GET | /v1/streams/NAME | Current definition |
| GET | /v1/streams/NAME/versions | Every version, oldest first |
| POST | /v1/streams/NAME/versions | The next version; 201 when added |
| GET | /v1/streams/NAME/stats | Counters, groups, buckets, coverage and frontiers |
| POST | /v1/ingest | One Batch; BatchResult after commit |
| POST | /v1/query | Query; exact statistics, quantiles and coverage |
| POST | /v1/streams/NAME/advance | Empty object {}; explicit retention result |
| GET | /v1/streams | {"streams": [names]} |
| POST | /v1/sql | {"sql": "SELECT ..."}; columns and rows, see docs/SQL.md |
| POST | /v1/advance | Empty object {}; results for every stream, by name. If any stream fails, the status and `error` follow the first failure, the message names every failed stream, and `results` still lists the streams that rolled up |
| GET | /v1/snapshot | Standalone snapshot download |
| POST | /api/v1/write | Prometheus remote write 1.0, with `--remote-write` only |
| GET | /api/v1/write/stats | Remote-write counters, with `--remote-write` only |

JSON POST requests require Content-Type: application/json, uncompressed UTF-8 object bodies, no unknown fields and no trailing objects. Other media types and compressed requests are refused. Ingest bodies are limited to 8 MiB; definitions, queries and advancement bodies to 1 MiB. Engine batches still have at most 10000 observations. HTTP ingestion is a single atomic JSON batch; streaming NDJSON is currently a CLI interface. Query parameters are unsupported. POST /v1/ingest takes the batch ID from `batch_id` in the body or from an `Idempotency-Key` header; when both are given they must be equal. A replayed batch returns 200 with the first result and `"replayed": true`; the same ID with other observations returns 409 `batch_conflict`.

The supplied stream can be created through HTTP with:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/api_latency_quantiles.json http://127.0.0.1:7070/v1/streams
curl --fail-with-body http://127.0.0.1:7070/v1/streams/api_latency/stats
curl --fail-with-body -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:7070/v1/streams/api_latency/advance
curl --fail-with-body http://127.0.0.1:7070/v1/snapshot -o downloaded.rollupdb
```

Use a fresh database or expect 409 if the stream already exists. The ingest/query endpoints take exactly the same JSON shapes as docs/API.md. There is no network-supplied clock override. Queries never run retention implicitly, and a late observation is still a counted rejection even if a coarser tier has a longer grace. A 200 BatchResult can therefore contain rejected observations; callers must inspect outcomes.

Errors are JSON objects shaped as {"error":{"code":"partial_bucket","message":"requested range cuts a retained bucket"}}. Bad JSON/envelopes use 400; unsupported media/encoding 415; oversized bodies 413; missing streams/routes 404; duplicate or not-yet-active streams, refused clock jumps (`clock_jump`) and reused batch IDs (`batch_conflict`) 409; partial buckets/numeric overflow 422; a closed handle 503; deadlines 504; canceled requests 408. Internal storage/sketch failures use a generic 500 without returning SQL or storage paths. Unknown methods use 405 and Allow. Normal engine results are unchanged. JSON responses and downloads use no-store and nosniff headers.

Server settings are 5-second header read, 30-second full read, 60-second write/idle timeouts and 16 KiB maximum headers. Engine request contexts have a 30-second deadline. GET /v1/snapshot is the exception: its export and download share a 30-minute budget for both the context and the write deadline, so a large database isn't cut off at 60 seconds. HTTP snapshots are staged in the system temp directory before sending; it needs room for a full copy of the database. This is not a hard transaction latency or mutex-wait guarantee: the handle serializes calls and waiting for its mutex is not context-aware. The server does not send an early success while an operation is still executing. A lost response or timeout does not prove a write rolled back. The caller must treat missing write acknowledgments conservatively: retry an ingest with the same batch ID, or reconcile first.

SIGINT/SIGTERM cancel active contexts and initiate shutdown with a ten-second grace before closing remaining connections. The CLI closes its database handle after the server returns; engine Close waits for active serialized methods. Application owners of transport.Serve retain responsibility for closing their DB afterward.

The snapshot endpoint exports into a private temporary directory, then returns an octet-stream attachment with Content-Length. It accepts no destination filesystem path. Export failure sends a JSON error before download; a disconnected or interrupted transfer is a partial file and must not be used. Check curl's successful exit and the complete response before opening/copying it. A new request exports a new snapshot, so range resume is unsupported. Temporary export space is a real limit, and a download is only complete when curl reports success.

## Prometheus remote write

`--remote-write MAP.json` turns on `POST /api/v1/write`. The map file is a JSON object with a `metrics` list; each entry has `metric` (the series' `__name__`), `stream` and `dimensions`, which maps every dimension of that stream to a label name. Each metric may appear once; several metrics may share a stream. The server checks the map against the database when it starts and refuses to start if a stream is missing or a dimension is unmapped. A series without one of the mapped labels gets an empty value for that dimension.

A request may hold at most 1,000,000 series and 1,000,000 samples after decoding, and two requests decode at a time; others wait. Requests must be remote write 1.0: `Content-Type: application/x-protobuf` (a `proto` parameter, if present, must be `prometheus.WriteRequest`) and `Content-Encoding: snappy`, at most 16 MiB compressed and 64 MiB decoded. Remote write 2.0 and other types get 415. Each float sample of a mapped metric becomes one observation at its millisecond timestamp. Stale markers, metrics not in the map, samples from before their stream's effective time (they could never be stored, so retrying would only stall Prometheus), exemplars, metadata, native histograms and labels no map entry uses are skipped. Samples are grouped per stream in the order they arrive and sent as batches of at most 10000, each with a batch ID made from the SHA-256 of the request body, the stream and the batch's position among that stream's batches, so a retried request replays what was already stored, even if the map gained other streams in between.

Responses follow what Prometheus expects. 204 means every batch committed (rejected samples, such as late ones, are counted, not retried). 400 is for a request that will never succeed: a body that isn't snappy or protobuf, a request over the caps, or an engine refusal of the batch itself; Prometheus drops it. 503 is for anything that may succeed later, such as a refused clock jump, a timeout or a closed database; Prometheus retries, and batches that committed before the failure are replayed rather than counted twice. Error bodies are plain text. Prometheus retries a 503 until it succeeds and holds back that shard's queue meanwhile, so a refused clock jump stops remote write until someone fixes the clock or raises `ROLLUPDB_MAX_CLOCK_STEP`. A slow upload holds one of the two decoding slots for up to the 30-second read timeout. `GET /api/v1/write/stats` returns requests, samples, submitted, unmapped, stale_markers, inactive, replayed_batches, the accepted and rejected counters, bad_requests and failed_requests since the server started.
