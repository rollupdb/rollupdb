# Streaming imports and local HTTP

The CLI streaming input and the HTTP service are adapters over the embedded engine. There is no format change: databases remain revision 4 with immutable initial stream definitions. Both adapters use the same engine validation, finest-tier grace, counters, quantile settings, explicit retention and commit semantics. They do not create a raw-event store or deduplicate retries.

## NDJSON command

```
rollupdb ingest-ndjson FILE STREAM INPUT [--batch-size N]
```

INPUT is a UTF-8 file, or `-` for stdin. Each physical line contains exactly one Observation object with explicit nonnull time/value and the exact declared dimensions. LF and CRLF are accepted, as is a final line without a newline. Empty lines, a BOM, null, unknown fields, malformed JSON, invalid UTF-8 and trailing objects are errors. JSON duplicate keys follow encoding/json's last-value behavior; this is not duplicate-key detection. As with JSON batches, a structurally valid observation can be rejected individually by the engine for timestamp, dimensions, numeric overflow, lateness or group capacity.

Records are limited to 256 KiB excluding LF/optional CR. Batch size defaults to 1000 records and must be in [1,10000]. A batch is also flushed before buffered serialized records would exceed 8 MiB. Those are input bounds, not an overall process memory promise: decoded maps, engine caches and sketches require additional memory. The importer does not buffer the entire file. Parsing the next record happens before a byte-capacity flush, so encountering malformed input does not unexpectedly commit the preceding pending batch.

Every successful engine batch produces one JSON-line receipt with type=batch, from_line, through_line and the complete BatchResult, including per-observation outcomes. A final JSON line has type=complete or error and reports committed_records, committed_batches, accepted/rejection totals, next_line, failed_line where applicable, discarded_records, unacknowledged_records and an error message. The exit status is nonzero on failure. committed_records includes rejected observations whose outcomes/counters committed, not just accepted data. next_line is one past the last successfully committed batch, not the scanner's read-ahead position.

An invalid line or input read error discards the pending not-yet-submitted observations and preserves earlier commits. The bad record itself is identified by failed_line; discarded_records counts parsed records that were never submitted. An engine error reports the submitted batch as unacknowledged_records. A commit-level I/O failure can be ambiguous, and a receipt/output failure may happen after a real commit. Do not blindly retry the whole input: resubmission is new input and can duplicate observations. Keep receipts and resolve an ambiguous write before choosing a replay point. An output failure may also prevent delivery of the final summary.

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

## HTTP service

```
rollupdb serve FILE [--listen 127.0.0.1:7070]
```

The file must already exist in the current format. The command never initializes or migrates it. Its first JSON line reports the actual listening address, including when port 0 chooses a free port. Only literal loopback IPs are allowed: 127.0.0.1 or [::1], for example. Wildcards, LAN IPs and hostnames are refused. Keep this as the one owning process for the file; use HTTP operations while it is running rather than opening another CLI writer.

There is no authentication or TLS. This is a trusted local-process interface, not a multi-user security boundary. Host values must be localhost or a literal loopback address; requests with any Origin header are refused and CORS is not enabled. These checks help keep browser requests out; they do not authenticate a local client. The exported transport.NewHandler must also be placed behind a loopback listener by its owner. The CLI cannot opt into public binding.

| Method | Path | Body / result |
| --- | --- | --- |
| GET | /v1/info | Engine Info |
| POST | /v1/streams | Definition; 201 on creation |
| GET | /v1/streams/NAME | Immutable Definition |
| GET | /v1/streams/NAME/stats | Counters, groups, buckets, coverage and frontiers |
| POST | /v1/ingest | One Batch; BatchResult after commit |
| POST | /v1/query | Query; exact statistics, quantiles and coverage |
| POST | /v1/streams/NAME/advance | Empty object {}; explicit retention result |
| GET | /v1/snapshot | Standalone snapshot download |

JSON POST requests require Content-Type: application/json, uncompressed UTF-8 object bodies, no unknown fields and no trailing objects. Other media types and compressed requests are refused. Ingest bodies are limited to 8 MiB; definitions, queries and advancement bodies to 1 MiB. Engine batches still have at most 10000 observations. HTTP ingestion is a single atomic JSON batch; streaming NDJSON is currently a CLI interface. Query parameters are unsupported.

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

Errors are JSON objects shaped as {"error":{"code":"partial_bucket","message":"requested range cuts a retained bucket"}}. Bad JSON/envelopes use 400; unsupported media/encoding 415; oversized bodies 413; missing streams/routes 404; duplicate or not-yet-active streams and refused clock jumps (`clock_jump`) 409; partial buckets/numeric overflow 422; a closed handle 503; deadlines 504; canceled requests 408. Internal storage/sketch failures use a generic 500 without returning SQL or storage paths. Unknown methods use 405 and Allow. Normal engine results are unchanged. JSON responses and downloads use no-store and nosniff headers.

Server settings are 5-second header read, 30-second full read, 60-second write/idle timeouts and 16 KiB maximum headers. Engine request contexts have a 30-second deadline. GET /v1/snapshot is the exception: its export and download share a 30-minute budget for both the context and the write deadline, so a large database isn't cut off at 60 seconds. HTTP snapshots are staged in the system temp directory before sending; it needs room for a full copy of the database. This is not a hard transaction latency or mutex-wait guarantee: the handle serializes calls and waiting for its mutex is not context-aware. The server does not send an early success while an operation is still executing. A lost response or timeout does not prove a write rolled back. The caller must treat missing write acknowledgments conservatively.

SIGINT/SIGTERM cancel active contexts and initiate shutdown with a ten-second grace before closing remaining connections. The CLI closes its database handle after the server returns; engine Close waits for active serialized methods. Application owners of transport.Serve retain responsibility for closing their DB afterward.

The snapshot endpoint exports into a private temporary directory, then returns an octet-stream attachment with Content-Length. It accepts no destination filesystem path. Export failure sends a JSON error before download; a disconnected or interrupted transfer is a partial file and must not be used. Check curl's successful exit and the complete response before opening/copying it. A new request exports a new snapshot, so range resume is unsupported. Temporary export space and non-atomic download publication are real limits, just as with the CLI snapshot command.
