"""Writes the remote-write fixtures: protobuf from the official protobuf
runtime (schema in prompb.proto) and snappy block compression from cramjam,
both independent of the Go decoder under test. Run from this folder:
python3 -m grpc_tools.protoc -I. --python_out=/tmp/rwgen prompb.proto
PYTHONPATH=/tmp/rwgen python3 generate.py"""
import json, struct
import cramjam
import prompb_pb2 as pb

STALE = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]
T0 = 1792929720000  # 2026-10-25T12:02:00Z in milliseconds

def series(labels, samples):
    ts = pb.TimeSeries()
    for k, v in labels:
        ts.labels.add(name=k, value=v)
    for v, t in samples:
        ts.samples.add(value=v, timestamp=t)
    return ts

def write(name, req, expected):
    raw = req.SerializeToString()
    with open(name + ".bin", "wb") as f:
        f.write(bytes(cramjam.snappy.compress_raw(raw)))
    with open(name + ".json", "w") as f:
        json.dump(expected, f, indent=1, sort_keys=True)
        f.write("\n")

# basic: two mapped series, a stale marker, an unmapped metric, plus an
# exemplar, a native histogram series and metadata that must be skipped.
req = pb.WriteRequest()
a = series([("__name__", "http_requests_total"), ("job", "api"), ("instance", "a:9090"), ("code", "200")],
           [(1.0, T0), (2.5, T0 + 1000), (STALE, T0 + 2000)])
a.exemplars.add(value=9.0, timestamp=T0).labels.add(name="trace_id", value="abc")
req.timeseries.append(a)
req.timeseries.append(series([("__name__", "http_requests_total"), ("job", "web")], [(7.0, T0 + 500)]))
req.timeseries.append(series([("__name__", "go_goroutines"), ("job", "api")], [(42.0, T0)]))
h = pb.TimeSeries()
h.labels.add(name="__name__", value="latency_seconds")
h.histograms.add(count_int=3, sum=1.5, schema=1, timestamp=T0)
req.timeseries.append(h)
req.metadata.add(type=1, metric_family_name="http_requests_total", help="Requests.", unit="")
write("basic", req, {
    "samples": 5,
    "stale_markers": 1,
    "unmapped": 1,
    "observations": [
        {"time_ms": T0, "value": 1.0, "job": "api", "instance": "a:9090"},
        {"time_ms": T0 + 1000, "value": 2.5, "job": "api", "instance": "a:9090"},
        {"time_ms": T0 + 500, "value": 7.0, "job": "web", "instance": ""},
    ],
})

# large: 12000 samples of one series (more than one batch, repetitive enough
# that snappy uses copies), values 0..11999 at 1 ms steps.
req = pb.WriteRequest()
req.timeseries.append(series([("__name__", "http_requests_total"), ("job", "bulk"), ("instance", "b")],
                             [(float(i), T0 + i) for i in range(12000)]))
write("large", req, {"samples": 12000, "stale_markers": 0, "unmapped": 0, "sum": 12000 * 11999 / 2, "first_ms": T0, "last_ms": T0 + 11999})
print("ok")
