# Standalone ParQ reader profiler

Build the executable from the project root:

```sh
make profile-reader
```

Run one file read, optionally with warmups and repeated measured passes:

```sh
build/parq_read_profile data.parquet --warmup 1 --iterations 5
```

The tool calls ParQ's C++ reader and conversion code directly. It does not
start q or load `ParQ.so`; `src/l64/c.o` supplies the K allocation functions
needed to measure ParQ's normal q-table conversion path. This allows the
process to be profiled as an ordinary C++ executable while retaining the cost
of converting Parquet columns into K arrays.

The output separates open/metadata time, row-group reads plus K conversion,
release of the returned K tables, and close time. It reports each row group's
row count and elapsed time, then gives average timings and throughput.
Throughput in MiB/s uses the compressed Parquet file size, so it is an estimate
of file-size throughput rather than decoded memory bandwidth.

For a CPU profile with Linux `perf`:

```sh
perf record -g --call-graph dwarf -- build/parq_read_profile data.parquet --warmup 1 --iterations 10
perf report
```

Use a file large enough to make the read phase measurable. Warmups can reduce
the impact of first-use initialization and filesystem cache effects; they do
not remove those effects entirely.
