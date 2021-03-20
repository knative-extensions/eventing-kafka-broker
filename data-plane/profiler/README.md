# Profiler Job

The [`run`](./run.sh) script attaches [async profiler](https://github.com/jvm-profiling-tools/async-profiler)
to receiver and dispatcher, that are configured to handle events for resources
in [`resources/ingress.json`](./resources/ingress.json).

Async profiler needs to capture kernel stacks using `perf_events` and this requires setting two environment variables:

```shell
sysctl kernel.perf_event_paranoid=1
sysctl kernel.kptr_restrict=0
```

for that reason, it will ask for sudo access to continue.

_Note: You can set those variables yourself and then run the script._
