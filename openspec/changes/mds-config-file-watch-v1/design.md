## Decisions

1. **Restart instead of hot reload.** Adding or removing streams touches startup
   reconciliation, realtime subscriptions, recovery and historical queues, and status.
   Restarting through the existing startup path keeps one well-tested code path. Its cost
   is about a minute of `connecting`, measured on 2026-10-17 at about 48 s from start to
   BTC 5m `ready`.

2. **Content hash, polled.** The watcher reads the file every `MDS_CONFIG_WATCH_SECONDS`
   and compares a SHA-256 of its bytes with the last seen hash. This works on Docker
   Desktop bind mounts, where inotify events from the macOS host are not reliable, and
   with editors that replace the file. Reading one small file every 10 s costs nothing.

3. **Semantic comparison.** A restart happens only when the validated enabled coverage
   differs from the running config. Saving the file with a new comment does not restart
   MDS.

4. **Graceful stop, distinct exit code.** The watcher sets the same `stop_event` that
   SIGTERM sets, so realtime unsubscribes and in-flight historical passes end as on a
   normal stop. `serve` then returns `75`. A different code than a normal stop (`0`)
   makes the reason visible in `docker inspect` and in Docker events.

5. **Invalid edits never stop MDS.** A bad file is reported and ignored until it changes
   again, so a typo cannot take the live BTC/ETH streams down. A bad file can still fail a
   later cold start (crash, host reboot), as any invalid config does today. The ERROR log
   line says so.

6. **Off by default.** Standalone runs and tests behave as before. The stack turns it on
   with `MDS_CONFIG_WATCH_SECONDS=10`.
