## Decisions

1. **Directory mount, read-only.** Mount `${BBB_DATA_ROOT}/mds` rather than the single
   file. Editors that save by writing a new file and renaming it over the old one leave a
   single-file bind mount pointing at the stale inode. MDS never writes its config.

2. **The existing setting selects the file.** `MDS_MARKETS_CONFIG_PATH` already exists;
   the stack only changes its value. If the external file is missing, MDS fails at
   startup with its config-not-found error. It never falls back silently to the baked
   list.

3. **Seed, not sync.** The host file is created once from the pinned image's config.
   After that it is the source of truth for the live stack. The repo copy is the default
   for tests and standalone runs.

4. **Restart instead of hot reload.** Adding or removing streams touches startup
   reconciliation, realtime subscriptions, the recovery and historical queues, and status.
   Restarting through the existing startup path keeps one tested code path, at the cost of
   about a minute of `connecting`.

5. **Polling a content hash.** Every `MDS_CONFIG_WATCH_SECONDS` the watcher reads the file
   and compares a SHA-256 of its bytes with the last seen value. This works on Docker
   Desktop bind mounts, where host inotify events are unreliable, and with editors that
   replace the file.

6. **Semantic comparison.** MDS restarts only when the validated enabled coverage differs:
   instruments, exchange symbols, enabled flags, timeframes, history policy. Comments and
   formatting do nothing.

7. **Graceful stop with a distinct exit code.** The watcher sets the same `stop_event` as
   SIGTERM, so realtime unsubscribes and historical passes end as on a normal stop. `serve`
   then returns `75`, which makes the reason visible in `docker inspect` and in Docker
   events.

8. **Invalid edits never stop MDS.** A bad file is logged once and ignored until it
   changes again, so a typo cannot take the live BTC/ETH streams down. A bad file can still
   fail a later cold start, as any invalid config does today; the ERROR line says so.

9. **Off by default.** Standalone runs and tests behave as before. The stack enables it
   with `MDS_CONFIG_WATCH_SECONDS=10`.
