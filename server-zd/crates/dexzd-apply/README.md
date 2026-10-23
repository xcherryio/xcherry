# dexzd-apply

## Purpose

Deterministic state machine that applies one server-zd command to Flow state and returns its writes, effects, and reply.

## Value and storage accounting

Each run tracks encoded Flow and History row bytes, including keys. Live and superseded value bytes use the encoded value sizes in birth records; garbage rows identify the superseded portion. Apply updates these totals from each command's writes and deletes.

The run state limit covers Flow state rows and live and superseded values. History bytes are tracked separately. A client value write that grows active state beyond the limit returns `RESOURCE_EXHAUSTED` without writes. A Worker result that does so becomes a non-retryable attempt failure. Attempt starts, timers, stop, and purge can proceed when the run is full. The superseded-value cap advances the time travel window through semantic points and collects values only after their last-holder sequence leaves it. The current semantic point always remains available, so a command can temporarily leave superseded bytes over the cap.
