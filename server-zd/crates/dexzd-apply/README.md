# dexzd-apply

## Purpose

Deterministic state machine that applies one server-zd command to Flow state and returns its writes, effects, and reply.

## Value and storage accounting

Each run tracks encoded Flow and History row bytes, including keys. Live and superseded value bytes use the encoded value sizes in birth records; garbage rows identify the superseded portion. Apply updates these totals from each command's writes and deletes.

The run state limit covers all four totals. A client command over the limit returns `RESOURCE_EXHAUSTED` without writes. A Worker result over the limit becomes a non-retryable attempt failure; that failure record is allowed so the Flow can close. The superseded-value cap advances the time travel window and collects values only after their last-holder sequence leaves it.
