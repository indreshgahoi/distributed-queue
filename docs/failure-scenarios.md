# Failure Scenarios

## F1 — Consumer crashes after receive

Initial state:

READY = [M1]

Sequence:

1. Consumer C1 calls `receive()`.
2. M1 is removed from the queue.
3. C1 crashes before processing completes.
4. M1 is no longer available.

Result:

The message is permanently lost.

## Violated Requirement

A consumer failure must not permanently remove unprocessed work.

## Required Behavior

Receiving a message should transfer temporary ownership
to a consumer instead of permanently deleting the message.

A message should become permanently removable only after
successful acknowledgement.

## F2 — Consumer crashes while message is IN_FLIGHT

Initial state:

READY = [M1]

Sequence:

1. Consumer C1 calls `receive()`.
2. M1 moves from READY to IN_FLIGHT.
3. C1 crashes before calling `ack()`.
4. No consumer can receive M1 again.

Result:

The message is not lost, but it is stuck indefinitely.

## Violated Requirement

Unacknowledged work must eventually become available again.

## Required Behavior

Ownership of an IN_FLIGHT message must be temporary.

If acknowledgement does not arrive before the ownership period
expires, the message must return to READY.

## F3 — Stale consumer acknowledgement

Initial state:

M1 is delivered to C1 with receipt handle R1.

Sequence:

1. C1 receives M1 using R1.
2. C1 stops making progress.
3. R1's lease expires.
4. M1 returns to READY.
5. C2 receives M1 with a new receipt handle R2.
6. C1 wakes up and tries to acknowledge R1.

Required behavior:

R1 must no longer be valid.

C1's acknowledgement must not remove C2's active delivery.

Invariant:

Only the current active delivery may be acknowledged.

## F4 — Poison message retries forever

Sequence:

1. M1 is delivered.
2. Processing fails.
3. Lease expires.
4. M1 is redelivered.
5. Processing fails again.
6. The cycle repeats indefinitely.

Result:

The message consumes queue capacity and worker effort forever.

Required behavior:

Retries must be bounded.

After the configured maximum delivery attempts,
the message must leave the normal READY/IN_FLIGHT lifecycle
and move to DEAD_LETTER.

## F5 - Know failure has to wait for the lease to expire before redelivery

Sequence:
1. M1 is delivered to C1.
2. C1 fails in M1 processing
3. M1 has to wait for the lease to expire before it can be redelivered to another consumer.

Result:
 C1 nack(M1) as soon as it knows that M1 processing has failed, but M1 is not redelivered until the lease expires.

Required behavior:
 A consumer should be able to explicitly reject its current delivery using nack() and request retry after a delay/backoff without waiting for the lease to expire.

## F5 — Process crashes after publish

T0  Producer publishes M1
T1  publish() returns success
T2  JVM crashes
T3  JVM restarts

Current behavior:
M1 is gone.

Required behavior:
If publish() returned success, M1 must be recoverable
after restart.
> A state transition must not be reported successful until the corresponding durable record has been written according to the queue's durability contract.

## F6 — NACKed Message Loses Its Retry Schedule After Restart

### Initial State

M1 is currently IN_FLIGHT on delivery attempt 1.

```text
M1
state = IN_FLIGHT
attempt = 1
receiptHandle = R1
```
Sequence
1. Consumer processing fails.
2. Consumer calls:
   nack(R1, 30 seconds)
3. The queue moves M1 to:
   DELAYED
   nextAttempt = 2
   retryAt = 10:00:30
4. nack() returns success.
5. The JVM crashes at 10:00:10.
6. The queue restarts at 10:00:15.
   Current Behavior
   If only PUBLISH and ACK are durable, recovery sees:
   PUBLISH M1
   but does not know that M1 was successfully NACKed.
   Recovery therefore reconstructs:
   M1 -> READY
   immediately.
   **Problem**
   The restart changes externally visible queue behavior.
   M1 becomes eligible before its previously accepted retry time.
   This violates the successful NACK contract.
   **Violated Requirement**
   A successful state transition that changes future message eligibility must
   survive restart.
   **Required Behavior**
   The NACK transition must be durably recorded before nack() returns success.
   The durable record must contain enough information to reconstruct:
   message identity
   next delivery attempt
   absolute retry eligibility time
   After restart:
   now < retryAt
   -> remain DELAYED

   now >= retryAt
   -> eligible to move to READY

## F7 — NACK WAL Failure After In-Memory Removal
Sequence
1. M1 is IN_FLIGHT with receipt handle R1.
2. nack(R1, 30s) begins.
3. The implementation removes R1 from IN_FLIGHT.
4. WAL append fails.
5. nack() throws an exception.    
   **Incorrect Result**  
   M1 is no longer IN_FLIGHT, but the NACK was not durable.  
   The queue has lost the active ownership state despite the operation failing.  
   **Required Ordering**  
   validate receipt handle    
   |  
   calculate retryAt  
   |  
   append NACK WAL record    
   |  
   WAL succeeds    
   |  
   remove IN_FLIGHT    
   |  
   add DELAYED    
   If WAL append fails:    
   IN_FLIGHT must remain unchanged    
   **Invariant**  
   > A failed durable transition must not partially mutate the in-memory queue state.  

## F8 — Retry Delay Is Recalculated Incorrectly After Restart
**Sequence**
10:00:00 nack(M1, 30 seconds)  
10:00:10 JVM crashes  
10:00:20 JVM restarts  
**Incorrect Recovery**  
If the WAL stores only:  
retryDelay = 30 seconds 
and recovery calculates: 
retryAt = restartTime + retryDelay  
then M1 becomes eligible at:  
10:00:50  
**Correct Recovery**  
The original accepted retry time was:  
10:00:30  
Therefore the WAL must preserve:  
retryAt = 10:00:30  
**Mental Model**  
Persist decisions, not enough information to accidentally make a different
decision during recovery.

## F9 — Lease Expiry Attempt Is Lost After Restart

### Sequence

1. M1 is published.
2. M1 is delivered as attempt 1 with receipt R1.
3. R1's visibility lease expires.
4. `requeueExpiredMessages()` successfully requeues M1.
5. M1 is now READY for attempt 2.
6. JVM crashes.
7. Queue restarts.

### Current Durable History

PUBLISH M1

### Incorrect Recovery

M1 -> READY(attempt=1)

### Required Recovery

M1 -> READY(attempt=2)

### Root Cause

The lease-expiry transition exists only in volatile memory.

### Required Fix

Persist the lease-expiry transition before changing the in-memory state.

## F10 — WAL Failure During Lease Expiry

### Sequence

M1 = IN_FLIGHT(R1, attempt=1)
|
lease expires
|
LEASE_EXPIRED WAL append fails

### Required Result

M1 must remain IN_FLIGHT under R1.

The queue must not expose a partially completed transition.

### Invariant

A failed durable state transition must leave the previous runtime state intact.

## F11 — Crash During WAL Record Write

### Sequence

1. PUBLISH M1 is durably written.
2. PUBLISH M2 is durably written.
3. PUBLISH M3 begins.
4. The process crashes after only part of M3 reaches the WAL.

The file may contain:

    [M1 complete]
    [M2 complete]
    [M3 partial]

### Risk

Without explicit framing, recovery may be unable to determine whether
the final bytes represent:

- a valid record,
- an incomplete record,
- or corrupted data.

### Required Direction

Each WAL entry must carry an explicit record length.

Recovery can therefore distinguish:

    complete frame
        vs
    incomplete trailing frame

F13 — New code interprets an old WAL using the wrong physical format

```text
Old file:
[length][payload]

New decoder expects:
[length][payload][checksum]
```

Without versioning:
bytes may be misinterpreted as valid framing/checksum data.

Required behavior:
detect unsupported format before record recovery begins.

The key mental model is:
> Data formats are APIs too. Once bytes can survive longer than the process that wrote them, format compatibility becomes an architectural concern.


## F14 — Unbounded WAL Growth

### Sequence

The queue runs for a long period.

Messages are continuously:

- published
- acknowledged
- retried
- dead-lettered

Most historical messages are already DONE, but their WAL records remain.

### Result

The WAL grows monotonically.

Recovery time becomes proportional to total historical operations rather
than current queue state.

Disk consumption also grows without bound.

### Required Direction

Periodically persist a snapshot of the current logical queue state.

Once a snapshot is safely durable, WAL history represented by that snapshot
may eventually be compacted or discarded according to a defined policy.

## F15 — Delivery Lease Is Lost After Queue Restart

### Initial State

M1 is READY.

### Sequence

1. Consumer calls `receive()`.
2. Queue removes M1 from READY.
3. Queue creates:

       receiptHandle = R1
       attempt = 1
       leaseUntil = 10:30

4. M1 becomes IN_FLIGHT.
5. Delivery is returned to the consumer.
6. Queue process crashes at 10:10.
7. Queue restarts at 10:12.

### Current Failure Without Durable Delivery State

If the WAL contains only:

    PUBLISH M1

recovery reconstructs:

    M1 -> READY

The same message can therefore be delivered again at 10:12 even though the
original lease was valid until 10:30.

### Problem

The queue forgot an externally visible ownership decision.

Restart changed delivery semantics.

### Required Behavior

Before exposing the delivery, persist:

    LEASE_STARTED
    messageId = M1
    receiptHandle = R1
    attempt = 1
    leaseUntil = 10:30

Recovery must restore:

    M1 -> IN_FLIGHT(R1)

until the lease terminates normally.


## F16 — Consumer Cannot ACK After Queue Restart

### Sequence

1. Consumer receives M1 with receipt handle R1.
2. Consumer processes M1 successfully.
3. Queue process restarts before ACK reaches the queue.
4. Consumer retries:

       ack(R1)

### Incorrect Policy

If queue restart invalidates all receipt handles:

    ack(R1) -> false

Eventually the lease expires and M1 is redelivered despite successful
processing.

### Required Behavior

Queue restart alone must not invalidate R1.

If the recovered lease is still active:

    ack(R1) -> true

Receipt-handle validity is tied to the delivery lease, not the lifetime of
one JVM process.


## F17 — receive() Exposes Ownership Before It Is Durable

### Incorrect Ordering

    remove M1 from READY
        |
        v
    create R1
        |
        v
    return Delivery
        |
        v
    append LEASE_STARTED

If the process crashes after returning the delivery but before the WAL append,
the consumer believes it owns M1 while recovery believes M1 is READY.

### Required Ordering

    identify READY M1
        |
        v
    create R1 + leaseUntil
        |
        v
    append LEASE_STARTED
        |
        v
    cross durability boundary
        |
        v
    READY -> IN_FLIGHT
        |
        v
    return Delivery

### Invariant

Externally visible ownership must never exist only in volatile memory.


## F18 — LEASE_STARTED WAL Failure Partially Changes Queue State

### Sequence

1. M1 is READY.
2. `receive()` begins.
3. Implementation removes M1 from READY.
4. LEASE_STARTED WAL append fails.

### Incorrect Result

M1 is no longer READY but no durable delivery lease exists.

The message may effectively disappear until another repair path detects it.

### Required Result

If LEASE_STARTED cannot be made durable:

    M1 remains READY
    no active receipt handle exists
    receive() does not return a Delivery

### Principle

A failed durable transition must leave the previous authoritative state intact.

## F19 — Crash While Writing Candidate Snapshot

Existing:

    snapshot.dat = S1

Process begins:

    snapshot.tmp = partial S2

Then crashes.

Required recovery:

    loadLatest() -> S1

The partial candidate must not replace S1.


## F20 — Candidate force fails

Existing:

    snapshot.dat = S1

Candidate bytes may have been written, but the durability operation fails.

Required behavior:

    save(S2) fails
    S1 remains authoritative
    no later code may assume S2 became durable


## F21 — Promotion fails

Candidate S2 is complete and durable, but rename/replacement fails.

Required behavior:

    save(S2) fails

The system must not report successful snapshot replacement unless promotion
itself succeeds.
```text
                S1 AUTHORITATIVE
                       |
                 create candidate S2
                       |
                 +-----+------+
                 |            |
               FAIL         complete
                 |            |
                 v            v
                S1        force S2
                              |
                        +-----+------+
                        |            |
                      FAIL         durable
                        |            |
                        v            v
                       S1         promote
                                     |
                               +-----+------+
                               |            |
                             FAIL         success
                               |            |
                               v            v
                              S1            S2
```
## F22 — WAL Is Compacted Before Snapshot Promotion

### Sequence

Existing recovery path:

    S1 + WAL

A newer snapshot S2 is being created.

Incorrect implementation performs:

    capture S2
        |
        v
    delete WAL history covered by S2
        |
        v
    promote S2
        |
      FAILURE

### Result

The old WAL history has already been removed.

S2 is not authoritative.

Recovery may no longer have a complete source of state.

### Required Behavior

Compaction must only begin after S2 has been successfully promoted.

Required order:

    durable candidate
    -> atomic promotion
    -> compaction
## F23 — Compaction Crosses the Snapshot Boundary

### State

Snapshot:

    WalPosition(7, 1200)

Recovery requires:

    snapshot
    +
    WAL from (7, 1200)

### Failure

Compaction accidentally removes history through:

    (7, 1800)

Records between 1200 and 1800 existed after the snapshot and are not
represented in it.

### Result

Recovery silently loses state transitions.

### Required Behavior

No history at or after the authoritative snapshot position may be reclaimed.

## F24 — Temporary Snapshot Advances Compaction Point

### Sequence

S1 is authoritative.

S2.tmp contains:

    WalPosition(9, 500)

but S2 promotion has not completed.

Compaction incorrectly reads the temporary candidate and deletes old WAL
segments.

The process then crashes before S2 becomes authoritative.

### Required Behavior

Temporary snapshot files must never participate in compaction decisions.

Only the committed snapshot path is authoritative.

## F25 — Compaction Fails After Deleting Some Eligible History

### Future segmented example

Authoritative snapshot:

    WalPosition(7, 1200)

Eligible segments:

    segment-1
    segment-2
    segment-3
    segment-4
    segment-5
    segment-6

Compaction deletes:

    segment-1
    segment-2
    segment-3

then filesystem operation fails.

### Required Behavior

Recovery must remain valid.

Deleting only a subset of eligible history is safe.

The system may retry remaining cleanup later.

### Principle

Compaction progress need not be atomic.

Compaction safety must be atomic with respect to the recovery boundary.

## F26 — Stale Snapshot Produces Older Compaction Point

Current compaction point:

    WalPosition(8, 100)

A stale snapshot appears with:

    WalPosition(7, 500)

### Risk

If the implementation treats every snapshot as authoritative without
monotonicity checks, storage lifecycle becomes inconsistent.

### Required Behavior

Compaction position must never move backward.

A stale snapshot must not resurrect or redefine already reclaimed history.

## F27 — Snapshot/WAL Position Mismatch

Snapshot claims:

    WalPosition(12, 500)

Available WAL only contains:

    segment 0

or the referenced position is not a valid frame boundary.

### Possible Causes

- snapshot copied from another WAL;
- stale files mixed together;
- manual filesystem modification;
- implementation bug.

### Required Behavior

Do not compact.

Fail recovery/compaction validation explicitly.

Never infer a safe deletion boundary from an invalid WalPosition.

## F28 — Compaction Deletes an Active WAL Segment

Writer is currently appending to:

    segment-8

Snapshot covers:

    segment-7

Compaction incorrectly treats segment-8 as reclaimable because of a race
or stale segment metadata.

### Required Behavior

The active WAL segment is never eligible for deletion.

Only immutable, fully closed segments strictly older than the snapshot's
segment boundary may eventually be reclaimed.

## F29 — Crash While Creating New Segment Candidate

Existing:

    segment-3.wal

Rotation begins:

    segment-4.tmp

Process crashes before candidate initialization completes.

Required recovery:

    ignore segment-4.tmp
    active segment remains segment-3.wal

## F30 — Fully Written Temp Segment Exists After Crash

State:

    segment-3.wal
    segment-4.tmp   <- complete and durable

Crash occurs before atomic promotion.

Required recovery:

    segment-4.tmp is not authoritative
    active segment remains segment-3.wal

The presence of a valid temporary file must not change recovery state.

## F30 — Fully Written Temp Segment Exists After Crash

State:

    segment-3.wal
    segment-4.tmp   <- complete and durable

Crash occurs before atomic promotion.

Required recovery:

    segment-4.tmp is not authoritative
    active segment remains segment-3.wal

The presence of a valid temporary file must not change recovery state.

## F31 — Crash Immediately After Segment Promotion

State:

    segment-3.wal
    segment-4.wal

Crash occurs before any record is appended to segment 4.

Required recovery:

    segment 4 is active
    segment 3 is sealed
    an empty/header-only active segment is valid

## F32 — Append Accidentally Continues to Sealed Segment

State:

    segment-3.wal
    segment-4.wal

If implementation later appends another record to segment 3, durable history
can become physically reordered.

Required behavior:

    only the highest authoritative segment may accept appends

## F33 — WAL Frame Split Across Two Segments

Incorrect implementation:

    segment-3:
        [length][partial payload]

    segment-4:
        [remaining payload][checksum]

Required behavior:

    reject this design entirely.

A WAL frame must be atomic with respect to segment membership.

## F34 — Gap in Segment IDs

Directory contains:

    segment-1.wal
    segment-2.wal
    segment-4.wal

segment 3 is missing.

Possible causes:

- accidental deletion
- partial filesystem restoration
- operator error
- implementation bug

Required policy:

    recovery fails explicitly.

Do not silently assume segment 4 follows segment 2.

## F35 — Duplicate/Invalid Segment Names

Examples:

    segment-000003.wal
    segment-3-copy.wal
    foo.wal

Required behavior:

    authoritative segment discovery must use the exact supported naming format.

Unexpected files must not silently participate in recovery.

## F36 — Torn Tail in Sealed Segment

Directory:

    segment-2.wal   <- incomplete tail
    segment-3.wal   <- valid newer segment

Because segment 3 already became authoritative, segment 2 was expected to be
complete before rotation.

Required behavior:

    fail recovery.

Do not truncate sealed history silently.

## F37 — Snapshot Missing After WAL Prefix Reclamation

The authoritative snapshot covers history before segment 7. Segments 0–6
have been reclaimed, but the snapshot is later missing at restart.

Required behavior:

    fail startup explicitly

The retained WAL suffix must never be mistaken for complete history.

## F38 — Snapshot Corrupt After WAL Prefix Reclamation

The WAL begins at segment 7 and the committed snapshot fails checksum or
format validation.

Required behavior:

    fail startup explicitly

Falling back to `readAll()` would silently omit state represented only by the
snapshot.

## F39 — Stale Snapshot Commit After Coordinator Restart

The committed snapshot is at `(8, 100)` and earlier WAL segments have already
been reclaimed. After restart, a caller attempts to commit `(5, 300)`.

Required behavior:

    reconstruct boundary (8, 100) from committed snapshot
    reject stale candidate before replacement

Restart must not reset monotonic snapshot authority.

## F40 — Candidate Snapshot References Invalid WAL Position

A candidate snapshot names an unknown segment or a byte offset that is not a
frame boundary.

Required behavior:

    reject candidate before promotion
    preserve previous authoritative snapshot
    do not advance boundary
    do not reclaim WAL history

## F41 — Power Loss After Snapshot Rename

The snapshot candidate is forced and atomically renamed, but the containing
directory entry has not crossed a durability boundary before power loss.

Required behavior:

    force the parent directory after rename
    report snapshot save success only after that force succeeds

If directory force fails, compaction must not begin.

## F42 — Power Loss After WAL Segment Promotion

A new segment is forced and renamed from `.tmp` to `.wal`, but its directory
entry is not durable. An append into that segment must not be reported durable
while the segment name itself can disappear after power loss.

Required behavior:

    force WAL directory after promotion
    poison writer if the post-promotion force fails
    require restart before another append

## F43 — Segment Deletion Metadata Is Not Durable

Several reclaimed segment files are deleted without forcing the directory.
After power loss, deletion persistence may differ from the order observed by
the process and can expose an unexpected prefix or gap.

Required behavior:

    delete one eligible segment
    force WAL directory
    only then continue to the next segment

A force failure stops cleanup; recovery safety remains more important than
reclaiming all eligible space.

## F44 — Snapshot Is Never Scheduled

The queue runs correctly for months, but no caller invokes snapshot and
compaction coordination. Segmented WAL converts one growing file into an
unbounded number of files.

Required behavior:

    observe durable WAL segment progress
    automatically checkpoint after configured segment distance
    reclaim only after authoritative promotion

## F45 — Snapshot Save Fails During Scheduled Maintenance

The policy requests a checkpoint, but candidate writing, forcing, or promotion
fails before a new snapshot is authoritative.

Required behavior:

    preserve existing recovery authority
    expose maintenance failure
    capture fresh queue state on a later eligible cycle
    do not reclaim from the failed candidate

## F46 — Reclamation Fails After Snapshot Commit

The new snapshot is authoritative, but deleting an eligible segment fails.
No further WAL segment rotation occurs before the next maintenance cycle.

Required behavior:

    remember/reconstruct authoritative snapshot position
    retry reclamation without requiring new WAL progress

## F47 — Scheduled Cycle Throws

A fixed-delay maintenance task encounters a transient storage exception. If
the exception escapes the scheduler task, subsequent execution may be
cancelled permanently.

Required behavior:

    record failure for observation
    contain exception at scheduler boundary
    continue later maintenance cycles

## F48 — Concurrent Manual and Scheduled Maintenance

An operator invokes maintenance while the scheduled cycle is capturing or
committing a snapshot.

Required behavior:

    serialize both paths through one lifecycle state machine
    never run overlapping snapshot commits or reclamation passes

## F49 — Snapshot Copied from Another Queue Generation

A valid snapshot file is copied beside a WAL from another queue or from an
older deleted-and-recreated generation. Its position may coincidentally be a
valid frame boundary.

Required behavior:

    compare the complete storage lineage before restoring state
    fail recovery on mismatch
    never fall back by treating the mismatch as ordinary corruption

## F50 — Foreign Segment Appears in a WAL Directory

An operator, restore process, or partial deployment mixes a valid segment from
another storage lineage into the authoritative segment sequence.

Required behavior:

    validate every segment header during startup
    reject the directory before replay or append
    do not repair, rename, or silently ignore the foreign segment

## F51 — Foreign Snapshot Presented to Compaction

A structurally valid snapshot from another lineage references a plausible WAL
position. Position-only validation would allow it to advance the compaction
boundary and delete the real recovery history.

Required behavior:

    validate lineage before snapshot promotion
    preserve the prior snapshot and boundary on mismatch
    perform no WAL deletion

## F52 — Create Response Is Lost After Database Commit

PostgreSQL commits queue creation, but the service or network fails before the
client receives its response.

Required behavior:

    retry with the same tenant-scoped idempotency key
    return the originally allocated queue and generation IDs
    never create a second descriptor

## F53 — Idempotency Key Is Reused for Different Parameters

A client accidentally reuses a create key for another queue name.

Required behavior:

    compare the canonical request hash inside the transaction
    reject the conflicting request
    preserve the original response mapping

## F54 — Concurrent Creates Target One Queue Name

Different requests concurrently create the same tenant/name.

Required behavior:

    commit at most one live descriptor
    reject the loser through the database uniqueness boundary
    roll back the loser's idempotency reservation

## F55 — Stale Provisioner Completes a Transition

A delayed worker reports completion after another actor already changed the
descriptor, or after the queue generation was replaced.

Required behavior:

    compare generation ID, expected state, and metadata version
    update exactly one row or fail as stale
    never overwrite newer authority

## F56 — Metadata Commits but Queue Storage Does Not Exist

Queue creation commits while no queue node has yet created lineage-matched
storage.

Required behavior:

    expose the descriptor as PROVISIONING, not ACTIVE
    do not route data-plane operations to it
    leave provisioning and reconciliation to the next milestone

## F57 — Delete Is Interrupted Before Physical Cleanup

The descriptor reaches `DELETING`, but the process fails before storage is
removed or deletion is completed.

Required behavior:

    preserve DELETING across service restart
    make repeated customer deletion idempotent
    do not free the live tenant/name until DELETED is authoritative

## F58 — Provisioner Crashes Before Creating Storage

The worker holds a claim but exits before materializing the WAL.

Required behavior:

    leave metadata in PROVISIONING
    permit takeover only after the claim lease expires
    issue a strictly greater fencing token to the next worker

## F59 — Provisioner Crashes After Storage Creation

Lineage-matched storage is durable, but the worker exits before reporting
completion.

Required behavior:

    keep metadata in PROVISIONING
    allow a later claimant to reopen and validate the same lineage
    make repeated materialization harmless
    activate only through a current fenced completion

## F60 — Stale Worker Completes After Takeover

Worker A pauses, its lease expires, and worker B takes over with a greater
token. Worker A then resumes and reports success.

Required behavior:

    compare the token atomically with the lifecycle update
    reject worker A even if its storage side effect succeeded
    allow only worker B's current unexpired claim to publish ACTIVE

## F61 — Foreign Lineage Exists at the Assigned Path

The target WAL directory contains valid storage for another queue,
generation, or partition.

Required behavior:

    fail storage materialization before append or recovery
    never rewrite or adopt the foreign lineage
    never report successful provisioning

## F62 — Provisioning Completion Response Is Lost

PostgreSQL commits `ACTIVE`, but the response to the queue node is lost.

Required behavior:

    a retry with the same claim identity returns the ACTIVE descriptor
    do not increment metadata version more than once
    reject completion from any older claim token

## F63 — Two Processes Start with the Same Node ID

The second process registers while the first is still running.

Required behavior:

    increment the durable registration epoch
    reject heartbeats and provisioning completion from the first process
    never let the shared node ID make both process incarnations authoritative

## F64 — Node Registration Lease Expires

A node pauses or loses metadata connectivity beyond its registration lease.

Required behavior:

    reject heartbeat under the expired epoch
    reject new claims and completion from that registration
    require registration with a higher epoch before new work

## F65 — Unassigned Node Attempts Provisioning

A live node polls while a queue partition is placed on another live node.

Required behavior:

    return no claim to the unassigned node
    preserve the existing placement
    allow only the assigned node incarnation to claim

## F66 — Assigned Node Becomes Unavailable

A node lease expires while its partition placement remains durable.

Required behavior:

    do not automatically move the placement
    do not provision a second filesystem copy as new authority
    expose unavailability until a future safe reassignment protocol exists

## F67 — Registration Changes During Provisioning

A node claims work, then another process registers with the same node ID before
the first process completes.

Required behavior:

    retain the storage side effect as non-authoritative
    reject completion carrying the old registration epoch
    allow only a claim under current registration and placement authority

## F68 — Runtime Recovery Finishes After Registration Changes

A queue opens storage under registration epoch one while a replacement process
registers at epoch two.

Required behavior:

    reject READY publication from epoch one
    close the recovered epoch-one runtime
    never install the stale result in the active runtime map

## F69 — Placement Changes During Runtime Recovery

Recovery starts under placement epoch one and finishes after metadata advances
the placement epoch.

Required behavior:

    reject publication under placement epoch one
    close the stale recovered runtime
    require reconciliation under the current placement

## F70 — Node Registration Is Lost While Serving

Heartbeat failure or lease expiry removes the process incarnation's authority.

Required behavior:

    close every active runtime observed by reconciliation
    stop reporting those runtimes as locally READY
    prefer unavailability over serving under ambiguous authority

## F71 — Storage Recovery Fails

Snapshot, WAL, lineage, or filesystem validation prevents a partition from
recovering.

Required behavior:

    do not install a partial runtime
    close any resources acquired before failure
    publish FAILED only if authority is still current
    continue reconciling unrelated partitions

## F72 — Runtime Reconciliation Is Repeated

The desired placement is unchanged across polling cycles.

Required behavior:

    retain one open LocalMessageQueue instance
    do not reopen storage or publish duplicate readiness on every poll

## F73 — Placement Exists Before Provisioning Completes

Metadata has selected a node but the queue remains `PROVISIONING`.

Required behavior:

    omit the partition from node-specific runtime placement discovery
    do not race runtime recovery with storage materialization
    activate only after fenced provisioning publishes ACTIVE

## F74 — Request Arrives Before Runtime Is READY

A client addresses a node while the partition is recovering, failed, closed,
or assigned elsewhere.

Required behavior:

    return 503 Service Unavailable
    do not open storage from the request path
    do not execute against placement or stale READY observation alone

## F75 — Deactivation Races with an In-Progress Operation

A data-plane operation has entered a runtime when reconciliation observes lost
registration authority and attempts to close it.

Required behavior:

    establish one order between operation and closure
    if operation entered first, let its durable transition finish before close
    if closure entered first, reject the operation without mutation
    never use a queue object after close

## F76 — Registration Expires Between Reconciliation Ticks

The node still has a runtime in its active map, but its locally observed
registration lease has expired before the next scheduler tick.

Required behavior:

    revalidate registration at request admission
    close the runtime and return 503
    do not wait for periodic reconciliation to reject new traffic

## F77 — Publish Commits but HTTP Response Is Lost

The WAL durably accepts a message, then the connection fails before the client
observes `201 Created`. The client retries the same payload.

Current v0.22 behavior:

    the retry can publish a second message
    document the result as ambiguous
    preserve both successfully committed messages

Future requirement:

    introduce a producer request identity and durable deduplication contract

## F78 — Empty Queue Is Mistaken for an Unavailable Runtime

A receive finds no READY messages in an otherwise authoritative runtime.

Required behavior:

    return 204 No Content for an empty READY queue
    reserve 503 for absence of a serviceable runtime

## F79 — Stale Receipt Is Submitted Through HTTP

A client retries ACK or NACK after the receipt expired or a newer delivery
attempt was created.

Required behavior:

    return succeeded=false
    preserve the current delivery
    do not translate a stale receipt into a server failure

## F80 — Slow WAL on One Queue Blocks Another Queue

A durable operation on queue A stalls in filesystem force while a client
addresses queue B on the same node.

Required behavior:

    queue B acquires its own runtime permit and proceeds independently
    queue A retains its existing durability and operation ordering
    do not hold the manager lifecycle monitor across either queue operation

## F81 — Request Retains a Handle During Concurrent Removal

A request reads a READY handle immediately before reconciliation removes it
from the serving index and begins closure.

Required behavior:

    require handle-local permit acquisition after lookup
    reject the request if the handle became CLOSING first
    if permit acquisition won first, drain it before closing the queue

## F82 — Admitted Operation Throws

A queue callback fails after acquiring a runtime permit.

Required behavior:

    release the permit on the exceptional path
    preserve the original operation exception
    allow later deactivation to drain and close the runtime

## F83 — Concurrent Runtime Close Attempts

Request-time authority rejection and reconciliation concurrently attempt to
close the same runtime handle.

Required behavior:

    stop new admission once
    wait for the same active-operation count to reach zero
    invoke RuntimeQueue.close exactly once
    converge on CLOSED without exposing the stale handle again

## F84 — Concurrent Publishes Compete for the Last Capacity Slot

Two producers observe a partition with room for one retained message.

Required behavior:

    serialize the capacity decision with publication
    allow at most one PUBLISH WAL append
    reject the other publish without durable or volatile mutation

## F85 — Multibyte Payload Bypasses a Character Limit

A payload contains characters whose UTF-8 representation uses multiple bytes.

Required behavior:

    measure encoded UTF-8 bytes rather than Java characters
    reject a payload above the byte limit before WAL append
    return 413 rather than reporting a storage failure

## F86 — Restart Restores a Full Queue

Snapshot plus WAL recovery reconstructs retained messages at the configured
count or byte limit.

Required behavior:

    derive counters from authoritative recovered non-DONE states
    reject subsequent publish before WAL append
    allow receive and ACK so the queue can release capacity

## F87 — Limits Are Reduced Below Recovered Usage

An operator restarts a partition with limits below its existing retained
state.

Required behavior:

    do not discard messages or fail startup merely because configuration fell
    below recovered logical usage
    reject new publishes until durable ACKs restore capacity
    keep delivery and acknowledgement available for draining

## F88 — Runtime Is Not READY During Route Resolution

A queue exists but provisioning, recovery, or activation has not produced a
runtime status matching the current placement and node registration epochs.

Required behavior:

    return no route and 503
    do not forward to the placement based on placement alone

## F89 — Node Lease Expires After READY Publication

The last runtime status is READY but its node registration lease has expired.

Required behavior:

    exclude the stale runtime in the metadata route query
    return 503 until a current process publishes matching readiness

## F90 — Route Becomes Stale Before Forwarding

Metadata returns a route, then authority changes before the request reaches the
selected node.

Required behavior:

    let the queue node perform final local admission
    preserve its 503 rejection
    do not automatically replay the operation elsewhere

## F91 — Publish Commits but Gateway Loses the Node Response

The selected node durably appends PUBLISH but the connection fails before the
gateway receives its response.

Required behavior:

    return 502 as an ambiguous result
    make no second node call
    make no second metadata resolution

## F92 — Metadata Is Unavailable to the Gateway

The gateway cannot complete authoritative route resolution within its bounded
network timeout.

Required behavior:

    return 503 routing-metadata-unavailable
    do not guess a node or use an unvalidated fallback route

## F93 — Follower Receives an Out-of-Order Entry

The follower has sequence 10 and receives sequence 12.

Required behavior:

    reject the entry and report that sequence 11 is required
    do not append the WAL record
    retain a higher supplied epoch so the obsolete leader remains fenced

## F94 — Leader Retries an Ambiguous Follower Append

The follower already durably contains the same record at the requested
sequence, but the prior response was lost.

Required behavior:

    return ALREADY_PRESENT
    do not append a duplicate WAL record
    reject the retry if the stored record differs

## F95 — Stale Leader Contacts a Restarted Follower

The follower previously observed epoch 8, restarted, and receives an entry from
epoch 7.

Required behavior:

    recover epoch 8 from durable state
    reject epoch 7 before WAL mutation

## F96 — Follower Crashes Between Epoch Fence and WAL Append

The follower durably publishes epoch 9 and crashes before appending the first
record sent by that leader.

Required behavior:

    retain epoch 9 after restart
    continue rejecting older epochs
    allow the missing next sequence to be retried at epoch 9

## F97 — Replication Opens a Reclaimed WAL Suffix

The local WAL no longer contains history from its initial segment, so record
count cannot prove the next logical replication sequence.

Required behavior:

    fail replica-log initialization closed
    do not guess a logical sequence from retained physical records

## F98 — Follower Response Is Lost After a Batch Commits

The follower forces all records but the leader loses the HTTP response.

Required behavior:

    allow the leader to resend the unchanged batch
    report its records as already present
    do not duplicate WAL records

## F99 — Batch Fails After a Durable Prefix

Some entries are forced before follower storage fails and poisons the writer.

Required behavior:

    never claim batch atomicity
    recover the follower storage before another attempt
    let an unchanged retry skip the durable prefix and continue

## F100 — Replica Batch Exceeds Receiver Bounds

A sender supplies more than 256 records or more than 1 MiB of payload.

Required behavior:

    reject before opening or mutating follower storage
    do not partially accept the oversized request

## F101 — Catch-Up Source No Longer Retains Requested Sequence

The follower requests history older than the leader's reclaimed WAL suffix.

Required behavior:

    fail catch-up explicitly
    do not silently start from the oldest retained physical record
    require a later replication-aware snapshot transfer milestone

## F102 — Batch Write Fails After a Complete Entry Prefix

Several frames are complete when a later frame write or the group force fails.

Required behavior:

    do not report the requested durable-through index
    poison the WAL instance until restart
    recover only checksum-valid complete frames
    remove only a torn tail in the active segment
    allow an unchanged logical retry to skip the recovered identical prefix

## F103 — Replicated Batch Conflicts with Retained History

A leader supplies the same index with a different term or queue operation.

Required behavior:

    reject the complete request before writing any new frame
    preserve the existing durable suffix
    do not truncate automatically
    distinguish conflict from a storage failure

## F104 — Restart Finds a Logical Gap Across WAL Segments

Checksummed entries are individually readable, but their indexes are not
consecutive across a frame or segment boundary.

Required behavior:

    fail startup closed
    do not infer continuity from segment names or byte positions
    do not expose queue state from the inconsistent history

## F105 — Snapshot Boundary Term Disagrees with Retained WAL

The snapshot names `(lastIncludedIndex,lastIncludedTerm)`, while a retained
entry at that index carries another term.

Required behavior:

    reject the snapshot
    do not replay or reclaim WAL using that snapshot
    keep the partition unavailable until authoritative storage is repaired

## F106 — Reclaimed Prefix Has No Authoritative Logical Boundary

The oldest retained WAL entry begins above index one and no valid snapshot can
prove the preceding index and term.

Required behavior:

    fail recovery rather than guessing the compacted boundary
    do not reset the next index from retained record count
    require restoration of an authoritative snapshot

## F107 — Hard State Claims Commit Beyond Local Durability

`replica-hard-state.bin` contains a commit index greater than the last complete
locally durable log index.

Required behavior:

    reject hard-state load and partition readiness
    never apply entries based on the impossible commit boundary
    do not reduce the stored commit index automatically

## F108 — Hard-State Candidate or Directory Force Fails

Writing, forcing, promoting, or directory-forcing a hard-state candidate fails.

Required behavior:

    never update in-memory authority before successful durable publication
    preserve the prior authority when failure occurs before promotion
    treat post-promotion directory-force failure as indeterminate
    require reload before further authority-changing operations

## F109 — Insufficient Live Nodes for Initial Replica Factor

Queue creation requests more replicas than the current live node registry can
provide.

Required behavior:

    keep the replica group in PENDING_CAPACITY
    create no partial member set and no bootstrap leader
    retry placement when a live node later asks for work

## F110 — Two Planners Select Initial Membership Concurrently

Two metadata-service transactions attempt to place pending groups at the same
time.

Required behavior:

    lock one group with FOR UPDATE SKIP LOCKED
    never publish two configurations for the same group
    permit bounded load imbalance across different concurrent groups

## F111 — Replica Provisioning Claim Becomes Stale

A claim expires, the node re-registers, its fencing token is superseded, or the
membership version changes before completion.

Required behavior:

    reject the completion
    do not mark that member READY
    do not activate the group or queue from stale evidence

## F112 — Only Some Initial Replicas Are Ready

One or more members materialize local storage while another member has not yet
completed provisioning.

Required behavior:

    retain accepted per-member readiness
    keep group and queue in PROVISIONING
    activate only when every current member is READY under a live incarnation

## F113 — Stale Node Requests Replica Assignments

An expired or replaced node process requests its desired replica list.

Required behavior:

    fail closed with lost-node-authority semantics
    disclose no assignments as authority for that stale process
