# Data Model: V2 WAL Segments

## V2 Segment Header

- **Store kind**: key/value, key/set, or key/sorted-map.
- **Timestamp granularity**: nonzero nanoseconds per bucket.
- **Base bucket**: last accepted timestamp at segment creation.
- **Segment identifier**: monotonically increasing unsigned 64-bit number.
- **Segment base**: cumulative byte position of this segment in the logical chain.
- **Integrity**: fixed magic/version/length fields, reserved zero bytes, and header checksum.

Validation requires a supported store kind, nonzero granularity, zero reserved fields, valid checksum, and exact version/header length.

## V2 Record

- **Action**: one of the existing six durable mutation actions.
- **Payload length/complement**: unsigned 64-bit length and bitwise complement.
- **Physical start**: unsigned 64-bit global record position.
- **Mutation start**: unsigned 64-bit global start of the logical mutation.
- **Group index/count**: member position and nonzero total count.
- **Timestamp bucket**: accepted mutation timestamp shared by every group member.
- **Payload**: family/action-specific encoded bytes.
- **Footer start**: duplicate physical start.
- **Integrity**: checksum over header, payload, and footer.

Validation uses checked arithmetic and requires all group members to be consecutive, share mutation start/count/timestamp, and end exactly at a record boundary.

## Segment Chain

- Zero or more sealed segment paths ordered by identifier.
- Exactly one canonical active path during normal operation.
- Optional staging and recovery artifacts only during an interrupted publication.
- For each segment after the first: `id = previous.id + 1` and `base = previous.base + previous.file_length`.

### State transitions

```text
Active(id=N)
  -> Next staging validated
  -> Active renamed Sealed(id=N)
  -> Next staging published Active(id=N+1)
  -> Directory authority synchronized when physical durability is selected
```

On startup, an incomplete transition is completed only when the chain and candidate header prove exactly one next authority.

## Runtime Rotation Configuration

- **Target bytes**: validated nonzero unsigned 64-bit value; default 1 GiB.
- **Force-next flag**: set when explicitly requested granularity differs from the active header.
- **Failure state**: fail-closed after a publication error that may have advanced namespace authority.

The target is not persisted. Rotation is checked against the complete encoded logical mutation before any member is written.

## Migration Source Capture

- Canonical active path.
- Ordered sealed paths discovered at capture time.
- Exact bytes for each captured artifact.
- Combined replay bytes.

Migration output is accepted only after its exact V2 bytes replay to the captured logical state and the captured source artifacts reread identically.

## Timestamp State

- **Persisted granularity**: taken from the active segment unless an explicit opening override exists.
- **Last bucket**: maximum of header base and all accepted record/group timestamps.
- **Next bucket**: current time rounded to granularity, clamped to at least last bucket.

Rotation writes the current last bucket into the next header. Migration writes the source last bucket into the compacted header and records.
