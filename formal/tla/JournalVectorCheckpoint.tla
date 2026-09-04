------------------------ MODULE JournalVectorCheckpoint ------------------------
EXTENDS Integers, FiniteSets, TLC

(***************************************************************************
This model captures the implemented full-state checkpoint protocol. Each
partition has an authoritative journal position. A prepared RocksDB snapshot
contains state exactly through a vector of positions. Publication atomically
selects that snapshot. Only positions covered by the published vector may be
truncated; recovery adds the remaining journal tail.

The Enforce* constants are mutation switches used by negative configurations.
***************************************************************************)

CONSTANTS Partitions, None, MaxPosition,
          EnforcePublicationBeforeTruncation, EnforceMonotonicPublication

ASSUME /\ Cardinality(Partitions) >= 2
       /\ MaxPosition >= 2

VARIABLES heads, prepared, published, truncated, recovered

vars == <<heads, prepared, published, truncated, recovered>>

Checkpoint(cut) == [cut |-> cut, state |-> cut]

Init ==
    /\ heads = [p \in Partitions |-> 0]
    /\ prepared = None
    /\ published = None
    /\ truncated = [p \in Partitions |-> 0]
    /\ recovered = None

AppendRecord(partition) ==
    /\ partition \in Partitions
    /\ heads[partition] < MaxPosition
    /\ heads' = [heads EXCEPT ![partition] = @ + 1]
    /\ recovered' = IF recovered = None
          THEN None
          ELSE [recovered EXCEPT ![partition] = @ + 1]
    /\ UNCHANGED <<prepared, published, truncated>>

Prepare(cut) ==
    /\ cut \in [Partitions -> 0..MaxPosition]
    /\ \A p \in Partitions: cut[p] <= heads[p]
    /\ prepared' = Checkpoint(cut)
    /\ UNCHANGED <<heads, published, truncated, recovered>>

Publish ==
    /\ prepared # None
    /\ IF EnforceMonotonicPublication
          THEN IF published = None
              THEN TRUE
              ELSE \A p \in Partitions: prepared.cut[p] >= published.cut[p]
          ELSE TRUE
    /\ published' = prepared
    /\ UNCHANGED <<heads, prepared, truncated, recovered>>

Truncate(partition, through) ==
    /\ partition \in Partitions
    /\ through \in truncated[partition]..heads[partition]
    /\ IF EnforcePublicationBeforeTruncation
          THEN IF published = None
              THEN FALSE
              ELSE through <= published.cut[partition]
          ELSE TRUE
    /\ truncated' = [truncated EXCEPT ![partition] = through]
    /\ UNCHANGED <<heads, prepared, published, recovered>>

Recover ==
    /\ published # None
    /\ recovered' = [p \in Partitions |->
          published.state[p] + (heads[p] - published.cut[p])]
    /\ UNCHANGED <<heads, prepared, published, truncated>>

Next ==
    \/ \E p \in Partitions: AppendRecord(p)
    \/ \E cut \in [Partitions -> 0..MaxPosition]: Prepare(cut)
    \/ Publish
    \/ \E p \in Partitions, through \in 0..MaxPosition: Truncate(p, through)
    \/ Recover

Spec == Init /\ [][Next]_vars

CheckpointType == [cut : [Partitions -> 0..MaxPosition],
                   state : [Partitions -> 0..MaxPosition]]

TypeOK ==
    /\ heads \in [Partitions -> 0..MaxPosition]
    /\ prepared \in {None} \cup CheckpointType
    /\ published \in {None} \cup CheckpointType
    /\ truncated \in [Partitions -> 0..MaxPosition]
    /\ recovered \in {None} \cup [Partitions -> 0..MaxPosition]

PreparedStateMatchesCut ==
    prepared # None => prepared.state = prepared.cut

PublishedStateMatchesCut ==
    published # None => published.state = published.cut

TruncationCovered ==
    \A p \in Partitions:
        truncated[p] <= IF published = None THEN 0 ELSE published.cut[p]

RecoveryMatchesAuthoritativeHeads ==
    recovered # None => recovered = heads

=============================================================================
