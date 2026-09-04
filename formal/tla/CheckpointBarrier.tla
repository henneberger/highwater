---------------------------- MODULE CheckpointBarrier --------------------------
EXTENDS Integers, FiniteSets, TLC

(***************************************************************************
This model isolates the checkpoint acknowledgement contract. A barrier records
an exact partition-position vector. Nodes may continue appending before they
acknowledge. Each acknowledgement names the state cut represented by its handle.

BindAckToBarrier is a mutation switch. TRUE models the documented protocol in
which an acknowledgement is tied to the barrier vector. FALSE models an
epoch-only acknowledgement: ownership can still be valid while the state handle
was created at a different cut.
***************************************************************************)

CONSTANTS Nodes, Partitions, None, MaxPosition, BindAckToBarrier

ASSUME /\ Cardinality(Nodes) >= 2
       /\ Cardinality(Partitions) >= 2
       /\ MaxPosition >= 2

VARIABLES positions, barrier, acknowledgements, published

vars == <<positions, barrier, acknowledgements, published>>

Init ==
    /\ positions = [p \in Partitions |-> 0]
    /\ barrier = None
    /\ acknowledgements = [n \in Nodes |-> None]
    /\ published = FALSE

AppendRecord(partition) ==
    /\ partition \in Partitions
    /\ positions[partition] < MaxPosition
    /\ positions' = [positions EXCEPT ![partition] = @ + 1]
    /\ UNCHANGED <<barrier, acknowledgements, published>>

StartBarrier ==
    /\ barrier = None
    /\ barrier' = positions
    /\ UNCHANGED <<positions, acknowledgements, published>>

Ack(node, handleCut) ==
    /\ barrier # None
    /\ ~published
    /\ node \in Nodes
    /\ acknowledgements[node] = None
    /\ handleCut \in [Partitions -> 0..MaxPosition]
    /\ \A p \in Partitions: handleCut[p] <= positions[p]
    /\ (~BindAckToBarrier \/ handleCut = barrier)
    /\ acknowledgements' = [acknowledgements EXCEPT ![node] = handleCut]
    /\ UNCHANGED <<positions, barrier, published>>

Publish ==
    /\ barrier # None
    /\ ~published
    /\ \A n \in Nodes: acknowledgements[n] # None
    /\ published' = TRUE
    /\ UNCHANGED <<positions, barrier, acknowledgements>>

Next ==
    \/ \E p \in Partitions: AppendRecord(p)
    \/ StartBarrier
    \/ \E n \in Nodes, cut \in [Partitions -> 0..MaxPosition]: Ack(n, cut)
    \/ Publish

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ positions \in [Partitions -> 0..MaxPosition]
    /\ barrier \in {None} \cup [Partitions -> 0..MaxPosition]
    /\ acknowledgements \in
          [Nodes -> {None} \cup [Partitions -> 0..MaxPosition]]
    /\ published \in BOOLEAN

PublishedHandlesMatchBarrier ==
    published => \A n \in Nodes: acknowledgements[n] = barrier

=============================================================================

