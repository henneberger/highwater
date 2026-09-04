--------------------------- MODULE JournalOwnership ---------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************
This model captures the safety-critical part of Highwater's partition journal:

* an owner generation is installed by a committed journal record;
* process activations carry the owner generation and an activation number;
* a completion can be prepared against an observed journal head;
* another node can take over between preparation and the head CAS;
* the same event can be granted again after an ambiguous response or takeover.

The Enforce* constants are mutation switches.  The checked configuration enables
all protections.  Negative configurations deliberately remove one protection so
that TLC must find a counterexample, demonstrating that the invariants are not
vacuous.
***************************************************************************)

CONSTANTS Nodes, Events, Tokens, None, MaxEpoch, MaxLogLength, MaxActivation,
          EnforceCAS, EnforceEpoch, EnforceDedup

ASSUME /\ Cardinality(Nodes) >= 2
       /\ Cardinality(Events) >= 1
       /\ Cardinality(Tokens) >= 2
       /\ MaxEpoch >= 2
       /\ MaxLogLength >= 4
       /\ MaxActivation >= 2

VARIABLES owner, epoch, headVersion, log, leases, prepared, nextActivation

vars == <<owner, epoch, headVersion, log, leases, prepared, nextActivation>>

OwnerRecord(n, ep) ==
    [kind |-> "owner", node |-> n, epoch |-> ep,
     event |-> None, activation |-> 0, token |-> None]

CompletionRecord(candidate, token) ==
    [kind |-> "complete", node |-> candidate.node,
     epoch |-> candidate.epoch, event |-> candidate.event,
     activation |-> candidate.activation, token |-> token]

CompletedEvents ==
    {log[i].event : i \in {j \in 1..Len(log) : log[j].kind = "complete"}}

Init ==
    \E initialOwner \in Nodes:
        /\ owner = initialOwner
        /\ epoch = 1
        /\ headVersion = 1
        /\ log = <<OwnerRecord(initialOwner, 1)>>
        /\ leases = [t \in Tokens |-> None]
        /\ prepared = [t \in Tokens |-> None]
        /\ nextActivation = 0

Grant(n, event, token) ==
    /\ n = owner
    /\ event \in Events
    /\ token \in Tokens
    /\ leases[token] = None
    /\ prepared[token] = None
    /\ nextActivation < MaxActivation
    /\ nextActivation' = nextActivation + 1
    /\ leases' = [leases EXCEPT
          ![token] = [node |-> n, epoch |-> epoch, event |-> event,
                      activation |-> nextActivation']]
    /\ UNCHANGED <<owner, epoch, headVersion, log, prepared>>

Prepare(token) ==
    /\ token \in Tokens
    /\ leases[token] # None
    /\ prepared[token] = None
    /\ leases[token].node = owner
    /\ leases[token].epoch = epoch
    /\ prepared' = [prepared EXCEPT
          ![token] = [node |-> leases[token].node,
                      epoch |-> leases[token].epoch,
                      event |-> leases[token].event,
                      activation |-> leases[token].activation,
                      expectedHead |-> headVersion]]
    /\ UNCHANGED <<owner, epoch, headVersion, log, leases, nextActivation>>

Takeover(n) ==
    /\ n \in Nodes \ {owner}
    /\ epoch < MaxEpoch
    /\ Len(log) < MaxLogLength
    /\ owner' = n
    /\ epoch' = epoch + 1
    /\ headVersion' = headVersion + 1
    /\ log' = Append(log, OwnerRecord(n, epoch'))
    /\ UNCHANGED <<leases, prepared, nextActivation>>

Commit(token) ==
    LET candidate == prepared[token] IN
    /\ token \in Tokens
    /\ candidate # None
    /\ Len(log) < MaxLogLength
    /\ (~EnforceCAS \/ candidate.expectedHead = headVersion)
    /\ (~EnforceEpoch \/
          /\ candidate.epoch = epoch
          /\ candidate.node = owner)
    /\ (~EnforceDedup \/ candidate.event \notin CompletedEvents)
    /\ headVersion' = headVersion + 1
    /\ log' = Append(log, CompletionRecord(candidate, token))
    /\ leases' = [leases EXCEPT ![token] = None]
    /\ prepared' = [prepared EXCEPT ![token] = None]
    /\ UNCHANGED <<owner, epoch, nextActivation>>

Abandon(token) ==
    /\ token \in Tokens
    /\ leases[token] # None \/ prepared[token] # None
    /\ leases' = [leases EXCEPT ![token] = None]
    /\ prepared' = [prepared EXCEPT ![token] = None]
    /\ UNCHANGED <<owner, epoch, headVersion, log, nextActivation>>

Next ==
    \/ \E n \in Nodes, event \in Events, token \in Tokens:
          Grant(n, event, token)
    \/ \E token \in Tokens: Prepare(token)
    \/ \E n \in Nodes: Takeover(n)
    \/ \E token \in Tokens: Commit(token)
    \/ \E token \in Tokens: Abandon(token)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ owner \in Nodes
    /\ epoch \in 1..MaxEpoch
    /\ headVersion \in 1..MaxLogLength
    /\ log \in Seq([
          kind : {"owner", "complete"}, node : Nodes,
          epoch : 1..MaxEpoch, event : Events \cup {None},
          activation : Nat, token : Tokens \cup {None}])
    /\ leases \in [Tokens ->
          {None} \cup [node : Nodes, epoch : 1..MaxEpoch,
                        event : Events, activation : Nat]]
    /\ prepared \in [Tokens ->
          {None} \cup [node : Nodes, epoch : 1..MaxEpoch,
                        event : Events, activation : Nat,
                        expectedHead : 1..MaxLogLength]]
    /\ nextActivation \in 0..MaxActivation

HeadMatchesLog == headVersion = Len(log)

(***************************************************************************
A completion is stale if any owner-generation record preceding it has a
strictly greater epoch.  This directly catches an old prepared completion
committing after takeover.
***************************************************************************)
NoStaleCompletions ==
    \A i, j \in 1..Len(log):
        (j <= i /\ log[i].kind = "complete" /\ log[j].kind = "owner")
        => log[i].epoch >= log[j].epoch

NoDuplicateCompletions ==
    \A i, j \in 1..Len(log):
        (log[i].kind = "complete" /\ log[j].kind = "complete" /\
         log[i].event = log[j].event)
        => i = j

OwnerEpochMonotonic ==
    \A i, j \in 1..Len(log):
        (i < j /\ log[i].kind = "owner" /\ log[j].kind = "owner")
        => log[i].epoch < log[j].epoch

=============================================================================
