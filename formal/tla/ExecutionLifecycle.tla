------------------------ MODULE ExecutionLifecycle ------------------------
EXTENDS Naturals, TLC

CONSTANTS MaxAttempts, MaxDeliveries, CountLostAttempts, EmitsOutput,
          RetainPendingOutput

VARIABLES status, lease, attempts, pendingOutput, output, deliveries, acked

vars == <<status, lease, attempts, pendingOutput, output, deliveries, acked>>

Init ==
    /\ status = "PENDING"
    /\ lease = "NONE"
    /\ attempts = 0
    /\ pendingOutput = FALSE
    /\ output = FALSE
    /\ deliveries = 0
    /\ acked = FALSE

Grant ==
    /\ status = "PENDING"
    /\ lease = "NONE"
    /\ lease' = "ACTIVE"
    /\ UNCHANGED <<status, attempts, pendingOutput, output, deliveries, acked>>

Complete ==
    /\ status = "PENDING"
    /\ lease = "ACTIVE"
    /\ status' = "COMMITTED"
    /\ lease' = "NONE"
    /\ pendingOutput' = EmitsOutput
    /\ UNCHANGED <<attempts, output, deliveries, acked>>

HandlerFailure ==
    /\ status = "PENDING"
    /\ lease = "ACTIVE"
    /\ attempts < MaxAttempts
    /\ attempts' = attempts + 1
    /\ status' = IF attempts' = MaxAttempts THEN "FAILED" ELSE "PENDING"
    /\ lease' = "NONE"
    /\ UNCHANGED <<pendingOutput, output, deliveries, acked>>

LoseLease ==
    /\ status = "PENDING"
    /\ lease = "ACTIVE"
    /\ IF CountLostAttempts
          THEN /\ attempts < MaxAttempts
               /\ attempts' = attempts + 1
               /\ status' = IF attempts' = MaxAttempts THEN "FAILED" ELSE "PENDING"
          ELSE /\ attempts' = attempts
               /\ status' = "PENDING"
    /\ lease' = "NONE"
    /\ UNCHANGED <<pendingOutput, output, deliveries, acked>>

PromoteOutput ==
    /\ status = "COMMITTED"
    /\ pendingOutput
    /\ ~output
    /\ output' = TRUE
    /\ pendingOutput' = IF RetainPendingOutput THEN pendingOutput ELSE FALSE
    /\ UNCHANGED <<status, lease, attempts, deliveries, acked>>

DeliverOutput ==
    /\ status = "COMMITTED"
    /\ output
    /\ ~acked
    /\ deliveries < MaxDeliveries
    /\ deliveries' = deliveries + 1
    /\ UNCHANGED <<status, lease, attempts, pendingOutput, output, acked>>

AcknowledgeOutput ==
    /\ status = "COMMITTED"
    /\ output
    /\ deliveries > 0
    /\ ~acked
    /\ acked' = TRUE
    /\ UNCHANGED <<status, lease, attempts, pendingOutput, output, deliveries>>

Next ==
    \/ Grant
    \/ Complete
    \/ HandlerFailure
    \/ LoseLease
    \/ PromoteOutput
    \/ DeliverOutput
    \/ AcknowledgeOutput

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Grant)
    /\ WF_vars(Complete)
    /\ WF_vars(HandlerFailure)
    /\ WF_vars(LoseLease)
    /\ WF_vars(PromoteOutput)
    /\ WF_vars(DeliverOutput)
    /\ WF_vars(AcknowledgeOutput)

TypeOK ==
    /\ status \in {"PENDING", "COMMITTED", "FAILED"}
    /\ lease \in {"NONE", "ACTIVE"}
    /\ attempts \in 0..MaxAttempts
    /\ pendingOutput \in BOOLEAN
    /\ output \in BOOLEAN
    /\ deliveries \in 0..MaxDeliveries
    /\ acked \in BOOLEAN

NoLeaseAfterTerminal == status = "PENDING" \/ lease = "NONE"
OutputRequiresCommit == ~output \/ status = "COMMITTED"
PendingOutputRequiresCommit == ~pendingOutput \/ status = "COMMITTED"
PromotedOutputRetainsSource == ~output \/ pendingOutput
CommittedHasExpectedPendingOutput == status # "COMMITTED" \/ pendingOutput = EmitsOutput
AckRequiresCommittedOutput == ~acked \/ (status = "COMMITTED" /\ output /\ deliveries > 0)
FailedConsumesBudget == status # "FAILED" \/ attempts = MaxAttempts

EventuallyTerminal == status = "PENDING" ~> status # "PENDING"
CommittedOutputEventuallyAcked == (status = "COMMITTED" /\ pendingOutput) ~> acked

=============================================================================
