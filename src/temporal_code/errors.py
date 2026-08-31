class TemporalCodeError(Exception):
    pass


class StreamBackpressure(TemporalCodeError):
    pass


class WorkflowFailed(TemporalCodeError):
    pass


class WorkflowCancelled(TemporalCodeError):
    pass


class NonDeterminismError(TemporalCodeError):
    pass


class QueryNotFound(TemporalCodeError):
    pass


class UpdateNotFound(TemporalCodeError):
    pass


class ActivityError(TemporalCodeError):
    pass


class ChildWorkflowError(TemporalCodeError):
    pass


class NonRetryableError(Exception):
    pass
