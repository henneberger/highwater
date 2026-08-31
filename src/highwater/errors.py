class HighwaterError(Exception):
    pass


class StreamBackpressure(HighwaterError):
    pass


class WorkflowFailed(HighwaterError):
    pass


class WorkflowCancelled(HighwaterError):
    pass


class NonDeterminismError(HighwaterError):
    pass


class QueryNotFound(HighwaterError):
    pass


class UpdateNotFound(HighwaterError):
    pass


class ActivityError(HighwaterError):
    pass


class ChildWorkflowError(HighwaterError):
    pass


class NonRetryableError(Exception):
    pass
