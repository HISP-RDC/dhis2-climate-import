class PipelineError(RuntimeError):
    """Raised for expected pipeline failures with actionable messages."""


class TransientPipelineError(PipelineError):
    """Raised when retrying later can reasonably succeed without a configuration change."""
