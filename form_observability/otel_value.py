"""Common string values for use as OpenTelemetry attributes.

Plain strings are used because:
*   Enums cannot be subclassed to add more values. Also iterating over all available
    attribute values or testing whether unknown strings are enum members are not
    likely use cases.
*   The aenum (Advanced Enum) library allows adding values to an existing enum, but
    then import order could affect the available values in an enum.
"""


class SpanName:
    """Names of spans which may be referenced in multiple places.

    These might be reference in application code, or in external dashboards/alerts.
    """

    #: The top-level span; a default name for the first span in the trace.
    ROOT = "trace-root"


class EventAttrKey:
    """Event attribute keys which may be referenced in external queries."""

    #: Types of events.
    TYPE = "type"
    #: The name field for a duration event. This cannot simply be "name" which is
    #: reserved for the actual event name.
    DURATION_NAME = "duration_name"
    #: The elapsed time value (in seconds) represented by a duration event.
    DURATION_SECONDS = "duration_s"


class EventAttrValue:
    """Event attribute values which may be referenced / filtered by in external queries."""

    #: An event type capturing the time taken by some portion of the pipeline.
    DURATION = "duration"
    #: An event type for log messages.
    LOG_MESSAGE = "log_message"
