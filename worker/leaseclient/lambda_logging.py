"""Making a function's INFO logs reach CloudWatch.

The Lambda Python runtime adds its own handler to the root logger before any of this
code is imported. With the plain-text log format it leaves the root level at Python's
default, WARNING, so every `logger.info` in a handler is discarded before it reaches
that handler. `logging.basicConfig` cannot fix it: it does nothing once the root logger
has a handler, which under Lambda it always does. Observed on a deployed reconciler,
whose log group held invocation reports and metrics and not one result line.

So the level is set on the root logger directly. The runtime's handler is kept rather
than replaced (`force=True` would replace it), because it is what stamps each line
with the request id.
"""

from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    """Set the root level from `LOG_LEVEL`, whether or not a handler already exists.

    Called at the top of each function's handler. Safe to call on every warm
    invocation: neither step adds a second handler or changes anything the first call
    did not.
    """
    level = os.environ.get("LOG_LEVEL", "INFO")
    # Outside Lambda (a local run) there is no handler yet, and this adds one.
    logging.basicConfig(level=level)
    # Under Lambda the call above did nothing, and this is what takes effect.
    logging.getLogger().setLevel(level)
