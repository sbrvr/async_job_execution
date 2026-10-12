"""Shared fakes: no Databricks SDK or Spark needed (python -m pytest tests)."""
import os
import sys
import types
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
# stub the SDK modules the scripts import at module level
for _m in ("databricks", "databricks.sdk", "databricks.sdk.service", "databricks.sdk.service.jobs",
           "databricks.sdk.errors"):
    sys.modules.setdefault(_m, types.ModuleType(_m))
sys.modules["databricks.sdk.service.jobs"].QueueSettings = lambda enabled: None
sys.modules["databricks.sdk.errors"].NotFound = type("NotFound", (Exception,), {})
sys.modules["databricks.sdk.errors"].PermissionDenied = type("PermissionDenied", (Exception,), {})


def enum(value):
    """SDK enums expose .value."""
    return NS(value=value) if value is not None else None


class Row(dict):
    """Spark Row stand-in: item access, asDict()."""

    def asDict(self, recursive=False):
        return dict(self)
