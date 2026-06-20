"""
fluence.providers — provider registry.

Importing this package imports every provider submodule, each of which calls
fluence.providers.base.register() at import time. This is the single extension
point: to add a vendor, drop a new module here that defines a Provider subclass
and calls register() — nothing else in the codebase needs to change.

Provider discovery is by explicit submodule import below (simple and debuggable).
Importing a provider module never fails on a missing vendor SDK: the SDK is only
imported lazily inside the methods that need it.
"""

from fluence.providers.base import (  # noqa: F401
    Provider,
    Task,
    TAG_KEY,
    log,
    register,
    all_providers,
    resolve,
    resolve_from_env,
)

# Import provider modules so they self-register. Add new providers here.
from fluence.providers import braket  # noqa: F401,E402
# from fluence.providers import ibm   # noqa: F401  (when implemented)
