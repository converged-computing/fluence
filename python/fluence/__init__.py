"""
fluence — quantum-classical scheduling coordination for the Fluence Kubernetes
scheduler.

This package is built into the Fluence sidecar image and staged into user
application containers at admission time (via an init container + shared volume
on PYTHONPATH), so the interceptor runs with zero user code changes.

Submodules:
  fluence.providers   provider interface + registry (per-vendor plug-ins)
  fluence.interceptor  runs every registered provider's submit-time tag hook
  fluence.sidecar      the sidecar coordination main loop
  fluence.ungate       generic worker ungating (Kubernetes patch logic)
"""

__version__ = "0.1.0"
