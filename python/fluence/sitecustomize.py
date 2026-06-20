# fluence sitecustomize — staged onto the user container's PYTHONPATH by the
# Fluence init container. Python imports `sitecustomize` automatically on every
# interpreter start (interactive OR script), so this runs the interceptor with
# zero user code changes and without relying on PYTHONSTARTUP (which only fires
# for interactive sessions).
#
# Guarded so a fluence-side error can never break the user's application.
try:
    import fluence.interceptor  # noqa: F401  (import side-effect installs hooks)
except Exception as _e:  # pragma: no cover
    import sys
    print(f"[fluence] interceptor skipped: {_e}", file=sys.stderr, flush=True)
