"""
fluence.stage — init-container entrypoint for Model C delivery.

The Fluence webhook injects an init container (the sidecar image, which has
`fluence` installed) that runs `python -m fluence.stage <dest>`. This copies the
installed `fluence` package plus a `sitecustomize.py` into <dest>, a shared
emptyDir volume. The webhook mounts that volume into the user's application
container and prepends <dest> to PYTHONPATH. Python then auto-imports
sitecustomize on startup, which imports fluence.interceptor — tagging the user's
quantum tasks with zero user code changes and no vendor SDK requirement on our
side (the interceptor patches whatever SDK the user already has).

Replaces the old build-interceptor.sh: assembly is real package staging, not
text concatenation.

Usage:
  python -m fluence.stage /opt/fluence-staged
"""

from __future__ import annotations

import os
import shutil
import sys

import fluence


def stage(dest: str) -> None:
    os.makedirs(dest, exist_ok=True)

    # Copy the installed `fluence` package into <dest>/fluence so it is importable
    # when <dest> is on PYTHONPATH. We copy only the pure-Python package — no
    # vendor SDKs — so we never perturb the user container's own dependencies.
    pkg_src = os.path.dirname(os.path.abspath(fluence.__file__))
    pkg_dst = os.path.join(dest, "fluence")
    if os.path.exists(pkg_dst):
        shutil.rmtree(pkg_dst)
    shutil.copytree(
        pkg_src, pkg_dst,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "tests"),
    )

    # Place sitecustomize.py at the TOP of <dest> (not inside the package) so
    # Python's site machinery imports it automatically on interpreter startup.
    src_sitecustomize = os.path.join(pkg_src, "sitecustomize.py")
    shutil.copyfile(src_sitecustomize, os.path.join(dest, "sitecustomize.py"))

    print(f"[fluence] staged package + sitecustomize into {dest}", flush=True)


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    dest = argv[0] if argv else os.environ.get("FLUENCE_STAGE_DIR", "/opt/fluence-staged")
    stage(dest)


if __name__ == "__main__":
    main()
