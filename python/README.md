# fluence (Python)

Quantum-classical scheduling coordination library for the Fluence Kubernetes
scheduler. Import name `fluence`; distributed on PyPI as `fluence-hpc`.

This package is **built into the Fluence sidecar image** and **staged into user
application containers at admission time** — users never install it.

## What it does

A hybrid quantum-classical workflow submits work to two queues: the Kubernetes
scheduler (classical) and a QPU vendor API (quantum). Classical worker pods would
idle while the QPU queue drains. Fluence gates the workers until the quantum task
is about to run, then releases them. This library is the runtime half:

- **interceptor** (`fluence.interceptor`) — runs inside the user container,
  monkey-patches the vendor SDK submit call to tag each task with the pod UID.
- **sidecar** (`fluence.sidecar`) — runs in a sidecar container, discovers the
  tagged task, polls queue position, and ungates the classical workers when the
  task is ready (or, in observe-only mode, just records the queue position).
- **providers** (`fluence.providers`) — per-vendor plug-ins implementing both
  halves. Providers self-register on import.

## Delivery (Model C)

The interceptor must run in the user's container, which does **not** have this
package installed. Rather than require a user install or concatenate a text
snippet, the Fluence webhook:

1. injects an **init container** (the sidecar image) running
   `python -m fluence.stage <dir>`, which copies the pure-Python `fluence`
   package plus a `sitecustomize.py` into a shared `emptyDir`;
2. mounts that volume into the user container and prepends `<dir>` to
   `PYTHONPATH`.

Python imports `sitecustomize` automatically on every interpreter start
(`python app.py` included — unlike `PYTHONSTARTUP`, which only fires for
interactive sessions), so `import fluence.interceptor` runs before user code.
The interceptor patches whichever vendor SDK is present and fail-soft skips the
rest. No user code changes, no vendor SDKs added to the user image.

## Adding a provider

Add one module under `fluence/providers/` that subclasses `Provider`, implements
`install_interceptor` (tag hook), `matches`, `find_my_task`, `is_ready_to_ungate`,
`queue_position` (optional), and `job_id`, and calls `register(PROVIDER)`. Import
it from `fluence/providers/__init__.py`. Nothing else changes.

## Tests

    python3 python/tests/test_fluence.py

## Building and releasing

The package is distributed on PyPI as `fluence-hpc` (the import name `fluence` is
already taken on PyPI). It is also baked into the sidecar image, so a release
moves the package version and the image tag together.

### Build the distributions

From `python/`:

    pip install --upgrade build twine
    python -m build

This produces `dist/fluence_hpc-<version>-py3-none-any.whl` and
`dist/fluence_hpc-<version>.tar.gz`. Upload both.

### Test on TestPyPI first

    twine upload --repository testpypi dist/*
    pip install --index-url https://test.pypi.org/simple/ fluence-hpc
    python -c "import fluence; print(fluence.__version__)"

### Release to PyPI

    twine upload dist/*

After this, `pip install fluence-hpc` works anywhere and imports as `fluence`.

### Versioning

Bump `version` in `pyproject.toml` and `__version__` in `fluence/__init__.py`
together (PyPI refuses to overwrite an existing version). Because the package is
version-locked into the sidecar image, tag the release so the image and the
package share a version — e.g. a `v0.1.1` git tag triggers both the
`sidecar-build-deploy` workflow (image) and a PyPI publish.

### Automated release (recommended)

Prefer GitHub Actions with PyPI Trusted Publishing (OIDC) over manual token
uploads: register the repo + workflow once on PyPI, then a release workflow
triggered by a version tag builds with `python -m build` and uploads with
`pypa/gh-action-pypi-publish` — no stored secret. The Docker image is built by
`.github/workflows/sidecar-build-deploy.yaml` on the same tag, keeping the
package version and image tag in lockstep.
