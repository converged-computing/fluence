# fluence_braket_intercept.py
#
# Injected by the Fluence webhook into every pod requesting a QPU resource.
# Patches AwsDevice.run() to automatically tag every quantum task submission
# with the pod UID, enabling the fluence-sidecar to find the task without
# any user application changes.
#
# Installed as a Python sitecustomize hook so it runs before any user code.
# The user application requires no changes.

import os


def _install_interceptor():
    try:
        from braket.aws import AwsDevice

        _original_run = AwsDevice.run

        def _patched_run(self, task_specification, *args, **kwargs):
            pod_uid = os.environ.get("FLUENCE_POD_UID", "")
            if pod_uid:
                tags = kwargs.get("tags", {})
                tags["fluence-pod-uid"] = pod_uid
                kwargs["tags"] = tags
            return _original_run(self, task_specification, *args, **kwargs)

        AwsDevice.run = _patched_run

    except ImportError:
        # amazon-braket-sdk not installed in this container — skip
        pass


_install_interceptor()
