# providers/braket/interceptor.py
#
# Braket half of the interceptor. Concatenated by the build into the single
# all-providers interceptor file mounted into user application containers via
# PYTHONSTARTUP. Stamps the shared fluence-pod-uid tag onto every Braket task
# submission so the sidecar's BraketProvider.find_my_task can discover it.
#
# Fail-soft: if amazon-braket-sdk is not importable in this container, this
# block no-ops. The one mounted file carries every provider's block; only the
# blocks whose SDKs are present take effect.

def _fluence_install_braket():
    import os
    try:
        from braket.aws import AwsDevice
    except ImportError:
        return  # braket SDK not present in this container — skip

    _original_run = AwsDevice.run

    def _patched_run(self, task_specification, *args, **kwargs):
        pod_uid = os.environ.get("FLUENCE_POD_UID", "")
        if pod_uid:
            tags = kwargs.get("tags", {})
            tags["fluence-pod-uid"] = pod_uid
            kwargs["tags"] = tags
        return _original_run(self, task_specification, *args, **kwargs)

    AwsDevice.run = _patched_run


_fluence_install_braket()
