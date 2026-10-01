"""Network-capable, one-request Provider sender isolated by systemd."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from orchestrator.runtime.provider_sender_process import ProviderSenderResponse

from .launcher import (
    InvalidSandboxRequest,
    IsolationUnavailable,
    SandboxLimits,
    SandboxResult,
    SandboxSession,
    SandboxTerminationReceipt,
    _systemd_cgroup_parent,
    _systemd_client_environment,
    _systemd_path_supported,
)


_MAX_OUTPUT_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ProviderSenderResult:
    """Bounded child response plus host-verified unit stop receipt."""

    unit_name: str
    response: ProviderSenderResponse | None
    termination_receipt: SandboxTerminationReceipt
    returncode: int
    elapsed_seconds: float
    cancelled: bool
    timed_out: bool
    output_limited: bool
    input_written: bool


class SystemdProviderSenderSession:
    """Cancellable sender whose result requires a host termination receipt."""

    def __init__(
        self, sandbox_session: SandboxSession, *, max_response_bytes: int
    ) -> None:
        if (
            isinstance(max_response_bytes, bool)
            or not isinstance(max_response_bytes, int)
            or not 1 <= max_response_bytes <= 8_000_000
        ):
            raise InvalidSandboxRequest("Provider sender response bound is invalid")
        self.unit_name = sandbox_session.unit_name
        self._sandbox_session = sandbox_session
        self._max_response_bytes = max_response_bytes
        self._result: ProviderSenderResult | None = None

    def cancel(self) -> bool:
        return self._sandbox_session.cancel()

    def wait(self) -> ProviderSenderResult:
        if self._result is not None:
            return self._result
        result: SandboxResult = self._sandbox_session.wait()
        receipt = result.termination_receipt
        if receipt is None or receipt.unit_name != self.unit_name:
            raise IsolationUnavailable(
                "Provider sender stop could not be proven; staging is retained"
            )
        response = None
        if (
            result.returncode == 0
            and not result.cancelled
            and not result.timed_out
            and not result.output_limited
            and result.input_written
        ):
            try:
                from orchestrator.runtime.provider_sender_process import (
                    decode_provider_sender_response,
                )

                response = decode_provider_sender_response(
                    result.stdout, max_response_bytes=self._max_response_bytes
                )
            except (TypeError, ValueError):
                # A malformed child frame cannot become a provider response.
                response = None
        self._result = ProviderSenderResult(
            unit_name=result.unit_name,
            response=response,
            termination_receipt=receipt,
            returncode=result.returncode,
            elapsed_seconds=result.elapsed_seconds,
            cancelled=result.cancelled,
            timed_out=result.timed_out,
            output_limited=result.output_limited,
            input_written=result.input_written,
        )
        return self._result


class SystemdProviderSenderLauncher:
    """Launch one fixed Python HTTPS sender without a workspace mount.

    Worker and Tool units keep their existing ``PrivateNetwork=yes`` profile.
    This dedicated service explicitly allows ordinary TCP/IP while keeping a
    fixed command, minimal environment, bounded IPC, a private temporary
    directory, and a separate exact unit/cgroup stop receipt.
    """

    def __init__(
        self, *, systemd_run: str | None = None, systemctl: str | None = None
    ) -> None:
        self._systemd_run = systemd_run or shutil.which("systemd-run")
        self._systemctl = systemctl or shutil.which("systemctl")

    def launch(
        self, frame: bytes, *, timeout_seconds: int, output_bytes: int
    ) -> SystemdProviderSenderSession:
        """Start the fixed sender helper with one validated stdin request frame."""

        from orchestrator.runtime.provider_sender_process import (
            MAX_PROVIDER_SENDER_FRAME_BYTES,
            decode_provider_sender_request,
        )

        if not isinstance(frame, bytes) or len(frame) > MAX_PROVIDER_SENDER_FRAME_BYTES:
            raise InvalidSandboxRequest("Provider sender input frame is outside its byte bound")
        try:
            request = decode_provider_sender_request(frame)
        except (TypeError, ValueError) as exc:
            raise InvalidSandboxRequest("Provider sender request frame is invalid") from exc
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 24 * 60 * 60
            or request.timeout_ms > timeout_seconds * 1_000
        ):
            raise InvalidSandboxRequest("Provider sender timeout is outside its bound")
        if (
            isinstance(output_bytes, bool)
            or not isinstance(output_bytes, int)
            or not 1 <= output_bytes <= _MAX_OUTPUT_BYTES
        ):
            raise InvalidSandboxRequest("Provider sender output bound is invalid")
        minimum_frame_output = (
            4 * ((request.max_response_bytes + 2) // 3) + 2_048
        )
        if output_bytes < minimum_frame_output:
            raise InvalidSandboxRequest(
                "Provider sender output bound cannot contain a maximum response frame"
            )
        if platform.system().lower() != "linux":
            raise IsolationUnavailable("the systemd Provider sender is Linux-only")
        if not self._systemd_run or not self._systemctl:
            raise IsolationUnavailable("systemd-run and systemctl are required")

        client_env = _systemd_client_environment()
        try:
            probe = subprocess.run(
                [self._systemctl, "--user", "show-environment"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=client_env,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise IsolationUnavailable("systemd user manager probe failed") from exc
        if probe.returncode != 0:
            raise IsolationUnavailable("a usable systemd user manager is required")
        cgroup_parent = _systemd_cgroup_parent(self._systemctl, client_env)

        staging = _create_provider_staging()
        staging_path = Path(staging.name)
        helper_mount = staging_path / "runtime"
        staged_helper = staging_path / "provider_sender_process.py"
        helper_target = helper_mount / "provider_sender_process.py"
        helper_source = (
            Path(__file__).resolve().parents[1]
            / "runtime"
            / "provider_sender_process.py"
        )
        try:
            helper_source = helper_source.resolve(strict=True)
            if not helper_source.is_file() or not _systemd_path_supported(helper_source):
                raise OSError("trusted Provider helper is not an accepted file")
            helper_mount.mkdir(mode=0o700)
            shutil.copyfile(helper_source, staged_helper)
            shutil.copyfile(helper_source, helper_target)
            os.chmod(staged_helper, 0o400)
            os.chmod(helper_target, 0o400)
        except OSError as exc:
            staging.cleanup()
            raise IsolationUnavailable("trusted Provider sender helper cannot be staged") from exc

        unit_name = f"maestro-provider-{uuid.uuid4().hex}.service"
        scope_cgroup = cgroup_parent / unit_name
        limits = SandboxLimits(
            memory_bytes=512 * 1024 * 1024,
            tasks=64,
            cpu_percent=100,
            timeout_seconds=timeout_seconds,
            output_bytes=output_bytes,
            nofile=128,
            file_bytes=64 * 1024 * 1024,
        )
        properties = _provider_sender_service_properties(
            helper_path=staged_helper,
            helper_target=helper_target,
            timeout_seconds=timeout_seconds,
            output_bytes=output_bytes,
            limits=limits,
        )
        child_command = [
            "/usr/bin/env",
            "-i",
            "PATH=/usr/bin:/bin",
            "LANG=C.UTF-8",
            "LC_ALL=C.UTF-8",
            "TMPDIR=/tmp",
            "/usr/bin/python3",
            "-I",
            "-S",
            str(helper_target),
        ]
        systemd_command = [
            self._systemd_run,
            "--user",
            "--slice=app.slice",
            "--quiet",
            "--wait",
            "--pipe",
            "--collect",
            f"--unit={unit_name.removesuffix('.service')}",
            *(f"--property={value}" for value in properties),
            "--",
            *child_command,
        ]
        try:
            process = subprocess.Popen(
                systemd_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=client_env,
                bufsize=0,
                close_fds=True,
            )
        except OSError as exc:
            staging.cleanup()
            raise IsolationUnavailable("Provider sender transient unit could not start") from exc

        sandbox_session = SandboxSession(
            process=process,
            unit_name=unit_name,
            systemctl=self._systemctl,
            client_env=client_env,
            output_limit=output_bytes,
            timeout_seconds=timeout_seconds,
            staging=staging,
            input_bytes=frame,
            scope_cgroup=scope_cgroup,
            cleanup_on_termination=True,
        )
        return SystemdProviderSenderSession(
            sandbox_session, max_response_bytes=request.max_response_bytes
        )


def _create_provider_staging() -> tempfile.TemporaryDirectory[str]:
    """Create a private staging directory outside any user workspace."""

    for base in (Path(tempfile.gettempdir()), Path("/var/tmp")):
        try:
            resolved = base.resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if not _systemd_path_supported(resolved):
            continue
        staging = None
        try:
            staging = tempfile.TemporaryDirectory(prefix="maestro-provider-", dir=str(resolved))
            os.chmod(staging.name, 0o700)
            return staging
        except OSError:
            if staging is not None:
                staging.cleanup()
            continue
    raise IsolationUnavailable("no supported private Provider sender staging exists")


def _provider_sender_service_properties(
    *,
    helper_path: Path,
    helper_target: Path,
    timeout_seconds: int,
    output_bytes: int,
    limits: SandboxLimits,
) -> tuple[str, ...]:
    """Dedicated outbound-TCP profile; never relaxes the Worker/Tool profile."""

    return (
        "PrivateNetwork=no",
        "PrivateTmp=yes",
        "PrivateUsers=yes",
        "PrivateDevices=yes",
        "ProtectHome=tmpfs",
        "ProtectSystem=strict",
        "ProtectProc=invisible",
        "ProcSubset=pid",
        "ProtectControlGroups=yes",
        "ProtectKernelTunables=yes",
        "ProtectKernelModules=yes",
        "ProtectKernelLogs=yes",
        "ProtectClock=yes",
        "ProtectHostname=yes",
        "NoNewPrivileges=yes",
        "CapabilityBoundingSet=",
        "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
        "RestrictRealtime=yes",
        "RestrictSUIDSGID=yes",
        "LockPersonality=yes",
        "MemoryDenyWriteExecute=yes",
        "InaccessiblePaths=-/run/user",
        "InaccessiblePaths=-/run/dbus",
        "InaccessiblePaths=-/etc/shadow",
        "InaccessiblePaths=-/etc/gshadow",
        "InaccessiblePaths=-/etc/sudoers",
        "InaccessiblePaths=-/etc/sudoers.d",
        "InaccessiblePaths=-/etc/ssh",
        "InaccessiblePaths=-/etc/ssl/private",
        "InaccessiblePaths=-/etc/NetworkManager/system-connections",
        "InaccessiblePaths=-/etc/credstore",
        "InaccessiblePaths=-/etc/credstore.encrypted",
        "InaccessiblePaths=-/etc/apt/auth.conf.d",
        f"BindReadOnlyPaths={helper_path}:{helper_target}",
        "WorkingDirectory=/",
        f"MemoryMax={limits.memory_bytes}",
        "MemorySwapMax=0",
        f"TasksMax={limits.tasks}",
        f"CPUQuota={limits.cpu_percent}%",
        "CPUQuotaPeriodSec=100ms",
        f"RuntimeMaxSec={timeout_seconds}s",
        "TimeoutStopSec=1s",
        "KillMode=control-group",
        f"LimitNOFILE={limits.nofile}",
        f"LimitFSIZE={min(limits.file_bytes, output_bytes)}",
        "LimitCORE=0",
    )


__all__ = [
    "ProviderSenderResult",
    "SystemdProviderSenderLauncher",
    "SystemdProviderSenderSession",
]
