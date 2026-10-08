"""Process-local, one-use authority for host artifact reads."""

from dataclasses import dataclass
from datetime import datetime, timezone
import secrets
from threading import Lock

from .store import ArtifactAccessGrant


@dataclass(slots=True)
class _GrantBinding:
    digest: str
    scope: tuple[str, ...]
    expires_at: datetime
    consumed: bool = False


class EphemeralArtifactGrantAuthority:
    """Mint opaque capabilities whose authority never leaves host memory.

    ``ArtifactAccessGrant.signature`` carries a random lookup token, not a
    cryptographic signature. A new authority cannot verify earlier tokens.
    """

    def __init__(self) -> None:
        self._issuer = secrets.token_urlsafe(32)
        self._bindings: dict[str, _GrantBinding] = {}
        self._lock = Lock()

    def issue(self, *, digest: str, run_id: str, expires_at: datetime) -> ArtifactAccessGrant:
        """Bind one fresh capability to one digest, Run, and aware expiry."""
        if run_id == "*":
            raise ValueError("run_id must identify one concrete Run")
        grant = ArtifactAccessGrant(
            digest=digest,
            scope=(run_id,),
            expires_at=expires_at,
            issuer=self._issuer,
            signature=secrets.token_urlsafe(32),
        )
        with self._lock:
            self._bindings[grant.signature] = _GrantBinding(
                digest=grant.digest, scope=grant.scope, expires_at=grant.expires_at,
            )
        return grant

    def verify(self, grant: ArtifactAccessGrant) -> bool:
        """Consume only an exact, unexpired capability issued here."""
        if not isinstance(grant, ArtifactAccessGrant) or not isinstance(grant.signature, str):
            return False
        with self._lock:
            binding = self._bindings.get(grant.signature)
            if (
                binding is None
                or binding.consumed
                or grant.issuer != self._issuer
                or grant.digest != binding.digest
                or grant.scope != binding.scope
                or grant.expires_at != binding.expires_at
                or binding.expires_at <= datetime.now(timezone.utc)
            ):
                return False
            binding.consumed = True
            return True


__all__ = ["EphemeralArtifactGrantAuthority"]
