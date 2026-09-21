"""Deterministic, fail-closed redaction for observations.

Observations (logs, traces, metrics) and any projection explanation must pass
through a :class:`Redactor` before they are stored or exposed.  Redaction is
the boundary that keeps secret values, secret-like headers, authorization
fields, and protected filesystem paths out of the observability plane.

The redactor never mutates committed events.  It only produces cleaned copies
of the free-form content handed to it.  Redaction is *fail closed*: a value it
cannot represent deterministically raises :class:`RedactionError` so the caller
can abort the observation write instead of emitting a raw fallback.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

REDACTED_MARKER = "[REDACTED]"
PROTECTED_PATH_MARKER = "[PROTECTED_PATH]"

# Bumping this invalidates cached ``redaction_version`` stamps on observations
# built by an older ruleset.
REDACTION_VERSION = 1

# Keys whose *normalized* form (lowercased, non-alphanumerics stripped) equals
# one of these are always redacted, whatever the value type.  Kept as exact
# matches so token-count fields such as ``reserved_tokens`` are not caught.
_EXACT_SENSITIVE_KEYS = frozenset(
    {
        "token",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "bearertoken",
        "sessiontoken",
        "authtoken",
    }
)

# Keys whose normalized form *contains* one of these substrings are redacted.
_SENSITIVE_KEY_SUBSTRINGS = (
    "authorization",
    "password",
    "passwd",
    "secret",
    "apikey",
    "accesskey",
    "privatekey",
    "clientsecret",
    "cookie",
    "credential",
    "passphrase",
)


def _normalize_key(key: str) -> str:
    return "".join(character for character in key.lower() if character.isalnum())


class RedactionError(RuntimeError):
    """Raised when a value cannot be redacted deterministically.

    The observation pipeline treats this as a write failure and stores nothing;
    it never falls back to emitting the raw value.
    """


class Redactor:
    """Recursively redacts secrets and protected paths from free-form content."""

    def __init__(
        self,
        secret_values: Sequence[str] | None = None,
        protected_paths: Sequence[str] | None = None,
        *,
        redaction_version: int = REDACTION_VERSION,
    ) -> None:
        # Longer secrets first so overlapping fragments collapse cleanly.
        self._secret_values: tuple[str, ...] = tuple(
            sorted(
                {value for value in (secret_values or []) if value},
                key=len,
                reverse=True,
            )
        )
        self._protected_paths: tuple[str, ...] = tuple(
            value for value in (protected_paths or []) if value
        )
        self._redaction_version = int(redaction_version)

    @property
    def redaction_version(self) -> int:
        return self._redaction_version

    def clean(self, value: Any) -> Any:
        """Return a redacted copy of ``value``.

        Supported inputs are JSON-shaped values (``dict``, ``list``, ``str``,
        ``int``, ``float``, ``bool``, ``None``), datetimes, and exceptions.
        Anything else raises :class:`RedactionError`.
        """

        return self._clean(value, key=None)

    def clean_text(self, text: str) -> str:
        """Redact a single string value."""

        return self._clean_string(text)

    # -- internals ------------------------------------------------------------

    def _clean(self, value: Any, *, key: str | None) -> Any:
        if key is not None and self._is_sensitive_key(key):
            return REDACTED_MARKER

        if isinstance(value, Mapping):
            return {
                str(child_key): self._clean(child_value, key=str(child_key))
                for child_key, child_value in value.items()
            }
        # ``bool`` is a subclass of ``int``; handle it before the str/bytes and
        # sequence branches so it is preserved as-is.
        if isinstance(value, bool) or value is None or isinstance(value, int):
            return value
        if isinstance(value, float):
            return value
        if isinstance(value, str):
            return self._clean_string(value)
        if isinstance(value, BaseException):
            return self._clean_string(f"{type(value).__name__}: {value}")
        if isinstance(value, (datetime, date)):
            return self._clean_string(value.isoformat())
        if isinstance(value, (list, tuple)):
            return [self._clean(item, key=None) for item in value]
        raise RedactionError(
            f"cannot redact value of type {type(value).__name__!r}"
        )

    def _clean_string(self, text: str) -> str:
        cleaned = text
        for secret in self._secret_values:
            if secret in cleaned:
                cleaned = cleaned.replace(secret, REDACTED_MARKER)
        for protected in self._protected_paths:
            if protected in cleaned:
                return PROTECTED_PATH_MARKER
        return cleaned

    def _is_sensitive_key(self, key: str) -> bool:
        normalized = _normalize_key(key)
        if normalized in _EXACT_SENSITIVE_KEYS:
            return True
        return any(fragment in normalized for fragment in _SENSITIVE_KEY_SUBSTRINGS)


__all__ = [
    "PROTECTED_PATH_MARKER",
    "REDACTED_MARKER",
    "REDACTION_VERSION",
    "RedactionError",
    "Redactor",
]
