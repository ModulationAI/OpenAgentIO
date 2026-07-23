"""Bridge configuration parser.

Supports the minimal YAML/JSON schema described in the v0.3.x roadmap:

.. code-block:: yaml

    version: "openagentio.bridge/v1"
    bridges:
      - name: "openclaw.wechat"
        type: "openclaw_chat_sse"
        config:
          base_url: "https://gateway.example/v1"
          token: "${OPENCLAW_GATEWAY_TOKEN}"
          model: "openclaw/default"
          timeout: 30
        mappings:
          text_field: "text"
          session_field: "x-openclaw-session-key"
          metadata_prefix: "openclaw."

YAML loading uses :mod:`pyyaml`, exposed via the optional ``bridge`` extra
(``pip install openagentio[bridge]``). JSON works with the stdlib only.

Environment variable resolution
-------------------------------

String values anywhere inside ``config`` — including nested mappings, lists,
and tuples — may contain placeholders of the form ``${VAR}`` or
``${VAR:-default}``. Resolution is **opt-in** via
:meth:`BridgeDefinition.resolve_env` / :meth:`BridgeConfig.resolve_env`;
it is *not* performed automatically during parsing so that callers can
inspect raw values and so that existing bridge-local resolution keeps
working during transition.

Missing variables without a default raise :class:`BridgeConfigError`.
Non-string values pass through unchanged.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


SUPPORTED_VERSION = "openagentio.bridge/v1"

#: Pattern for ``${VAR}`` and ``${VAR:-default}`` placeholders.
_ENV_PLACEHOLDER_RE = re.compile(r"\$\{(?P<name>[^}:]+)(?::-?(?P<default>[^}]*))?\}")


class BridgeConfigError(ValueError):
    """Raised when the bridge configuration is malformed."""


@dataclass(frozen=True)
class BridgeMappings:
    """Field-mapping hints for translating between Envelope and the
    external system. Concrete bridges decide which keys they consume; unknown
    keys are preserved in :attr:`extra` so future bridge types can extend the
    schema without breaking the parser.
    """

    tool: str = ""
    text_field: str = "text"
    session_field: str = "session_id"
    metadata_prefix: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "BridgeMappings":
        if data is None:
            return cls()
        if not isinstance(data, Mapping):
            raise BridgeConfigError(
                f"'mappings' must be a mapping, got {type(data).__name__}"
            )
        known = {"tool", "text_field", "session_field", "metadata_prefix"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            tool=str(data.get("tool", "") or ""),
            text_field=str(data.get("text_field", "text") or "text"),
            session_field=str(data.get("session_field", "session_id") or "session_id"),
            metadata_prefix=str(data.get("metadata_prefix", "") or ""),
            extra=extra,
        )


@dataclass(frozen=True)
class BridgeDefinition:
    """A single bridge entry parsed from configuration."""

    name: str
    type: str
    config: Mapping[str, Any] = field(default_factory=dict)
    mappings: BridgeMappings = field(default_factory=BridgeMappings)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BridgeDefinition":
        if not isinstance(data, Mapping):
            raise BridgeConfigError(
                f"bridge entry must be a mapping, got {type(data).__name__}"
            )
        name = data.get("name")
        if not isinstance(name, str) or not name:
            raise BridgeConfigError("bridge entry is missing required 'name'")
        kind = data.get("type")
        if not isinstance(kind, str) or not kind:
            raise BridgeConfigError(
                f"bridge entry '{name}' is missing required 'type'"
            )
        raw_config = data.get("config", {}) or {}
        if not isinstance(raw_config, Mapping):
            raise BridgeConfigError(
                f"bridge entry '{name}': 'config' must be a mapping"
            )
        mappings = BridgeMappings.from_dict(data.get("mappings"))
        return cls(name=name, type=kind, config=dict(raw_config), mappings=mappings)

    def resolve_env(self) -> "BridgeDefinition":
        """Return a new definition with environment-variable placeholders in
        :attr:`config` resolved.

        Resolution is opt-in and does not mutate this frozen instance.
        Missing variables without a default raise :class:`BridgeConfigError`.
        """
        resolved = {
            k: _resolve_env_placeholders(v, f"{self.name}.config.{k}")
            for k, v in self.config.items()
        }
        return BridgeDefinition(
            name=self.name,
            type=self.type,
            config=resolved,
            mappings=self.mappings,
        )


@dataclass(frozen=True)
class BridgeConfig:
    """Top-level bridge configuration document."""

    version: str
    bridges: tuple[BridgeDefinition, ...]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BridgeConfig":
        if not isinstance(data, Mapping):
            raise BridgeConfigError(
                f"bridge config root must be a mapping, got {type(data).__name__}"
            )
        version = data.get("version")
        if not isinstance(version, str) or not version:
            raise BridgeConfigError("bridge config is missing required 'version'")
        if version != SUPPORTED_VERSION:
            raise BridgeConfigError(
                f"unsupported bridge config version: {version!r} "
                f"(expected {SUPPORTED_VERSION!r})"
            )
        raw_bridges = data.get("bridges")
        if raw_bridges is None:
            raise BridgeConfigError("bridge config is missing required 'bridges'")
        if not isinstance(raw_bridges, list):
            raise BridgeConfigError("'bridges' must be a list")
        defs = tuple(BridgeDefinition.from_dict(entry) for entry in raw_bridges)
        names: set[str] = set()
        for d in defs:
            if d.name in names:
                raise BridgeConfigError(f"duplicate bridge name: {d.name!r}")
            names.add(d.name)
        return cls(version=version, bridges=defs)

    @classmethod
    def from_file(cls, path: str | Path) -> "BridgeConfig":
        p = Path(path)
        text = p.read_text(encoding="utf-8")
        suffix = p.suffix.lower()
        if suffix in (".yaml", ".yml"):
            data = _load_yaml(text)
        elif suffix == ".json":
            data = json.loads(text)
        else:
            # Best-effort: try YAML if available, otherwise JSON.
            try:
                data = _load_yaml(text)
            except _YAMLUnavailable:
                data = json.loads(text)
        return cls.from_dict(data)

    def resolve_env(self) -> "BridgeConfig":
        """Return a new config with environment-variable placeholders resolved
        in every bridge definition.

        Resolution is opt-in and does not mutate this frozen instance.
        """
        return BridgeConfig(
            version=self.version,
            bridges=tuple(b.resolve_env() for b in self.bridges),
        )


class _YAMLUnavailable(RuntimeError):
    """Sentinel raised when pyyaml is not installed."""


def _load_yaml(text: str) -> Any:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - exercised in env without pyyaml
        raise _YAMLUnavailable(
            "PyYAML is required to load YAML bridge configs. "
            "Install the optional extra: pip install 'openagentio[bridge]'"
        ) from exc
    return yaml.safe_load(text)


def _resolve_env_placeholders(value: Any, source_name: str) -> Any:
    """Resolve ``${VAR}`` / ``${VAR:-default}`` placeholders recursively.

    Strings are substituted. Mappings have their values resolved while keys
    are preserved. Lists and tuples have their elements resolved. All other
    values pass through unchanged. Missing variables without a default raise
    :class:`BridgeConfigError`.
    """
    if isinstance(value, str):

        def replacer(match: "re.Match[str]") -> str:
            name = match.group("name")
            default = match.group("default")
            env_value = os.environ.get(name)
            if env_value is not None:
                return env_value
            if default is not None:
                return default
            raise BridgeConfigError(
                f"environment variable {name!r} is required by {source_name}"
            )

        return _ENV_PLACEHOLDER_RE.sub(replacer, value)

    if isinstance(value, Mapping):
        return {
            k: _resolve_env_placeholders(v, f"{source_name}.{k}")
            for k, v in value.items()
        }

    if isinstance(value, list):
        return [
            _resolve_env_placeholders(v, f"{source_name}[{i}]")
            for i, v in enumerate(value)
        ]

    if isinstance(value, tuple):
        return tuple(
            _resolve_env_placeholders(v, f"{source_name}[{i}]")
            for i, v in enumerate(value)
        )

    return value


__all__ = [
    "SUPPORTED_VERSION",
    "BridgeConfig",
    "BridgeConfigError",
    "BridgeDefinition",
    "BridgeMappings",
]
