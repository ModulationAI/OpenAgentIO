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
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


SUPPORTED_VERSION = "openagentio.bridge/v1"


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


__all__ = [
    "SUPPORTED_VERSION",
    "BridgeConfig",
    "BridgeConfigError",
    "BridgeDefinition",
    "BridgeMappings",
]
