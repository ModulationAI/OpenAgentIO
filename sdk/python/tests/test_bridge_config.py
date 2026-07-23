"""Tests for openagentio.bridge.config — config parsing and validation."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from openagentio.bridge.config import (
    SUPPORTED_VERSION,
    BridgeConfig,
    BridgeConfigError,
    BridgeDefinition,
    BridgeMappings,
)


# ---------------------------------------------------------------------------
# BridgeMappings
# ---------------------------------------------------------------------------


class TestBridgeMappings:
    def test_defaults(self) -> None:
        m = BridgeMappings()
        assert m.tool == ""
        assert m.text_field == "text"
        assert m.session_field == "session_id"
        assert m.metadata_prefix == ""
        assert m.extra == {}

    def test_from_dict_none(self) -> None:
        m = BridgeMappings.from_dict(None)
        assert m == BridgeMappings()

    def test_from_dict_full(self) -> None:
        m = BridgeMappings.from_dict({
            "tool": "send_message",
            "text_field": "content",
            "session_field": "thread_id",
            "metadata_prefix": "openclaw.",
        })
        assert m.tool == "send_message"
        assert m.text_field == "content"
        assert m.session_field == "thread_id"
        assert m.metadata_prefix == "openclaw."

    def test_from_dict_extra_keys_preserved(self) -> None:
        m = BridgeMappings.from_dict({
            "tool": "send_message",
            "custom_key": 42,
        })
        assert m.extra == {"custom_key": 42}

    def test_from_dict_not_mapping_raises(self) -> None:
        with pytest.raises(BridgeConfigError, match="mapping"):
            BridgeMappings.from_dict("not a dict")

    def test_from_dict_empty_values_fall_back(self) -> None:
        m = BridgeMappings.from_dict({"tool": "", "text_field": None})
        assert m.tool == ""
        assert m.text_field == "text"


# ---------------------------------------------------------------------------
# BridgeDefinition
# ---------------------------------------------------------------------------


class TestBridgeDefinition:
    def test_minimal(self) -> None:
        d = BridgeDefinition.from_dict({
            "name": "my.bridge",
            "type": "openclaw_chat_sse",
        })
        assert d.name == "my.bridge"
        assert d.type == "openclaw_chat_sse"
        assert d.config == {}
        assert d.mappings == BridgeMappings()

    def test_full(self) -> None:
        d = BridgeDefinition.from_dict({
            "name": "openclaw.wechat",
            "type": "openclaw_chat_sse",
            "config": {"base_url": "https://gateway.example/v1"},
            "mappings": {"metadata_prefix": "openclaw."},
        })
        assert d.name == "openclaw.wechat"
        assert d.config["base_url"] == "https://gateway.example/v1"
        assert d.mappings.metadata_prefix == "openclaw."

    def test_missing_name(self) -> None:
        with pytest.raises(BridgeConfigError, match="name"):
            BridgeDefinition.from_dict({"type": "openclaw_chat_sse"})

    def test_missing_type(self) -> None:
        with pytest.raises(BridgeConfigError, match="type"):
            BridgeDefinition.from_dict({"name": "x"})

    def test_empty_name(self) -> None:
        with pytest.raises(BridgeConfigError, match="name"):
            BridgeDefinition.from_dict({"name": "", "type": "openclaw_chat_sse"})

    def test_config_not_mapping(self) -> None:
        with pytest.raises(BridgeConfigError, match="config.*mapping"):
            BridgeDefinition.from_dict({
                "name": "x",
                "type": "openclaw_chat_sse",
                "config": 42,
            })

    def test_not_mapping_raises(self) -> None:
        with pytest.raises(BridgeConfigError, match="mapping"):
            BridgeDefinition.from_dict([1, 2])


# ---------------------------------------------------------------------------
# BridgeConfig
# ---------------------------------------------------------------------------


class TestBridgeConfig:
    def _valid_doc(self) -> dict:
        return {
            "version": SUPPORTED_VERSION,
            "bridges": [
                {
                    "name": "openclaw.wechat",
                    "type": "openclaw_chat_sse",
                    "config": {"base_url": "https://gateway.example/v1"},
                }
            ],
        }

    def test_from_dict_happy(self) -> None:
        cfg = BridgeConfig.from_dict(self._valid_doc())
        assert cfg.version == SUPPORTED_VERSION
        assert len(cfg.bridges) == 1
        assert cfg.bridges[0].name == "openclaw.wechat"

    def test_from_dict_multiple_bridges(self) -> None:
        doc = self._valid_doc()
        doc["bridges"].append({"name": "other", "type": "openapi"})
        cfg = BridgeConfig.from_dict(doc)
        assert len(cfg.bridges) == 2

    def test_duplicate_name_rejected(self) -> None:
        doc = self._valid_doc()
        doc["bridges"].append({
            "name": "openclaw.wechat",
            "type": "openclaw_chat_sse",
        })
        with pytest.raises(BridgeConfigError, match="duplicate"):
            BridgeConfig.from_dict(doc)

    def test_missing_version(self) -> None:
        with pytest.raises(BridgeConfigError, match="version"):
            BridgeConfig.from_dict({"bridges": []})

    def test_wrong_version(self) -> None:
        doc = self._valid_doc()
        doc["version"] = "wrong"
        with pytest.raises(BridgeConfigError, match="unsupported"):
            BridgeConfig.from_dict(doc)

    def test_missing_bridges(self) -> None:
        with pytest.raises(BridgeConfigError, match="bridges"):
            BridgeConfig.from_dict({"version": SUPPORTED_VERSION})

    def test_bridges_not_list(self) -> None:
        with pytest.raises(BridgeConfigError, match="list"):
            BridgeConfig.from_dict({"version": SUPPORTED_VERSION, "bridges": "nope"})

    def test_root_not_mapping(self) -> None:
        with pytest.raises(BridgeConfigError, match="mapping"):
            BridgeConfig.from_dict([1, 2])

    def test_from_file_json(self, tmp_path: Path) -> None:
        p = tmp_path / "bridges.json"
        p.write_text(json.dumps(self._valid_doc()))
        cfg = BridgeConfig.from_file(p)
        assert cfg.bridges[0].name == "openclaw.wechat"

    def test_from_file_yaml(self, tmp_path: Path) -> None:
        yaml_text = textwrap.dedent(f"""\
            version: "{SUPPORTED_VERSION}"
            bridges:
              - name: openclaw.wechat
                type: openclaw_chat_sse
                config:
                  base_url: https://gateway.example/v1
        """)
        p = tmp_path / "bridges.yaml"
        p.write_text(yaml_text)
        cfg = BridgeConfig.from_file(p)
        assert cfg.bridges[0].name == "openclaw.wechat"

    def test_from_file_unknown_suffix_tries_yaml_then_json(
        self, tmp_path: Path
    ) -> None:
        p = tmp_path / "bridges.txt"
        p.write_text(json.dumps(self._valid_doc()))
        cfg = BridgeConfig.from_file(p)
        assert cfg.bridges[0].name == "openclaw.wechat"


# ---------------------------------------------------------------------------
# Environment variable resolution
# ---------------------------------------------------------------------------


class TestResolveEnv:
    def test_resolve_plain_placeholder(self, monkeypatch) -> None:
        monkeypatch.setenv("OAI_TEST_URL", "https://example.com")
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"base_url": "${OAI_TEST_URL}"},
        )
        resolved = d.resolve_env()
        assert resolved.config["base_url"] == "https://example.com"
        # Original is unchanged.
        assert d.config["base_url"] == "${OAI_TEST_URL}"

    def test_resolve_placeholder_with_default(self, monkeypatch) -> None:
        monkeypatch.delenv("OAI_MISSING_VAR", raising=False)
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"timeout": "${OAI_MISSING_VAR:-30}"},
        )
        resolved = d.resolve_env()
        assert resolved.config["timeout"] == "30"

    def test_resolve_default_can_be_empty(self, monkeypatch) -> None:
        monkeypatch.delenv("OAI_MISSING_VAR", raising=False)
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"token": "${OAI_MISSING_VAR:-}"},
        )
        resolved = d.resolve_env()
        assert resolved.config["token"] == ""

    def test_missing_var_without_default_raises(self, monkeypatch) -> None:
        monkeypatch.delenv("OAI_MISSING_VAR", raising=False)
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"token": "${OAI_MISSING_VAR}"},
        )
        with pytest.raises(BridgeConfigError, match="OAI_MISSING_VAR"):
            d.resolve_env()

    def test_non_string_value_passes_through(self, monkeypatch) -> None:
        monkeypatch.delenv("OAI_NUM", raising=False)
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"count": 42, "enabled": True},
        )
        resolved = d.resolve_env()
        assert resolved.config["count"] == 42
        assert resolved.config["enabled"] is True

    def test_multiple_placeholders_in_one_value(self, monkeypatch) -> None:
        monkeypatch.setenv("OAI_HOST", "gateway.example")
        monkeypatch.setenv("OAI_PORT", "8080")
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"url": "https://${OAI_HOST}:${OAI_PORT}/v1"},
        )
        resolved = d.resolve_env()
        assert resolved.config["url"] == "https://gateway.example:8080/v1"

    def test_resolve_env_on_config(self, monkeypatch) -> None:
        monkeypatch.setenv("OAI_A", "alpha")
        monkeypatch.setenv("OAI_B", "beta")
        cfg = BridgeConfig(
            version=SUPPORTED_VERSION,
            bridges=(
                BridgeDefinition(name="b1", type="t", config={"x": "${OAI_A}"}),
                BridgeDefinition(name="b2", type="t", config={"y": "${OAI_B}"}),
            ),
        )
        resolved = cfg.resolve_env()
        assert resolved.bridges[0].config["x"] == "alpha"
        assert resolved.bridges[1].config["y"] == "beta"
        assert cfg.bridges[0].config["x"] == "${OAI_A}"

    def test_resolve_nested_mapping_and_list(self, monkeypatch) -> None:
        """MCP-style nested headers/env/args are resolved recursively."""
        monkeypatch.setenv("MCP_TOKEN", "tok123")
        monkeypatch.setenv("API_KEY", "key456")
        monkeypatch.setenv("TOKEN", "tok789")
        d = BridgeDefinition(
            name="mcp",
            type="mcp_tool",
            config={
                "headers": {"Authorization": "Bearer ${MCP_TOKEN}"},
                "env": {"API_KEY": "${API_KEY}"},
                "args": ["--token=${TOKEN}", "--verbose"],
            },
        )
        resolved = d.resolve_env()
        assert resolved.config["headers"]["Authorization"] == "Bearer tok123"
        assert resolved.config["env"]["API_KEY"] == "key456"
        assert resolved.config["args"] == ["--token=tok789", "--verbose"]
        # Original is unchanged.
        assert d.config["headers"]["Authorization"] == "Bearer ${MCP_TOKEN}"

    def test_missing_var_in_nested_mapping_raises(self, monkeypatch) -> None:
        monkeypatch.delenv("NESTED_SECRET", raising=False)
        d = BridgeDefinition(
            name="mcp",
            type="mcp_tool",
            config={"headers": {"Authorization": "Bearer ${NESTED_SECRET}"}},
        )
        with pytest.raises(BridgeConfigError, match="NESTED_SECRET"):
            d.resolve_env()

    def test_resolve_tuple_passthrough(self, monkeypatch) -> None:
        monkeypatch.setenv("OAI_T", "tval")
        d = BridgeDefinition(
            name="b",
            type="t",
            config={"items": ("${OAI_T}", 42)},
        )
        resolved = d.resolve_env()
        assert resolved.config["items"] == ("tval", 42)
        assert isinstance(resolved.config["items"], tuple)
