"""Read only model/transport settings; never copy credentials or tool configuration."""
import json
import os
from pathlib import Path
import re
import tomllib

def inference_options() -> list[str]:
    """Keep the installed user's inference route without importing MCP/hooks.

    In particular, some ChatGPT CLI configurations explicitly use HTTP rather
    than WebSockets. Ignoring that transport can make otherwise valid auth hang.
    Values are argv items, never interpreted by a shell.
    """
    config_root = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    path = config_root / "config.toml"
    if not path.exists():
        return []
    data = tomllib.loads(path.read_text(encoding="utf-8-sig"))
    options = []
    def add(key, value):
        if type(value) in (str, bool, int):
            options.extend(["-c", key + "=" + json.dumps(value, ensure_ascii=False)])
    for key in ("model", "model_provider", "model_reasoning_effort"):
        if key in data:
            add(key, data[key])
    windows_sandbox = data.get("windows", {}).get("sandbox")
    if windows_sandbox in {"unelevated", "elevated"}:
        add("windows.sandbox", windows_sandbox)
    provider = data.get("model_provider")
    if isinstance(provider, str):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", provider):
            raise ValueError("Provider ID must be a simple name for safe CLI overrides")
        prefix = "model_providers." + provider + "."
        config = data.get("model_providers", {}).get(provider, {})
        for key in ("name", "base_url", "wire_api", "requires_openai_auth",
                    "supports_websockets", "supports_standalone_web_search"):
            if key in config:
                add(prefix + key, config[key])
    return options


def clean_environment() -> dict[str, str]:
    # Authenticate through the existing CLI login, without forwarding API tokens,
    # SSH agents, Git overrides, cloud credentials, or arbitrary application env.
    allowed = {
        "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
        "USERPROFILE", "HOME", "APPDATA", "LOCALAPPDATA", "PROGRAMFILES",
        "PROGRAMFILES(X86)", "PROGRAMDATA", "HOMEDRIVE", "HOMEPATH",
        "CODEX_HOME", "LANG", "LC_ALL",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}
