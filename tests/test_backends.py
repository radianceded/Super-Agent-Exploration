import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from agent_runtime.backends import CodexBackend


class BackendTests(unittest.TestCase):
    def test_exact_session_resume_and_shell_boundary(self):
        captured = []
        def popen(command, **options):
            captured.append((command, options))
            path = Path(command[command.index("--output-last-message") + 1])
            path.write_text('{"decisions": []}', encoding="utf-8")
            options["stdout"].write(b'{"type":"thread.started","thread_id":"known-session"}\n')
            return Mock(returncode=0)
        with tempfile.TemporaryDirectory() as temp, patch("agent_runtime.backends.shutil.which", return_value="codex.exe"), patch("agent_runtime.backends.subprocess.Popen", side_effect=popen), patch("agent_runtime.codex_config.inference_options", return_value=[]):
            backend = CodexBackend()
            request = {"context": {"index_file": "local-index"}, "event_ids": [], "events": [{"text": "untrusted full payload"}]}
            _, first = backend.run("router", request, Path(temp))
            _, resumed = backend.run("router", request, Path(temp), first)
        self.assertEqual(first, resumed)
        self.assertNotIn("--ephemeral", captured[0][0])
        self.assertEqual(captured[1][0][-3:], ["resume", "known-session", "-"])
        self.assertNotIn("--last", captured[1][0])
        self.assertFalse(captured[1][1]["shell"])

    def test_timeout_stops_launched_process_without_retry(self):
        import subprocess
        process = Mock()
        process.communicate.side_effect = subprocess.TimeoutExpired("codex", 1)
        with tempfile.TemporaryDirectory() as temp, patch("agent_runtime.backends.shutil.which", return_value="codex.exe"), patch("agent_runtime.backends.subprocess.Popen", return_value=process) as start, patch("agent_runtime.backends.kill_tree") as stop, patch("agent_runtime.codex_config.inference_options", return_value=[]):
            with self.assertRaises(subprocess.TimeoutExpired):
                CodexBackend(timeout=1).run("worker", {}, Path(temp))
        stop.assert_called_once_with(process)
        start.assert_called_once()
