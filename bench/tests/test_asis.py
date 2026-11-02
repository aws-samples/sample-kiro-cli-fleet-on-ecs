"""M1e workload: the as-is spec workflow's shape on synthetic input."""

import json

from fleetbench import asis, fleet


def test_check_value_is_the_last_line_of_each_function(tmp_path):
    source = asis.source_c(3, 5)
    blocks = source.split("/* SF-")
    assert f"SF-0003 check value: {asis.check_value(3)}" in source
    # The check constant follows the function body, so a read that stops early misses it.
    assert blocks[-1].strip().endswith(f"check value: {asis.check_value(3)} */")


def test_workspace_has_four_agents_with_as_is_models(tmp_path):
    asis.write_workspace(tmp_path, 4, 10, "claude-sonnet-4.6", "claude-opus-4.8", prompt_words=40)
    agents = {p.stem: json.loads(p.read_text()) for p in (tmp_path / ".kiro/agents").glob("*.json")}
    assert set(agents) == {asis.ORCHESTRATOR, asis.COLLECTOR, asis.GENERATOR, asis.REVIEWER}
    assert agents[asis.GENERATOR]["model"] == "claude-opus-4.8"
    assert agents[asis.REVIEWER]["model"] == "claude-sonnet-4.6"
    # Only the orchestrator may delegate; subagents get read, write and the shell.
    assert "use_subagent" in agents[asis.ORCHESTRATOR]["allowedTools"]
    assert "use_subagent" not in agents[asis.COLLECTOR]["allowedTools"]
    assert "execute_bash" in agents[asis.COLLECTOR]["allowedTools"]
    assert (tmp_path / "tools/validate.py").is_file()


def test_empty_model_falls_back_to_the_cli_default(tmp_path):
    configs = asis.agent_configs("", "", 10)
    assert all("model" not in c for c in configs.values())


def test_prompt_is_sequential_by_default_and_batches_when_parallel():
    assert "one subagent at a time" in asis.prompt(4, 1)
    assert "up to 4 subagents in parallel" in asis.prompt(4, 4)


def test_final_total(tmp_path):
    (tmp_path / "output").mkdir()
    assert asis.final_total_ok(tmp_path, 4) is None
    total = asis.expected_total(4)
    (tmp_path / "output/final.md").write_text(f"Sum of check values: {total:,}\n")
    assert asis.final_total_ok(tmp_path, 4) is True


def test_launcher_forwards_asis_arguments():
    args = fleet._parse_args(
        ["launch", "--workload", "asis", "--sub-functions", "6", "--parallel", "2"]
    )
    command = fleet.Launcher._command(type("L", (), {"args": args, "profile_name": "asis"})(), 3, 1.0, "r")
    assert command[command.index("--workload") + 1] == "asis"
    assert command[command.index("--sub-functions") + 1] == "6"
    assert command[command.index("--parallel") + 1] == "2"
    assert command[command.index("--generator-model") + 1] == "claude-opus-4.8"


def test_orchestrator_may_only_spawn_the_registered_agents():
    orchestrator = asis.agent_configs("m", "g", 10)[asis.ORCHESTRATOR]
    delegates = [asis.COLLECTOR, asis.GENERATOR, asis.REVIEWER]
    for key in ("use_subagent", "subagent"):
        assert orchestrator["toolsSettings"][key]["availableAgents"] == delegates
        assert orchestrator["toolsSettings"][key]["trustedAgents"] == delegates
    assert "never\ndelegate to the default agent" in asis.prompt(4, 1)


def test_subagent_invocations_and_expected_delegations():
    log = (
        "Invoking subagent: asis-data-collector with query: x\n"
        "Invoking subagent: asis-spec-generator with query: y\n"
        "Invoking subagent: asis-spec-generator with query: z\n"
    )
    assert asis.subagent_invocations(log) == {"asis-data-collector": 1, "asis-spec-generator": 2}
    coloured = "\x1b[38;5;141mInvoking subagent: \x1b[0m\x1b[1masis-quality-reviewer\x1b[0m with query"
    assert asis.subagent_invocations(coloured) == {"asis-quality-reviewer": 1}
    assert asis.delegation_denied("Subagent cannot run in non-interactive mode with tool permission request")
    assert not asis.delegation_denied(log)
    assert asis.expected_delegations(4) == 12


def test_denial_context_keeps_the_lines_before_an_early_denial():
    log = "\n".join(
        ["[tool] Reading seed/meta.json"]
        + ["Subagent cannot run in non-interactive mode with tool permission request"]
        + [f"[tool] Running: python3 tools/validate.py {i}" for i in range(40)]
    )
    context = asis.denial_context(log)
    assert context[0] == "[tool] Reading seed/meta.json"
    assert any("tool permission request" in line for line in context)
    assert len(context) <= 4
