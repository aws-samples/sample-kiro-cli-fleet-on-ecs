"""Workflow definitions, the output-path guard, the AWS adapters, and the task entry point."""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
import pytest

from runner.aws import S3ResultStore, SqsQueue
from runner.definition import (
    CLI_MCP_PATH,
    CLI_STEERING_PATH,
    InvalidWorkflow,
    WorkflowDefinition,
    seed_cli_configuration,
)
from runner.guard import Snapshot, compare
from runner.workflow import Remedy, Verdict, classify, prepare_scoped_home

WORKFLOWS = Path(__file__).resolve().parents[2] / "workflows/examples"
MCP_SERVERS = Path(__file__).resolve().parents[2] / "workflows/mcp-servers"
REPO_WORKFLOW = WORKFLOWS / "repo-file-summary"
AUDIT_WORKFLOW = WORKFLOWS / "convention-audit"


def write_workflow(directory: Path, **overrides) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    manifest = {
        "name": "probe",
        "description": "for tests",
        "promptFile": "prompt.md",
        "trustTools": ["fs_read", "fs_write"],
        "requiresMcpServers": [],
        "expectsStdout": True,
        **overrides,
    }
    (directory / "workflow.json").write_text(json.dumps(manifest))
    if "promptFile" not in overrides:
        (directory / "prompt.md").write_text("Write output/result.md")
    return directory


class TestTheShippedWorkflow:
    def test_the_repository_example_loads(self) -> None:
        """If this breaks, the image carries a workflow no task can run."""
        definition = WorkflowDefinition.load(REPO_WORKFLOW)

        assert definition.name == "repo-file-summary"
        assert definition.prompt
        assert "fs_write" in definition.trust_tools

    def test_it_grants_a_write_tool_because_it_must_produce_files(self) -> None:
        """The completion check requires output, so a grant without a write tool would
        fail every run."""
        definition = WorkflowDefinition.load(REPO_WORKFLOW)

        assert {"fs_read", "fs_write"} <= set(definition.trust_tools)

    def test_its_prompt_names_where_output_goes(self) -> None:
        definition = WorkflowDefinition.load(REPO_WORKFLOW)

        assert "./output" in definition.prompt

    def test_every_required_server_is_configured(self) -> None:
        definition = WorkflowDefinition.load(REPO_WORKFLOW)

        configured = set(definition.mcp_config["mcpServers"])
        assert set(definition.requires_mcp_servers) <= configured


class TestDefinitionValidation:
    def test_a_missing_manifest_is_reported(self, tmp_path) -> None:
        with pytest.raises(InvalidWorkflow, match="no workflow.json"):
            WorkflowDefinition.load(tmp_path)

    def test_malformed_json_is_reported_with_the_path(self, tmp_path) -> None:
        (tmp_path / "workflow.json").write_text("{not json")

        with pytest.raises(InvalidWorkflow, match="not valid JSON"):
            WorkflowDefinition.load(tmp_path)

    @pytest.mark.parametrize("field", ["name", "promptFile", "trustTools"])
    def test_a_missing_required_field_is_reported(self, tmp_path, field: str) -> None:
        directory = write_workflow(tmp_path / "w")
        manifest = json.loads((directory / "workflow.json").read_text())
        del manifest[field]
        (directory / "workflow.json").write_text(json.dumps(manifest))

        with pytest.raises(InvalidWorkflow, match=field):
            WorkflowDefinition.load(directory)

    def test_a_prompt_file_that_is_not_there_is_reported(self, tmp_path) -> None:
        directory = write_workflow(tmp_path / "w", promptFile="absent.md")

        with pytest.raises(InvalidWorkflow, match="absent.md"):
            WorkflowDefinition.load(directory)

    def test_an_empty_prompt_is_reported(self, tmp_path) -> None:
        directory = write_workflow(tmp_path / "w")
        (directory / "prompt.md").write_text("   \n")

        with pytest.raises(InvalidWorkflow, match="empty"):
            WorkflowDefinition.load(directory)

    def test_a_required_server_with_no_mcp_file_is_reported(self, tmp_path) -> None:
        directory = write_workflow(tmp_path / "w", requiresMcpServers=["thinking"])

        with pytest.raises(InvalidWorkflow, match="no mcp.json"):
            WorkflowDefinition.load(directory)

    def test_a_required_server_missing_from_mcp_json_is_reported(self, tmp_path) -> None:
        """Caught here rather than left to the CLI, whose failure for this is the same
        exit code as its failure for anything else about MCP."""
        directory = write_workflow(tmp_path / "w", requiresMcpServers=["thinking"])
        (directory / "mcp.json").write_text(json.dumps({"mcpServers": {"other": {}}}))

        with pytest.raises(InvalidWorkflow, match="thinking"):
            WorkflowDefinition.load(directory)


class TestSeedingTheCliConfiguration:
    def test_the_mcp_configuration_lands_where_the_cli_reads_it(self, tmp_path) -> None:
        definition = WorkflowDefinition.load(REPO_WORKFLOW)
        home = tmp_path / "home"

        seed_cli_configuration(home, definition)

        written = json.loads((home / CLI_MCP_PATH).read_text())
        assert "sequentialthinking" in written["mcpServers"]

    def test_seeding_replaces_whatever_was_there(self, tmp_path) -> None:
        """So a task cannot inherit configuration from anything but the image."""
        home = tmp_path / "home"
        (home / CLI_MCP_PATH).parent.mkdir(parents=True)
        (home / CLI_MCP_PATH).write_text(json.dumps({"mcpServers": {"stale": {}}}))

        seed_cli_configuration(home, WorkflowDefinition.load(REPO_WORKFLOW))

        written = json.loads((home / CLI_MCP_PATH).read_text())
        assert "stale" not in written["mcpServers"]

    def test_a_workflow_with_no_servers_writes_nothing(self, tmp_path) -> None:
        directory = write_workflow(tmp_path / "w")
        home = tmp_path / "home"

        seed_cli_configuration(home, WorkflowDefinition.load(directory))

        assert not (home / CLI_MCP_PATH).exists()


def write_steering(directory: Path, **files: str) -> Path:
    steering = directory / "steering"
    steering.mkdir(parents=True, exist_ok=True)
    for name, body in files.items():
        (steering / f"{name}.md").write_text(body)
    return steering


class TestSteeringAsPayload:
    """The claim this pattern rests on: a team's existing instructions run on the fleet.

    If steering does not reach the CLI, the fleet is running a prompt and calling it a
    workflow. Nothing about a result would reveal the difference, which is why this is
    covered rather than assumed.
    """

    def test_the_example_that_demonstrates_it_loads(self) -> None:
        definition = WorkflowDefinition.load(AUDIT_WORKFLOW)

        assert definition.name == "convention-audit"
        assert [path.name for path in definition.steering_files] == [
            "error-handling.md",
            "naming.md",
            "reporting.md",
        ]

    def test_files_are_ordered_so_two_tasks_see_the_same_thing(self) -> None:
        """Directory order is not defined. Sorted, so the instructions a task receives do
        not depend on the filesystem it was unpacked onto."""
        definition = WorkflowDefinition.load(AUDIT_WORKFLOW)

        assert list(definition.steering_files) == sorted(definition.steering_files)

    def test_steering_lands_where_the_cli_reads_it(self, tmp_path) -> None:
        home = tmp_path / "home"

        seed_cli_configuration(home, WorkflowDefinition.load(AUDIT_WORKFLOW))

        landed = sorted(path.name for path in (home / CLI_STEERING_PATH).glob("*.md"))
        assert landed == ["error-handling.md", "naming.md", "reporting.md"]

    def test_content_arrives_intact(self, tmp_path) -> None:
        """Copied, not summarized or templated. The file the team wrote is the file the
        model sees."""
        home = tmp_path / "home"
        definition = WorkflowDefinition.load(AUDIT_WORKFLOW)

        seed_cli_configuration(home, definition)

        for source in definition.steering_files:
            assert (home / CLI_STEERING_PATH / source.name).read_text() == (
                source.read_text()
            )

    def test_seeding_clears_a_previous_workflow_s_steering(self, tmp_path) -> None:
        """Otherwise a task is governed partly by rules it does not carry, and the output
        looks plausible either way."""
        home = tmp_path / "home"
        stale = home / CLI_STEERING_PATH / "from-another-workflow.md"
        stale.parent.mkdir(parents=True)
        stale.write_text("# rules this workflow never shipped")

        seed_cli_configuration(home, WorkflowDefinition.load(AUDIT_WORKFLOW))

        assert not stale.exists()

    def test_a_workflow_carrying_none_leaves_no_steering_behind(self, tmp_path) -> None:
        home = tmp_path / "home"
        stale = home / CLI_STEERING_PATH / "stale.md"
        stale.parent.mkdir(parents=True)
        stale.write_text("# leftover")

        seed_cli_configuration(home, WorkflowDefinition.load(REPO_WORKFLOW))

        assert not stale.exists()

    def test_an_undeclared_directory_is_an_error_not_a_silent_skip(self, tmp_path) -> None:
        """The failure being prevented is invisible: the run still produces output, just
        not to the conventions it was written against."""
        directory = write_workflow(tmp_path / "w")
        write_steering(directory, naming="# rules")

        with pytest.raises(InvalidWorkflow, match="would be ignored"):
            WorkflowDefinition.load(directory)

    def test_declaring_steering_without_shipping_it_is_an_error(self, tmp_path) -> None:
        directory = write_workflow(tmp_path / "w", steering=True)

        with pytest.raises(InvalidWorkflow, match="no steering/ directory"):
            WorkflowDefinition.load(directory)

    def test_no_workflow_fetches_its_mcp_server_at_run_time(self) -> None:
        """`npx -y` was 94% of one run's network traffic, and fatal in the deployed VPC.

        Egress is restricted to named destinations and npm is not among them, so a workflow
        that downloads its server at start would fail `--require-mcp-startup` and take the
        task with it. It also meant the agent's tooling was whatever the registry served at
        that moment, while the image around it was pinned by digest and checksum.

        Servers are installed into the image from a lockfile instead. This keeps it that
        way, because the broken form is the one every MCP example on the internet shows.
        """
        for mcp_path in sorted(WORKFLOWS.glob("*/mcp.json")):
            servers = json.loads(mcp_path.read_text()).get("mcpServers", {})
            for name, spec in servers.items():
                command = spec.get("command", "")
                assert command not in ("npx", "npm", "pnpm", "yarn", "uvx", "pipx"), (
                    f"{mcp_path.parent.name}/{name} fetches its server at run time via "
                    f"{command!r}; install it in workflows/mcp-servers instead"
                )

    def test_mcp_commands_are_portable_between_a_container_and_a_laptop(self) -> None:
        """A bare command name, resolved on PATH, not a path inside the image.

        A config naming /opt/mcp-servers would have to differ between a local run and a
        task, and a local run that exercises a different configuration is not evidence
        about the task.
        """
        for mcp_path in sorted(WORKFLOWS.glob("*/mcp.json")):
            servers = json.loads(mcp_path.read_text()).get("mcpServers", {})
            for name, spec in servers.items():
                command = spec.get("command", "")
                assert command and "/" not in command, (
                    f"{mcp_path.parent.name}/{name} names {command!r}; use the command "
                    "the package installs, so the same config works in both places"
                )

    def test_every_declared_server_is_one_the_image_installs(self) -> None:
        """The workflow and the lockfile have to agree, and nothing else checks that.

        A workflow naming a server absent from workflows/mcp-servers produces an image
        that builds and a task that fails at start. The Dockerfile asserts the shipped
        example's server is on PATH; this catches the same mistake for any workflow added
        later, without needing a build.
        """
        manifest = json.loads((MCP_SERVERS / "package.json").read_text())
        installed = set(manifest.get("dependencies", {}))

        for mcp_path in sorted(WORKFLOWS.glob("*/mcp.json")):
            servers = json.loads(mcp_path.read_text()).get("mcpServers", {})
            for name, spec in servers.items():
                command = spec["command"]
                # The convention npm packages follow: a `server-x` package installs
                # `mcp-server-x`. Checked by suffix so the assertion does not need the
                # package's bin map, which is not in the lockfile.
                suffix = command.removeprefix("mcp-")
                assert any(package.endswith(suffix) for package in installed), (
                    f"{mcp_path.parent.name}/{name} runs {command!r}, which no package in "
                    f"workflows/mcp-servers provides. Installed: {sorted(installed)}"
                )

    def test_the_sentinel_rule_lives_only_in_steering(self) -> None:
        """What makes the audit example able to answer whether steering was applied.

        `AUDIT-COMPLETE:` is required by `steering/reporting.md` and must appear in no
        prompt. A model cannot emit it unless the steering reached it, so its presence in
        a result is evidence the CLI loaded user-level steering in a headless session —
        which is otherwise awkward to establish without an interactive session. Copying
        the rule into the prompt would destroy the evidence while leaving every test
        passing, so the invariant is asserted rather than trusted to a comment.
        """
        definition = WorkflowDefinition.load(AUDIT_WORKFLOW)
        steering = "\n".join(path.read_text() for path in definition.steering_files)

        assert "AUDIT-COMPLETE" in steering
        assert "AUDIT-COMPLETE" not in definition.prompt

    def test_a_local_run_starts_from_the_same_configuration_as_a_task(
        self, tmp_path
    ) -> None:
        """The claim that makes local mode worth having.

        A local run isolates HOME so a developer's own configuration cannot decide
        whether it succeeds. An isolated HOME with nothing in it is reproducible but not
        comparable: on a task the CLI reads the workflow's steering and MCP servers from
        under HOME, so without them a local run tells you about the loop and nothing about
        the workflow. `python3` stands in for the CLI here because the function only needs
        an executable it can locate.
        """
        scoped = prepare_scoped_home(
            tmp_path / "run", "python3", WorkflowDefinition.load(AUDIT_WORKFLOW)
        )

        assert scoped is not None
        landed = sorted(path.name for path in (scoped / CLI_STEERING_PATH).glob("*.md"))
        assert landed == ["error-handling.md", "naming.md", "reporting.md"]

    def test_an_isolated_home_without_a_definition_stays_empty(self, tmp_path) -> None:
        """Still supported, for the ad-hoc `--prompt` path that carries no definition."""
        scoped = prepare_scoped_home(tmp_path / "run", "python3")

        assert scoped is not None
        assert not (scoped / CLI_STEERING_PATH).exists()

    def test_an_empty_steering_directory_is_an_error(self, tmp_path) -> None:
        """A misspelled extension would otherwise produce a workflow that declares
        conventions and carries none."""
        directory = write_workflow(tmp_path / "w", steering=True)
        (directory / "steering").mkdir()
        (directory / "steering" / "naming.txt").write_text("# wrong extension")

        with pytest.raises(InvalidWorkflow, match="no .md files"):
            WorkflowDefinition.load(directory)


class TestOutputPathGuard:
    def workspace(self, tmp_path: Path) -> Path:
        workspace = tmp_path / "ws"
        (workspace / "input").mkdir(parents=True)
        (workspace / "output").mkdir(parents=True)
        (workspace / "input" / "source.c").write_text("int main(void){return 0;}")
        return workspace

    def test_writing_only_under_output_is_allowed(self, tmp_path) -> None:
        workspace = self.workspace(tmp_path)
        before = Snapshot.of(workspace)

        (workspace / "output" / "result.md").write_text("fine")

        assert not compare(before, Snapshot.of(workspace))

    def test_creating_a_file_elsewhere_is_a_violation(self, tmp_path) -> None:
        workspace = self.workspace(tmp_path)
        before = Snapshot.of(workspace)

        (workspace / "sneaky.txt").write_text("should not be here")

        violations = compare(before, Snapshot.of(workspace))
        assert violations
        assert "sneaky.txt" in violations.created

    def test_modifying_an_input_is_a_violation(self, tmp_path) -> None:
        """Inputs are shared; a workflow that edits one corrupts the next item's run."""
        workspace = self.workspace(tmp_path)
        before = Snapshot.of(workspace)

        (workspace / "input" / "source.c").write_text("int main(void){return 1;} // edited")

        violations = compare(before, Snapshot.of(workspace))
        assert "input/source.c" in violations.modified

    def test_deleting_an_input_is_a_violation(self, tmp_path) -> None:
        workspace = self.workspace(tmp_path)
        before = Snapshot.of(workspace)

        (workspace / "input" / "source.c").unlink()

        assert "input/source.c" in compare(before, Snapshot.of(workspace)).removed

    def test_the_worker_scratch_directory_is_not_a_violation(self, tmp_path) -> None:
        workspace = self.workspace(tmp_path)
        before = Snapshot.of(workspace)

        (workspace / ".run").mkdir()
        (workspace / ".run" / "transcript.txt").write_text("worker's own")

        assert not compare(before, Snapshot.of(workspace))

    def test_a_violation_outranks_an_otherwise_clean_run(self, tmp_path) -> None:
        """Results from a run that wrote where it should not are not trustworthy, whatever
        its exit code said."""
        output = tmp_path / "output" / "result.md"
        output.parent.mkdir(parents=True)
        output.write_text("looks fine")

        verdict = classify(
            0, "an answer", "", (output,), wrote_outside_output=True
        )

        assert verdict is Verdict.WROTE_OUTSIDE_OUTPUT

    def test_a_violation_is_dead_lettered_rather_than_retried(self) -> None:
        """Retrying would repeat it; the definition is what needs fixing."""
        from runner.workflow import REMEDIES

        assert REMEDIES[Verdict.WROTE_OUTSIDE_OUTPUT] is Remedy.DEAD_LETTER


@pytest.fixture
def sqs(aws):
    client = aws.client("sqs")
    url = client.create_queue(QueueName="work")["QueueUrl"]
    return SqsQueue(client, url, wait_seconds=0)


@pytest.fixture
def s3(aws):
    client = aws.client("s3")
    client.create_bucket(Bucket="results")
    client.create_bucket(Bucket="reference")
    return S3ResultStore(client, bucket="results", reference_bucket="reference")


def seed_message(queue: SqsQueue, item_id: str, prompt: str, inputs=()) -> None:
    queue.client.send_message(
        QueueUrl=queue.queue_url,
        MessageBody=json.dumps({"itemId": item_id, "prompt": prompt, "inputs": list(inputs)}),
    )


class TestSqsQueue:
    def test_a_received_item_carries_what_the_worker_needs(self, sqs) -> None:
        seed_message(sqs, "item-0", "do the thing", inputs=("a.c",))

        item = sqs.receive()

        assert (item.item_id, item.prompt, item.inputs) == ("item-0", "do the thing", ("a.c",))
        assert item.receipt

    def test_an_empty_queue_returns_nothing(self, sqs) -> None:
        assert sqs.receive() is None

    def test_a_received_item_is_invisible_to_another_worker(self, sqs) -> None:
        """This is how a work item is owned: no lock of the worker's own is needed."""
        seed_message(sqs, "item-0", "go")
        sqs.receive()

        assert sqs.receive() is None

    def test_deleting_removes_the_item(self, sqs) -> None:
        seed_message(sqs, "item-0", "go")
        item = sqs.receive()

        sqs.delete(item.receipt)
        sqs.client.purge_queue  # nothing hidden: the queue is simply empty now

        assert sqs.depth() == 0

    def test_releasing_makes_it_available_again(self, sqs) -> None:
        seed_message(sqs, "item-0", "go")
        item = sqs.receive()

        sqs.release(item.receipt)

        assert sqs.receive() is not None

    def test_extending_keeps_ownership_of_a_slow_item(self, sqs) -> None:
        """Without it a long run gets a second worker on the same item."""
        seed_message(sqs, "item-0", "go")
        item = sqs.receive()

        sqs.extend(item.receipt, seconds=120)

        assert sqs.receive() is None


class TestS3ResultStore:
    def test_results_are_stored_under_a_key_derived_from_the_item(
        self, s3, tmp_path
    ) -> None:
        output = tmp_path / "output"
        output.mkdir()
        (output / "summary.md").write_text("a summary")

        location = s3.store("item-0", ((output / "summary.md"),), output)

        assert "item-0" in location
        assert s3.results_for("item-0") == ["results/item-0/summary.md"]

    def test_a_retry_overwrites_rather_than_duplicating(self, s3, tmp_path) -> None:
        """Deterministic keys are what make redelivery harmless."""
        output = tmp_path / "output"
        output.mkdir()
        (output / "summary.md").write_text("first")
        s3.store("item-0", ((output / "summary.md"),), output)

        (output / "summary.md").write_text("second")
        s3.store("item-0", ((output / "summary.md"),), output)

        assert s3.results_for("item-0") == ["results/item-0/summary.md"]
        body = s3.client.get_object(Bucket="results", Key="results/item-0/summary.md")
        assert body["Body"].read() == b"second"

    def test_nested_output_keeps_its_shape(self, s3, tmp_path) -> None:
        output = tmp_path / "output"
        (output / "src").mkdir(parents=True)
        (output / "src" / "a.md").write_text("nested")

        s3.store("item-0", ((output / "src" / "a.md"),), output)

        assert s3.results_for("item-0") == ["results/item-0/src/a.md"]

    def test_inputs_come_from_the_reference_bucket(self, s3, tmp_path) -> None:
        """Separate from results: inputs are shared and read only, so one grant over both
        would be wider than the task needs."""
        s3.client.put_object(Bucket="reference", Key="source.c", Body=b"int main(void){}")
        destination = tmp_path / "input"

        s3.fetch_inputs(("source.c",), destination)

        assert (destination / "source.c").read_bytes() == b"int main(void){}"

    def test_a_missing_input_is_reported(self, s3, tmp_path) -> None:
        with pytest.raises(FileNotFoundError, match="absent.c"):
            s3.fetch_inputs(("absent.c",), tmp_path / "input")

    def test_a_nested_object_keeps_its_path(self, s3, tmp_path) -> None:
        """The same layout the local store gives: `into/<key>`."""
        s3.client.put_object(Bucket="reference", Key="src/lib/a.c", Body=b"a")

        s3.fetch_inputs(("src/lib/a.c",), tmp_path / "input")

        assert (tmp_path / "input" / "src" / "lib" / "a.c").read_bytes() == b"a"

    @pytest.mark.parametrize("name", ["data/sub/", "data/sub"])
    def test_a_prefix_holding_one_object_is_laid_out_like_any_prefix(
        self, s3, tmp_path, name: str
    ) -> None:
        """Measured as a bug: one object under a prefix landed as a file named `sub`."""
        s3.client.put_object(Bucket="reference", Key="data/sub/x.txt", Body=b"x")

        s3.fetch_inputs((name,), tmp_path / "input")

        assert (tmp_path / "input" / "data" / "sub" / "x.txt").read_bytes() == b"x"
        assert not (tmp_path / "input" / "sub").exists()

    def test_a_prefix_does_not_match_a_longer_sibling(self, s3, tmp_path) -> None:
        s3.client.put_object(Bucket="reference", Key="data/sub/x.txt", Body=b"x")
        s3.client.put_object(Bucket="reference", Key="data/subway/y.txt", Body=b"y")

        s3.fetch_inputs(("data/sub",), tmp_path / "input")

        assert not (tmp_path / "input" / "data" / "sub" / "way").exists()
        assert sorted(p.name for p in (tmp_path / "input").rglob("*") if p.is_file()) == [
            "x.txt"
        ]

    def test_a_denied_head_is_raised_not_reported_as_missing(self, s3, tmp_path) -> None:
        from botocore.exceptions import ClientError

        def denied(**_kwargs):
            raise ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
            )

        s3.client.head_object = denied

        with pytest.raises(ClientError, match="403"):
            s3.fetch_inputs(("source.c",), tmp_path / "input")


class TestTheTaskEntryPoint:
    def test_missing_configuration_exits_before_touching_work(
        self, monkeypatch, isolated_aws_environment
    ) -> None:
        from runner.main import EXIT_CONFIGURATION, main

        for name in (
            "SUBSCRIPTIONS_TABLE", "LEASES_TABLE", "WORK_QUEUE_URL",
            "RESULTS_BUCKET", "WORKFLOW_DIR",
        ):
            monkeypatch.delenv(name, raising=False)

        assert main([]) == EXIT_CONFIGURATION

    def test_an_unusable_workflow_definition_exits_as_configuration(
        self, monkeypatch, tmp_path, isolated_aws_environment
    ) -> None:
        from runner.main import EXIT_CONFIGURATION, main

        for name, value in {
            "SUBSCRIPTIONS_TABLE": "subs",
            "LEASES_TABLE": "leases",
            "WORK_QUEUE_URL": "http://localhost/queue",
            "RESULTS_BUCKET": "results",
            "WORKFLOW_DIR": str(tmp_path / "nothing-here"),
        }.items():
            monkeypatch.setenv(name, value)

        assert main([]) == EXIT_CONFIGURATION

    def test_the_exit_statuses_are_distinct(self) -> None:
        """Scale-in makes several outcomes normal, and a fleet cannot tell them from a
        crash if they share a status."""
        from runner.main import (
            EXIT_CONFIGURATION,
            EXIT_ENVIRONMENT_BROKEN,
            EXIT_OK,
            EXIT_SATURATED,
        )

        statuses = {EXIT_OK, EXIT_SATURATED, EXIT_ENVIRONMENT_BROKEN, EXIT_CONFIGURATION}
        assert len(statuses) == 4
        assert EXIT_OK == 0


class TestTheWorkerLoopDoesNotOwnTheWorkflowContract:
    """What the loop supplies and what the workflow declares must not overlap.

    A real defect, found by deploying: `Worker` carried its own `trust_tools` and the wrapper in
    `main.py` supplied the definition's as well. Python saw a duplicate keyword and raised, but
    only on a task — the local runner deleted one of them before calling, so every local run
    passed and the first deployed task failed after it had already claimed a subscription.

    The workaround is what made this invisible. These assertions are about the shape of the
    seam rather than any behaviour, because the failure was a shape.
    """

    def test_the_worker_does_not_accept_a_tool_grant(self) -> None:
        import inspect

        from runner.worker import Worker

        parameters = inspect.signature(Worker.__init__).parameters

        assert "trust_tools" not in parameters, (
            "the workflow definition owns the tool grant; a second source for it is what "
            "produced a duplicate keyword argument on a task"
        )

    def test_the_worker_passes_no_grant_to_the_run_callable(self) -> None:
        """Asserted on the call rather than the signature, so adding it back is caught."""
        import inspect

        from runner.worker import Worker

        source = inspect.getsource(Worker._process)

        assert "trust_tools" not in source

    def test_the_run_callable_supplies_everything_the_definition_declares(self) -> None:
        """The other half: whoever does own it has to pass all of it.

        `expects_stdout` and `definition` travel with the grant. Dropping one of them silently
        changes how a run is judged — `expects_stdout` false becoming true fails every run that
        correctly writes a file and says nothing.
        """
        import inspect

        import runner.main

        source = inspect.getsource(runner.main.main)

        for supplied in (
            "trust_tools=definition.trust_tools_argument",
            "expects_stdout=definition.expects_stdout",
            "definition=definition",
        ):
            assert supplied in source, f"the run wrapper no longer supplies {supplied}"

    def test_the_run_timeout_and_the_protection_expiry_come_from_one_figure(self) -> None:
        """Protection must outlast the run. Two figures could drift until it did not."""
        import inspect

        import runner.main

        source = inspect.getsource(runner.main.main)

        assert "timeout_seconds=workflow_timeout" in source
        assert "workflow_timeout_seconds=workflow_timeout" in source
