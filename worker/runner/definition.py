"""Loading a workflow definition, and placing its configuration where the CLI reads it.

A workflow is data the image carries. This reads it and does the setup the CLI needs
before it starts: MCP server configuration and steering files have to be on disk under
`HOME`, because that is where the CLI looks for them.

Steering is the part that makes this pattern worth the trouble. The instructions a team
already trusts locally are markdown files in a directory, so they move onto the fleet as
they are rather than being reimplemented against an SDK. That is the whole claim of
prompts-as-code, and it only holds if the fleet actually carries them.

The definition format is the repository's own, not the CLI's agent configuration file. The
CLI's schema cannot be validated without a credential, since `kiro-cli agent validate`
requires authentication, and shipping configuration whose correctness cannot be checked at
build time means finding a typo on a task rather than in a build.
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

logger = logging.getLogger(__name__)

DEFINITION_FILE: Final = "workflow.json"
MCP_FILE: Final = "mcp.json"
STEERING_DIR: Final = "steering"

# Where the CLI reads MCP configuration from, relative to HOME. Confirmed by running
# `kiro-cli mcp add` and observing what it wrote.
CLI_MCP_PATH: Final = Path(".kiro/settings/mcp.json")

# Where the CLI reads steering from, relative to HOME. The CLI names both locations
# itself, in the footer of `/config — steering`: "modify ~/.kiro/steering/ (user-level) or
# .kiro/steering/ (workspace-level)".
#
# User-level is the right one here. Workspace-level would put the steering inside the
# directory the workflow writes to, where it is both visible to the agent as an ordinary
# file and caught by the output guard's snapshot. Under HOME it belongs to the image
# instead, which is the same reason MCP configuration is seeded there.
CLI_STEERING_PATH: Final = Path(".kiro/steering")


class InvalidWorkflow(RuntimeError):
    """A workflow definition is missing or cannot be used."""


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    description: str
    prompt: str
    trust_tools: tuple[str, ...]
    requires_mcp_servers: tuple[str, ...]
    expects_stdout: bool
    mcp_config: dict[str, Any] | None
    steering_files: tuple[Path, ...]
    source: Path

    @property
    def trust_tools_argument(self) -> str:
        return ",".join(self.trust_tools)

    @classmethod
    def load(cls, directory: Path | str) -> Self:
        directory = Path(directory)
        manifest_path = directory / DEFINITION_FILE
        if not manifest_path.is_file():
            raise InvalidWorkflow(f"no {DEFINITION_FILE} in {directory}")

        try:
            manifest = json.loads(manifest_path.read_text())
        except json.JSONDecodeError as exc:
            raise InvalidWorkflow(f"{manifest_path} is not valid JSON: {exc}") from exc

        for field in ("name", "promptFile", "trustTools"):
            if field not in manifest:
                raise InvalidWorkflow(f"{manifest_path} has no {field!r}")

        prompt_path = directory / manifest["promptFile"]
        if not prompt_path.is_file():
            raise InvalidWorkflow(
                f"{manifest_path} names promptFile {manifest['promptFile']!r}, "
                "which is not in this directory"
            )
        prompt = prompt_path.read_text().strip()
        if not prompt:
            raise InvalidWorkflow(f"{prompt_path} is empty")

        required_servers = tuple(manifest.get("requiresMcpServers", ()))
        mcp_config = cls._load_mcp(directory, required_servers)
        steering_files = cls._load_steering(directory, bool(manifest.get("steering", False)))

        return cls(
            name=manifest["name"],
            description=manifest.get("description", ""),
            prompt=prompt,
            trust_tools=tuple(manifest["trustTools"]),
            requires_mcp_servers=required_servers,
            expects_stdout=bool(manifest.get("expectsStdout", True)),
            mcp_config=mcp_config,
            steering_files=steering_files,
            source=directory,
        )

    @staticmethod
    def _load_steering(directory: Path, declared: bool) -> tuple[Path, ...]:
        """The workflow's steering files, if it declared any.

        Declared in the manifest rather than inferred from the directory existing, so a
        misspelled directory name is an error instead of a workflow that silently runs
        without the instructions it was written against. That failure would be invisible:
        the run still produces output, just not to the conventions the steering carries.
        """
        steering_dir = directory / STEERING_DIR
        if not declared:
            if steering_dir.is_dir():
                raise InvalidWorkflow(
                    f"{directory} has a {STEERING_DIR}/ directory but workflow.json does "
                    'not set "steering": true, so it would be ignored'
                )
            return ()

        if not steering_dir.is_dir():
            raise InvalidWorkflow(
                f'{directory} sets "steering": true but has no {STEERING_DIR}/ directory'
            )

        files = tuple(sorted(steering_dir.glob("*.md")))
        if not files:
            raise InvalidWorkflow(f"{steering_dir} contains no .md files")
        return files

    @staticmethod
    def _load_mcp(
        directory: Path, required: tuple[str, ...]
    ) -> dict[str, Any] | None:
        mcp_path = directory / MCP_FILE
        if not mcp_path.is_file():
            if required:
                raise InvalidWorkflow(
                    f"requiresMcpServers names {list(required)} but there is no "
                    f"{MCP_FILE} in {directory}"
                )
            return None

        try:
            config = json.loads(mcp_path.read_text())
        except json.JSONDecodeError as exc:
            raise InvalidWorkflow(f"{mcp_path} is not valid JSON: {exc}") from exc

        configured = set((config.get("mcpServers") or {}).keys())
        # Checked here rather than left to the CLI, because the CLI's failure for a
        # missing server is the same exit code as its failure for anything else about
        # MCP, and that ambiguity is worth removing where it can be.
        missing = [name for name in required if name not in configured]
        if missing:
            raise InvalidWorkflow(
                f"requiresMcpServers names {missing}, which {mcp_path} does not configure"
            )
        return config


def seed_cli_configuration(home: Path, definition: WorkflowDefinition) -> None:
    """Place the workflow's MCP configuration and steering where the CLI will find them.

    The CLI reads both from under `HOME`. In a container `HOME` is a directory the image
    owns, so seeding it from the definition means a workflow behaves the same on every
    task. Nothing outside the image can change it, which is the property a local run
    cannot have: there `HOME` belongs to the developer.

    Both are replaced rather than merged. A task that inherited half its instructions from
    whatever was on the filesystem would produce results nobody could attribute to a
    prompt, which defeats carrying the prompt in the image at all.
    """
    _seed_mcp(home, definition)
    _seed_steering(home, definition)


def _seed_mcp(home: Path, definition: WorkflowDefinition) -> None:
    if definition.mcp_config is None:
        logger.info("workflow %s requires no MCP servers", definition.name)
        return

    target = home / CLI_MCP_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(definition.mcp_config, indent=2) + "\n")
    logger.info(
        "seeded %d MCP server(s) for %s at %s",
        len(definition.mcp_config.get("mcpServers", {})),
        definition.name,
        target,
    )


def _seed_steering(home: Path, definition: WorkflowDefinition) -> None:
    """Copy the workflow's steering to the user-level directory the CLI reads.

    Cleared first, so the set of instructions in effect is exactly the set the workflow
    ships. Leaving a stale file behind would mean a run governed partly by a previous
    workflow's conventions, and the output would look plausible either way.
    """
    target_dir = home / CLI_STEERING_PATH
    if target_dir.exists():
        shutil.rmtree(target_dir)

    if not definition.steering_files:
        logger.info("workflow %s carries no steering", definition.name)
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    for path in definition.steering_files:
        shutil.copy2(path, target_dir / path.name)

    logger.info(
        "seeded %d steering file(s) for %s at %s",
        len(definition.steering_files),
        definition.name,
        target_dir,
    )


def stage_workflow(definition: WorkflowDefinition, workspace: Path) -> None:
    """Copy the workflow's own files into the workspace, read-only in spirit.

    Some workflows reference files next to their prompt. Copying rather than pointing at
    the image path keeps every path the workflow uses relative to the workspace, which is
    the whole point of the contract.
    """
    destination = workspace / ".workflow"
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(definition.source, destination)
