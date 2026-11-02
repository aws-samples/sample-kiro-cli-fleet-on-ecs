"""M1b's pure core: the corpus, the prompt, and what a run's stderr says."""

from fleetbench import fanout


def test_corpus_has_one_group_per_subagent_and_no_throttle_words():
    files = fanout.corpus_files(subagents=3, files_per_subagent=4)
    assert sorted({p.split("/")[1][0] for p in files}) == ["a", "b", "c"]
    assert len(files) == 12
    # A note the model reads must never look like a throttle message on stderr.
    for text in files.values():
        assert fanout.analyse_stderr(text).throttle_hits == 0


def test_corpus_is_deterministic():
    assert fanout.corpus_files(2, 3) == fanout.corpus_files(2, 3)


def test_note_words_scales_note_size_and_keeps_the_fact():
    small = fanout.corpus_files(1, 1)["notes/a-01.md"]
    large = fanout.corpus_files(1, 1, note_words=4000)["notes/a-01.md"]
    assert len(large.split()) > 30 * len(small.split())
    assert "Fact: the count for this note is" in large
    assert not fanout._THROTTLE.search(large)


def test_fact_is_the_last_line_so_a_partial_read_misses_it():
    note = fanout.corpus_files(1, 1, note_words=5000)["notes/a-01.md"]
    assert note.rstrip("\n").splitlines()[-1].startswith("Fact: the count for this note is")
    assert "Fact:" not in "\n".join(note.splitlines()[:5])
    assert fanout.note_lines(5000) == note.count("\n")


def test_read_coverage_counts_ranged_reads_that_stop_early_as_partial():
    total = 20
    stderr = (
        "\x1b[1m[tool] Reading a-01.md:1-5\x1b[0m\n"
        "[tool] Reading a-02.md:1-20\n"
        "Reading a-03.md\n"
        "[tool] Reading a-04.md:1\n"
        "[tool] Reading summary-a.md:1, summary-b.md:1\n"
        "[tool] status: Completed\n"
    )
    lines = fanout.read_lines(stderr)
    assert len(lines) == 5
    assert fanout.read_coverage(lines, total) == (4, 3)


def test_prompt_asks_for_whole_file_reads():
    assert "read each file in full" in fanout.prompt(2, 3)


def test_prompt_assigns_every_group_and_asks_for_one_file_per_call():
    text = fanout.prompt(subagents=2, files_per_subagent=6)
    assert "notes/a-01.md through notes/a-06.md" in text
    assert "output/summary-b.md" in text
    assert "one file per read tool call" in text


def test_stderr_counts_recovered_throttles_tool_uses_and_approval_blocks():
    stderr = "\n".join(
        [
            "Reading file: notes/a-01.md (using tool: read)",
            "\x1b[31mThrottlingException: Request quota exceeded\x1b[0m, retrying",
            "Reading file: notes/a-02.md (using tool: read)",
            "Kiro rate limit reached",
            "Tool approval required but --no-interactive was specified",
        ]
    )
    f = fanout.analyse_stderr(stderr)
    assert f.throttle_hits == 2
    assert f.tool_uses == 2
    assert f.approval_blocks == 1
    assert f.throttle_lines[0].startswith("ThrottlingException")


def test_stderr_counts_the_tool_lines_kiro_cli_2_30_prints():
    stderr = "\n".join(
        [
            "[tool] Spawning agent crew",
            "[denied] tool permission approval is not supported in non-interactive mode.",
            "[tool] status: Failed",
            "[tool] Reading listing .",
            "[tool] status: Completed",
        ]
    )
    f = fanout.analyse_stderr(stderr)
    assert f.tool_uses == 2
    assert f.approval_blocks == 1
    assert f.throttle_hits == 0


def test_stderr_masks_long_tokens_in_kept_lines():
    f = fanout.analyse_stderr("throttled for key " + "x" * 40)
    assert "<REDACTED>" in f.throttle_lines[0]


def _rec(sent, ended, exit_code=0, hits=0, outputs=3, tools=20):
    return {
        "sent_at": sent,
        "ended_at": ended,
        "exit_code": exit_code,
        "throttle_hits": hits,
        "outputs": outputs,
        "tool_uses": tools,
        "approval_blocks": 0,
    }


def test_level_summary_separates_recovered_from_failed_throttles():
    records = [
        _rec(0, 60),
        _rec(0, 120, hits=3),  # retried, still finished with output: recovered
        _rec(0, 90, exit_code=1, hits=5, outputs=0),  # gave up
    ]
    f = fanout.summarise_level(3, records)
    assert (f.records, f.finished, f.throttled_runs, f.recovered_runs) == (3, 2, 2, 1)
    assert f.throttle_hits == 8
    assert f.median_seconds == 90
    assert f.tool_uses_per_minute == 30  # 60 tool uses over a 120 s window


def test_markdown_has_a_row_per_level():
    levels = [fanout.summarise_level(n, [_rec(0, 60)]) for n in (1, 2)]
    text = fanout.fanout_markdown("fanout-s2", 2, levels)
    assert "| 1 |" in text and "| 2 |" in text


def test_subagent_agent_is_seeded_and_named_in_prompt(tmp_path):
    import json as _json

    fanout.write_corpus(tmp_path, 2, 2, subagent_agent="note-reader")
    config = _json.loads((tmp_path / ".kiro/agents/note-reader.json").read_text())
    assert config["allowedTools"] == ["fs_read", "fs_write"]
    assert "agent named note-reader" in fanout.prompt(2, 2, "note-reader")
    assert "agent named" not in fanout.prompt(2, 2)


def test_final_total_check(tmp_path):
    assert fanout.expected_total(2, 15) == 4575
    assert fanout.final_total_ok(tmp_path, 2, 15) is None
    (tmp_path / "output").mkdir()
    (tmp_path / "output/final.md").write_text("**Total of all counts: 4,575**\n")
    assert fanout.final_total_ok(tmp_path, 2, 15) is True
    (tmp_path / "output/final.md").write_text("Total: 4570\n")
    assert fanout.final_total_ok(tmp_path, 2, 15) is False


def test_tool_lines_on_stdout_are_counted():
    log = "I will read (using tool: read)\nCreating: /w/output/final.md\n"
    assert fanout.analyse_stderr(log).tool_uses == 1
