import src.cli as cli_module


def test_paper_cycle_summary_table_has_stable_structure() -> None:
    summary = cli_module.PaperCycleSummary(
        execution_mode="paper",
        risk_profile="strict",
        max_signals=20,
        timeout_seconds=60.0,
        signals_collected=20,
        signals_accepted=2,
        signals_rejected=13,
        trades_persisted=2,
        open_positions=1,
        sources_polled=["pump_fun", "whale_tracker", "twitter"],
        source_signal_counts={"pump_fun": 10, "twitter": 2, "whale_tracker": 3},
        source_failures={},
        composite_opportunities=0,
        rejection_reasons={"top10_holder_check_failed": 5},
        candidates_evaluated=15,
        passed_risk_checks=2,
        summary_rejection_reasons={
            "liquidity_unknown": 4,
            "top10_holder": 5,
            "mint_authority": 2,
            "age": 1,
            "honeypot": 1,
        },
        source_evaluated_counts={"pump_fun": 10, "twitter": 2, "whale_tracker": 3},
        source_pass_counts={"pump_fun": 1, "twitter": 0, "whale_tracker": 1},
        holder_lookup_outcomes={},
        termination_reason="max_signals",
        elapsed_seconds=12.345,
    )

    lines = summary.summary_table_lines()

    assert lines[0] == "═══ Paper Cycle Summary ═══"
    assert "Signals collected:     20" in lines
    assert "Candidates evaluated:  15" in lines
    assert "Passed risk checks:    2" in lines
    assert "Rejected:              13" in lines
    assert "  - liquidity_unknown: 4" in lines
    assert "  - top10_holder: 5" in lines
    assert "By source:" in lines
    assert "  - pump_fun: 10 (1 passed)" in lines
    assert "  - whale_tracker: 3 (1 passed)" in lines
    assert "  - twitter: 2 (0 passed)" in lines
    assert lines[-2] == "Paper trades executed: 2"
    assert lines[-1] == "═══════════════════════════"
