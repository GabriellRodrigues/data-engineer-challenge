import pytest

from src.observability.alerts import Rule, evaluate_alerts, load_rules
from src.observability.run_registry import pipeline_run
from src.observability.store import PIPELINE_ALERTS, read
from src.utils.paths import ALERT_RULES_FILE

REFUSED_ERROR = Rule(name="refused_high", type="threshold", metric="refused_rate",
                     operator=">", value=0.10, severity="error")
REFUSED_WARN = Rule(name="refused_elevated", type="threshold", metric="refused_rate",
                    operator=">", value=0.01, severity="warn")
VOLUME = Rule(name="volume", type="deviation", metric="rows_read", max_ratio=0.5,
              window=7, min_history=3, severity="warn")


def alerts(base_dir) -> list[dict]:
    table = read(base_dir, PIPELINE_ALERTS).sort_by("evaluated_at")
    return table.select(["rule", "status", "expected"]).to_pylist()


def run_with(base_dir, metric: str, value: float, rules: list[Rule]) -> bool:
    with pipeline_run("test.job", "ingest", base_dir) as run:
        run.metric("bronze", "orders", metric, value)
        return evaluate_alerts(run, rules)


def test_threshold_rules_pass_warn_and_fail(tmp_path):
    assert not run_with(tmp_path / "ok", "refused_rate", 0.0, [REFUSED_ERROR, REFUSED_WARN])
    assert not run_with(tmp_path / "warn", "refused_rate", 0.05, [REFUSED_ERROR, REFUSED_WARN])
    assert run_with(tmp_path / "fail", "refused_rate", 0.5, [REFUSED_ERROR, REFUSED_WARN])

    assert [a["status"] for a in alerts(tmp_path / "ok")] == ["pass", "pass"]
    assert [a["status"] for a in alerts(tmp_path / "warn")] == ["pass", "warn"]
    assert [a["status"] for a in alerts(tmp_path / "fail")] == ["fail", "warn"]


def test_deviation_waits_for_minimum_history(tmp_path):
    for value in (100, 100):
        run_with(tmp_path, "rows_read", value, [VOLUME])
    run_with(tmp_path, "rows_read", 1000, [VOLUME])

    assert [a["status"] for a in alerts(tmp_path)] == ["pass", "pass", "pass"]


def test_deviation_warns_against_median_of_history(tmp_path):
    for value in (100, 110, 90):
        run_with(tmp_path, "rows_read", value, [VOLUME])
    blocking = run_with(tmp_path, "rows_read", 10, [VOLUME])

    last = alerts(tmp_path)[-1]
    assert not blocking
    assert last == {"rule": "volume", "status": "warn", "expected": 100.0}


def test_rules_only_apply_to_their_metric_and_layer(tmp_path):
    rule = Rule(name="silver_only", type="threshold", metric="refused_rate", operator=">",
                value=0.0, severity="error", layer="silver")
    assert not run_with(tmp_path, "refused_rate", 1.0, [rule])
    assert alerts(tmp_path) == []


def test_project_rules_file_is_valid():
    assert {rule.name for rule in load_rules(ALERT_RULES_FILE)} >= {"refused_rate_high", "rows_read_deviation"}


def test_invalid_rule_is_rejected(tmp_path):
    rules_file = tmp_path / "rules.yml"
    rules_file.write_text(
        "rules:\n  - {name: bad, type: threshold, metric: m, operator: '~', value: 1, severity: warn}\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="threshold needs an operator"):
        load_rules(rules_file)


def test_rules_can_target_a_single_table(tmp_path):
    rule = Rule(name="settlement_overdue", type="threshold", metric="arrival_lag_hours",
                operator=">", value=26, severity="error", table_name="settlement")
    with pipeline_run("test.job", "ingest", tmp_path) as run:
        run.metric("bronze", "settlement", "arrival_lag_hours", 30)
        run.metric("bronze", "enterprise_company", "arrival_lag_hours", 500)
        blocking = evaluate_alerts(run, [rule])

    assert blocking
    table = read(tmp_path, PIPELINE_ALERTS).select(["table_name", "status"])
    assert table.to_pylist() == [{"table_name": "settlement", "status": "fail"}]
