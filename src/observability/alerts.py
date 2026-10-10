import operator
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.compute as pc

from src.observability.run_registry import RunContext
from src.observability.store import PIPELINE_ALERTS, PIPELINE_METRICS, append, read
from src.utils.logger import get_logger
from src.utils.yaml_reader import read_yaml

logger = get_logger("observability.alerts")

THRESHOLD = "threshold"
DEVIATION = "deviation"
SEVERITIES = {"warn", "error"}
OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}
BREACH_STATUS = {"warn": "warn", "error": "fail"}
PASS = "pass"


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    metric: str
    severity: str
    layer: str | None = None
    table_name: str | None = None
    operator: str | None = None
    value: float | None = None
    max_ratio: float | None = None
    window: int = 7
    min_history: int = 3


def load_rules(path: Path) -> list[Rule]:
    rules = [Rule(**raw) for raw in read_yaml(path).get("rules") or []]
    for rule in rules:
        if rule.type not in (THRESHOLD, DEVIATION):
            raise ValueError(f"Rule '{rule.name}': unknown type '{rule.type}'")
        if rule.severity not in SEVERITIES:
            raise ValueError(f"Rule '{rule.name}': severity must be one of {sorted(SEVERITIES)}")
        if rule.type == THRESHOLD and (rule.operator not in OPERATORS or rule.value is None):
            raise ValueError(f"Rule '{rule.name}': threshold needs an operator in {sorted(OPERATORS)} and a value")
        if rule.type == DEVIATION and rule.max_ratio is None:
            raise ValueError(f"Rule '{rule.name}': deviation needs max_ratio")
    return rules


def _previous_metrics(run: RunContext) -> list[dict]:
    history = read(run.base_dir, PIPELINE_METRICS)
    history = history.filter(pc.not_equal(history["run_id"], run.run_id))
    history = history.sort_by([("computed_at", "descending")])
    return history.select(["layer", "table_name", "metric", "value"]).to_pylist()


def _history(previous: list[dict], metric: dict, window: int) -> list[float]:
    values = [
        row["value"] for row in previous
        if row["layer"] == metric["layer"]
        and row["table_name"] == metric["table_name"]
        and row["metric"] == metric["metric"]
    ]
    return values[:window]


def _evaluate_threshold(rule: Rule, value: float) -> tuple[bool, float, str]:
    breached = OPERATORS[rule.operator](value, rule.value)
    message = f"{rule.metric} = {value:g} ({'breaches' if breached else 'within'} {rule.operator} {rule.value:g})"
    return breached, rule.value, message


def _evaluate_deviation(rule: Rule, value: float, history: list[float]) -> tuple[bool, float | None, str]:
    if len(history) < rule.min_history:
        return False, None, f"insufficient history ({len(history)} of {rule.min_history} runs)"
    median = statistics.median(history)
    if median == 0:
        ratio = 0.0 if value == 0 else float("inf")
    else:
        ratio = abs(value - median) / abs(median)
    breached = ratio > rule.max_ratio
    message = (
        f"{rule.metric} = {value:g} vs median {median:g} of last {len(history)} runs "
        f"({ratio:.0%} deviation, limit {rule.max_ratio:.0%})"
    )
    return breached, median, message


def evaluate_alerts(run: RunContext, rules: list[Rule]) -> bool:
    previous = []
    if any(rule.type == DEVIATION for rule in rules):
        previous = _previous_metrics(run)

    alerts = []
    for rule in rules:
        for metric in run.metrics:
            if metric["metric"] != rule.metric:
                continue
            if rule.layer and metric["layer"] != rule.layer:
                continue
            if rule.table_name and metric["table_name"] != rule.table_name:
                continue
            value = metric["value"]
            if rule.type == THRESHOLD:
                breached, expected, message = _evaluate_threshold(rule, value)
            else:
                history = _history(previous, metric, rule.window)
                breached, expected, message = _evaluate_deviation(rule, value, history)

            status = BREACH_STATUS[rule.severity] if breached else PASS
            alerts.append({
                "run_id": run.run_id,
                "rule": rule.name,
                "layer": metric["layer"],
                "table_name": metric["table_name"],
                "source_file": metric["source_file"],
                "metric": rule.metric,
                "value": value,
                "expected": expected,
                "severity": rule.severity,
                "status": status,
                "message": message,
                "evaluated_at": datetime.now(UTC),
            })

            target = metric["table_name"] + (f" ({metric['source_file']})" if metric["source_file"] else "")
            if status == "fail":
                logger.error("Alert %s failed on %s: %s", rule.name, target, message)
            elif status == "warn":
                logger.warning("Alert %s warned on %s: %s", rule.name, target, message)

    append(run.base_dir, PIPELINE_ALERTS, alerts)
    return any(alert["status"] == "fail" for alert in alerts)
