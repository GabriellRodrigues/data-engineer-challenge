import pytest

from etl_jobs.gold.tables import GOLD_TABLES
from etl_jobs.observability import lineage as lineage_job
from etl_jobs.silver.tables import SILVER_TABLES


@pytest.fixture(scope="module")
def lineage():
    return lineage_job.build_lineage()


def test_every_table_of_every_layer_is_a_node(lineage):
    layers = {"silver": SILVER_TABLES, "gold": GOLD_TABLES}
    for layer, tables in layers.items():
        assert {f"{layer}.{t.name}" for t in tables} <= lineage.nodes.keys()
    assert "bronze.settlement_paysettler" in lineage.nodes


@pytest.mark.parametrize("layer, tables", [("silver", SILVER_TABLES), ("gold", GOLD_TABLES)])
def test_sql_dependencies_agree_with_the_declared_inputs(lineage, layer, tables):
    previous = {"silver": "bronze", "gold": "silver"}[layer]
    for table in tables:
        direct = {
            upstream.split(".", 1)[1]
            for upstream, downstream in lineage.edges
            if downstream == f"{layer}.{table.name}" and upstream.startswith(f"{previous}.")
        }
        assert direct <= set(table.inputs), table.name


def test_dependencies_inside_a_layer_are_found(lineage):
    assert ("gold.fact_transaction", "gold.agg_volume_daily") in lineage.edges
    assert ("silver.reconciliation_results_deduped", "silver.reconciliation_results") in lineage.edges
    assert ("bronze.settlement_paysettler", "silver.settlements") in lineage.edges


def test_graph_has_no_cycles(lineage):
    for node_id in lineage.nodes:
        assert node_id not in lineage.downstream(node_id)


def test_critical_tables_are_declared_in_gold_contracts(lineage):
    assert lineage.critical() == [
        "gold.agg_amount_at_risk_daily",
        "gold.agg_reconciliation_daily",
        "gold.agg_volume_daily",
        "gold.fact_reconciliation_result",
        "gold.fact_reconciliation_run",
        "gold.fact_transaction",
        "gold.fact_transaction_event",
    ]
    assert "bronze.settlement_paysettler" in lineage.feeds_critical()
    assert "gold.agg_settlement_daily" not in lineage.feeds_critical()


def test_committed_lineage_doc_is_up_to_date(lineage):
    assert lineage_job.LINEAGE_DOC.read_text(encoding="utf-8") == lineage_job.render(lineage), (
        "docs/pipeline_docs/lineage.md is stale: run make lineage"
    )


def test_main_writes_the_document(tmp_path):
    output = tmp_path / "lineage.md"

    lineage_job.main(["--output", str(output)])

    assert "```mermaid" in output.read_text(encoding="utf-8")


def test_impact_lists_affected_tables(caplog):
    with caplog.at_level("INFO"):
        lineage_job.main(["--impact", "silver.settlements"])

    assert "gold.fact_settlement" in caplog.text
    assert "Critical tables affected: gold.agg_amount_at_risk_daily" in caplog.text


def test_impact_of_unknown_table_exits(caplog):
    with pytest.raises(SystemExit) as exit_info:
        lineage_job.main(["--impact", "settlements"])

    assert exit_info.value.code == 1
