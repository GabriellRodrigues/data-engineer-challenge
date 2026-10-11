import pytest

from src.lineage.graph import Lineage, Node, is_critical, references, to_mermaid


@pytest.fixture
def lineage() -> Lineage:
    graph = Lineage()
    for node in (
        Node("bronze", "orders"),
        Node("silver", "orders_dedup", published=False),
        Node("silver", "orders"),
        Node("gold", "fact_orders", critical=True),
        Node("gold", "agg_orders"),
        Node("bronze", "unused"),
    ):
        graph.add(node)
    graph.link("bronze.orders", "silver.orders_dedup")
    graph.link("silver.orders_dedup", "silver.orders")
    graph.link("silver.orders", "gold.fact_orders")
    graph.link("gold.fact_orders", "gold.agg_orders")
    return graph


def test_references_match_whole_names_with_prefix():
    sql = "SELECT * FROM silver_transactions_history JOIN silver_transactions USING (transaction_id)"

    assert references(sql, ["transactions", "transactions_history", "settlements"], "silver_") == {
        "transactions",
        "transactions_history",
    }
    assert references("SELECT transaction_id FROM t", ["transaction"]) == set()


def test_walks_are_transitive(lineage):
    assert lineage.upstream("gold.agg_orders") == {
        "gold.fact_orders", "silver.orders", "silver.orders_dedup", "bronze.orders"
    }
    assert lineage.downstream("bronze.orders") == {
        "silver.orders_dedup", "silver.orders", "gold.fact_orders", "gold.agg_orders"
    }
    assert lineage.downstream("bronze.unused") == set()


def test_critical_tables_and_what_feeds_them(lineage):
    assert lineage.critical() == ["gold.fact_orders"]
    assert lineage.feeds_critical() == {"bronze.orders", "silver.orders_dedup", "silver.orders"}
    assert lineage.impact("bronze.orders") == ["gold.fact_orders"]
    assert lineage.impact("gold.agg_orders") == []


def test_unknown_nodes_are_rejected(lineage):
    with pytest.raises(KeyError):
        lineage.link("bronze.orders", "gold.missing")
    with pytest.raises(KeyError):
        lineage.downstream("gold.missing")


@pytest.mark.parametrize(
    "contract, expected",
    [
        ({"name": "a"}, False),
        ({"name": "a", "customProperties": None}, False),
        ({"name": "a", "customProperties": [{"property": "owner", "value": "ops"}]}, False),
        ({"name": "a", "customProperties": [{"property": "critical", "value": True}]}, True),
        ({"name": "a", "customProperties": [{"property": "critical", "value": False}]}, False),
    ],
)
def test_criticality_comes_from_the_contract(contract, expected):
    assert is_critical(contract) is expected


def test_criticality_must_be_a_boolean():
    with pytest.raises(ValueError, match="critical"):
        is_critical({"name": "a", "customProperties": [{"property": "critical", "value": "yes"}]})


def test_mermaid_groups_layers_and_styles_nodes(lineage):
    text = to_mermaid(lineage, ["bronze", "silver", "gold"])

    assert text.startswith("flowchart LR")
    assert text.index("subgraph bronze") < text.index("subgraph silver") < text.index("subgraph gold")
    assert 'silver__orders_dedup["orders_dedup (temp)"]' in text
    assert "    silver__orders --> gold__fact_orders" in text
    assert "class gold__fact_orders critical" in text
    assert "class bronze__orders,silver__orders,silver__orders_dedup feeds" in text
    assert "class silver__orders_dedup temp" in text
