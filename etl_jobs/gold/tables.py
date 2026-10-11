from src.common.layer_build import LayerTable

RECONCILIATION_WINDOW_DAYS = 7

RESULTS = ("reconciliation_results", "reconciliation_runs")
TRANSACTIONS = ("transactions", "transactions_history")

GOLD_TABLES = (
    LayerTable("dim_date", ("transactions", "reconciliation_runs", "settlements")),
    LayerTable("dim_category", ()),
    LayerTable("dim_merchant", ("merchants",)),
    LayerTable("fact_reconciliation_run", RESULTS, "reference_date"),
    LayerTable(
        "fact_reconciliation_result",
        (*RESULTS, "transactions", "settlements", "merchants"),
        "reference_date",
    ),
    LayerTable(
        "fact_transaction_event",
        ("transactions_history", "reconciliation_results"),
        "created_date",
        RECONCILIATION_WINDOW_DAYS,
    ),
    LayerTable(
        "fact_transaction",
        (*TRANSACTIONS, *RESULTS, "merchants"),
        "created_date",
        RECONCILIATION_WINDOW_DAYS,
    ),
    LayerTable("fact_settlement", ("settlements", "transactions", "merchants"), "reference_date"),
    LayerTable(
        "audit_reconciliation_recheck",
        (*RESULTS, "transactions", "settlements"),
        "reference_date",
    ),
    LayerTable("agg_reconciliation_daily", RESULTS),
    LayerTable("agg_amount_at_risk_daily", (*RESULTS, "transactions", "settlements")),
    LayerTable("agg_reconciliation_merchant", (*RESULTS, "merchants")),
    LayerTable("agg_volume_daily", ("transactions",)),
    LayerTable("agg_merchant_volume_daily", ("transactions",)),
    LayerTable("agg_settlement_daily", ("settlements", "transactions")),
)
