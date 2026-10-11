from src.common.layer_build import LayerTable

RECONCILIATION_WINDOW_DAYS = 7

SILVER_TABLES = (
    LayerTable(
        "transactions_history",
        ("transactions",),
        "created_date",
        RECONCILIATION_WINDOW_DAYS,
        dedup_source="transactions",
    ),
    LayerTable("transactions", ("transactions",), "created_date", RECONCILIATION_WINDOW_DAYS),
    LayerTable(
        "settlements",
        ("settlement_paysettler",),
        "reference_date",
        dedup_source="settlement_paysettler",
    ),
    LayerTable("reconciliation_runs", ("reconciliation_runs",), "reference_date"),
    LayerTable(
        "reconciliation_results_deduped",
        ("reconciliation_results",),
        publish=False,
        dedup_source="reconciliation_results",
    ),
    LayerTable(
        "reconciliation_results",
        ("reconciliation_results", "reconciliation_runs"),
        "reference_date",
    ),
    LayerTable(
        "reconciliation_results_quarantine",
        ("reconciliation_results", "reconciliation_runs"),
    ),
    LayerTable("merchants", ("enterprise_company",)),
)
