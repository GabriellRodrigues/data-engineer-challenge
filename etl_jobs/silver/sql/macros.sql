CREATE OR REPLACE MACRO parse_ts_utc(value) AS
    CASE
        WHEN value IS NULL OR trim(value) = '' THEN NULL
        WHEN regexp_matches(trim(value), '(Z|[+-][0-9]{2}(:?[0-9]{2})?)$') THEN TRY_CAST(trim(value) AS TIMESTAMPTZ)
        ELSE TRY_CAST(TRY_CAST(trim(value) AS TIMESTAMP) AS TIMESTAMPTZ)
    END;

CREATE OR REPLACE MACRO parse_amount(value) AS
    CASE
        WHEN value IS NULL OR trim(value) = '' THEN NULL
        WHEN contains(value, ',') THEN TRY_CAST(
            replace(replace(regexp_replace(trim(value), '^R\$ ?', ''), '.', ''), ',', '.') AS DECIMAL(18, 2)
        )
        ELSE TRY_CAST(trim(value) AS DECIMAL(18, 2))
    END;

CREATE OR REPLACE MACRO to_money(value) AS CAST(round(value, 2) AS DECIMAL(18, 2));

CREATE OR REPLACE MACRO norm_merchant(value) AS nullif(upper(trim(value)), '');

CREATE OR REPLACE MACRO norm_code(value) AS nullif(upper(trim(value)), '');

CREATE OR REPLACE MACRO op_rank(op) AS CASE op WHEN 'D' THEN 3 WHEN 'U' THEN 2 ELSE 1 END;

CREATE OR REPLACE MACRO file_date(source_file) AS
    TRY_STRPTIME(
        replace(nullif(regexp_extract(source_file, '([0-9]{4}-?[0-9]{2}-?[0-9]{2})', 1), ''), '-', ''),
        '%Y%m%d'
    )::DATE;
