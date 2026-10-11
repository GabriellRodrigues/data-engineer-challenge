CREATE OR REPLACE MACRO safe_rate(numerator, denominator) AS
    CASE WHEN denominator > 0 THEN numerator::DOUBLE / denominator END;

CREATE OR REPLACE MACRO valid_at(moment, valid_from, valid_to) AS
    moment >= valid_from AND (valid_to IS NULL OR moment < valid_to);

CREATE OR REPLACE MACRO mask_cnpj(document) AS
    CASE
        WHEN document IS NULL THEN NULL
        WHEN length(document) = 14 THEN
            substr(document, 1, 2) || '.' || substr(document, 3, 3) || '.' || substr(document, 6, 3) || '/****-**'
        ELSE repeat('*', length(document))
    END;

CREATE OR REPLACE MACRO op_rank(op) AS CASE op WHEN 'D' THEN 3 WHEN 'U' THEN 2 ELSE 1 END;

CREATE OR REPLACE MACRO money(value) AS CAST(value AS DECIMAL(18, 2));
