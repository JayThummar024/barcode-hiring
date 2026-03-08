-- +goose Up

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Extend the product table with an updated_at timestamp.
--    Useful for auditing which record was last touched and when.
-- ─────────────────────────────────────────────────────────────────────────────
-- +goose StatementBegin
ALTER TABLE dirac.product
    ADD COLUMN updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
-- +goose StatementEnd


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Barcode table.
--
--    Design decisions:
--      • Separate table (not a column on product) so a product can later have
--        multiple barcodes without schema changes.
--      • raw_barcode  : exactly what was ingested — preserved for audit/debug.
--      • normalized_barcode : canonical 14-digit GTIN-14 form — this is the
--        deduplication key; UNIQUE constraint enforces one row per product.
--      • barcode_type : detected from original digit-length before padding
--        (EAN-8 / UPC-A / EAN-13 / ISBN-13 / GTIN-14).
--      • ON DELETE CASCADE : if a product is ever hard-deleted, its barcodes
--        go with it — no orphaned rows.
-- ─────────────────────────────────────────────────────────────────────────────
-- +goose StatementBegin
CREATE TABLE dirac.barcode (
    id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id         BIGINT      NOT NULL REFERENCES dirac.product(id) ON DELETE CASCADE,
    raw_barcode        TEXT        NOT NULL,
    normalized_barcode CHAR(14)    NOT NULL,
    barcode_type       TEXT        NOT NULL,
    created_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (normalized_barcode)
);
-- +goose StatementEnd

-- Index so lookups by product are fast (e.g. "give me all barcodes for product X")
-- +goose StatementBegin
CREATE INDEX idx_barcode_product_id ON dirac.barcode (product_id);
-- +goose StatementEnd


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. normalize_barcode(p_raw TEXT)
--
--    Converts any supported barcode string into its canonical 14-digit GTIN-14
--    representation and detects its type.
--
--    Algorithm:
--      a) Strip all non-digit characters (handles dashes, spaces from manual entry).
--      b) Validate length — we support 8, 12, 13, 14 digits.
--         UPC-E (6 digits) excluded: expansion logic is complex and rare in
--         price-list / scraping contexts; document and revisit if needed.
--      c) Detect barcode type from original length.
--      d) Left-pad with zeros to 14 digits.
--         GS1 guarantees check-digit validity is preserved under left-zero-padding,
--         so UPC-A "012345678905" and EAN-13 "0012345678905" both become
--         "00012345678905" with the same valid check digit.
--      e) Validate the check digit using the GS1 GTIN-14 algorithm:
--           for positions i = 1..13 (left to right):
--             weight = 3 if i is odd, 1 if i is even
--           check_digit = (10 - (sum % 10)) % 10
--
--    Raises an exception for invalid length or bad check digit — bad data is
--    rejected at the boundary before it can corrupt the product table.
--
--    Marked IMMUTABLE: same input always produces same output (or same error),
--    allowing Postgres to inline/cache calls in query plans.
-- ─────────────────────────────────────────────────────────────────────────────
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION dirac.normalize_barcode(p_raw TEXT)
RETURNS TABLE (normalized CHAR(14), barcode_type TEXT)
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_digits TEXT;
    v_len    INT;
    v_padded CHAR(14);
    v_sum    INT := 0;
    v_i      INT;
    v_digit  INT;
    v_weight INT;
    v_check  INT;
    v_type   TEXT;
BEGIN
    -- a) Strip non-digit characters
    v_digits := regexp_replace(p_raw, '[^0-9]', '', 'g');
    v_len    := length(v_digits);

    -- b) Validate length
    IF v_len NOT IN (8, 12, 13, 14) THEN
        RAISE EXCEPTION
            'Unsupported barcode length % for input "%". Expected 8 (EAN-8), 12 (UPC-A), 13 (EAN-13/ISBN-13) or 14 (GTIN-14) digits.',
            v_len, p_raw;
    END IF;

    -- c) Detect type from original digit count (before padding)
    v_type := CASE v_len
        WHEN  8 THEN 'EAN-8'
        WHEN 12 THEN 'UPC-A'
        WHEN 13 THEN
            CASE WHEN LEFT(v_digits, 3) IN ('978', '979') THEN 'ISBN-13' ELSE 'EAN-13' END
        WHEN 14 THEN 'GTIN-14'
    END;

    -- d) Pad to 14 digits
    v_padded := LPAD(v_digits, 14, '0');

    -- e) Validate check digit (GS1 GTIN-14 algorithm)
    FOR v_i IN 1..13 LOOP
        v_digit  := CAST(SUBSTR(v_padded, v_i, 1) AS INT);
        v_weight := CASE WHEN v_i % 2 = 1 THEN 3 ELSE 1 END;
        v_sum    := v_sum + v_digit * v_weight;
    END LOOP;

    v_check := (10 - (v_sum % 10)) % 10;

    IF v_check <> CAST(SUBSTR(v_padded, 14, 1) AS INT) THEN
        RAISE EXCEPTION
            'Invalid check digit for barcode "%": computed %, got %.',
            p_raw, v_check, SUBSTR(v_padded, 14, 1);
    END IF;

    normalized   := v_padded;
    barcode_type := v_type;
    RETURN NEXT;
END;
$$;
-- +goose StatementEnd


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. upsert_product(p_name, p_description, p_barcode)
--
--    The single public entry point for all product ingestion.
--    Returns the product id (existing or newly created).
--
--    Flow:
--      1. Normalise and validate the barcode — bad input raises immediately.
--      2. Look up the normalised barcode in dirac.barcode.
--         • FOUND   → duplicate: return the existing product_id unchanged.
--                     We deliberately do NOT overwrite name/description.
--                     Rationale: sources name products inconsistently;
--                     first-seen data is kept as canonical to avoid flickering.
--         • NOT FOUND → new product: insert product row, then barcode row.
--
--    Concurrency note:
--      The INSERT on barcode has a UNIQUE constraint, so a concurrent race
--      between two identical barcodes will cause one to fail with a unique
--      violation. For the current scale (batch ingestion, not high-frequency
--      concurrent writes) this is acceptable. If needed, wrap the caller in
--      ON CONFLICT DO NOTHING or use advisory locks.
-- ─────────────────────────────────────────────────────────────────────────────
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION dirac.upsert_product(
    p_name        VARCHAR(255),
    p_description TEXT,
    p_barcode     TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_normalized CHAR(14);
    v_type       TEXT;
    v_product_id BIGINT;
BEGIN
    -- 1. Normalise (raises on invalid barcode)
    SELECT n.normalized, n.barcode_type
      INTO v_normalized, v_type
      FROM dirac.normalize_barcode(p_barcode) n;

    -- 2. Deduplicate by normalised barcode
    SELECT b.product_id
      INTO v_product_id
      FROM dirac.barcode b
     WHERE b.normalized_barcode = v_normalized;

    IF FOUND THEN
        RETURN v_product_id;
    END IF;

    -- 3. New product
    INSERT INTO dirac.product (name, description)
    VALUES (p_name, p_description)
    RETURNING id INTO v_product_id;

    INSERT INTO dirac.barcode (product_id, raw_barcode, normalized_barcode, barcode_type)
    VALUES (v_product_id, p_barcode, v_normalized, v_type);

    RETURN v_product_id;
END;
$$;
-- +goose StatementEnd


-- ─────────────────────────────────────────────────────────────────────────────
-- Down migration — tears everything down in reverse dependency order
-- ─────────────────────────────────────────────────────────────────────────────
-- +goose Down

-- +goose StatementBegin
DROP FUNCTION IF EXISTS dirac.upsert_product(VARCHAR, TEXT, TEXT);
-- +goose StatementEnd

-- +goose StatementBegin
DROP FUNCTION IF EXISTS dirac.normalize_barcode(TEXT);
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dirac.barcode;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dirac.product DROP COLUMN IF EXISTS updated_at;
-- +goose StatementEnd
