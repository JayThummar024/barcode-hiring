-- +goose Up
-- +goose StatementBegin

-- ─────────────────────────────────────────────────────────────────────────────
-- pgTAP in-database test suite for the barcode ingestion system.
--
-- How to run:
--   psql $GOOSE_DBSTRING -c "SELECT * FROM runtests('tests', '^test_');"
--
-- Or with pg_prove (if installed):
--   pg_prove -d $GOOSE_DBSTRING
--
-- Each test function runs inside a savepoint that pgTAP rolls back automatically,
-- so test data never leaks into the real tables.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS pgtap;

CREATE SCHEMA IF NOT EXISTS tests;

-- ─────────────────────────────────────────────────────────────────────────────
-- normalize_barcode tests
-- ─────────────────────────────────────────────────────────────────────────────

-- Valid EAN-13 → padded with one leading zero to reach 14 digits
CREATE OR REPLACE FUNCTION tests.test_01_normalize_ean13()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM dirac.normalize_barcode('4006381333931');
    RETURN NEXT is(r.normalized,    '04006381333931',   'EAN-13: normalized to 14 digits');
    RETURN NEXT is(r.barcode_type,  'EAN-13',           'EAN-13: type detected correctly');
END;
$$;


-- Valid UPC-A (12 digits) → padded with two leading zeros
CREATE OR REPLACE FUNCTION tests.test_02_normalize_upca()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM dirac.normalize_barcode('012345678905');
    RETURN NEXT is(r.normalized,    '00012345678905',   'UPC-A: normalized to 14 digits');
    RETURN NEXT is(r.barcode_type,  'UPC-A',            'UPC-A: type detected correctly');
END;
$$;


-- Valid EAN-8 (8 digits) → padded with six leading zeros
CREATE OR REPLACE FUNCTION tests.test_03_normalize_ean8()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM dirac.normalize_barcode('40170725');
    RETURN NEXT is(r.normalized,    '00000040170725',   'EAN-8: normalized to 14 digits');
    RETURN NEXT is(r.barcode_type,  'EAN-8',            'EAN-8: type detected correctly');
END;
$$;


-- Valid GTIN-14 (already 14 digits) → no padding needed
CREATE OR REPLACE FUNCTION tests.test_04_normalize_gtin14()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM dirac.normalize_barcode('00012345678905');
    RETURN NEXT is(r.normalized,    '00012345678905',   'GTIN-14: returned as-is');
    RETURN NEXT is(r.barcode_type,  'GTIN-14',          'GTIN-14: type detected correctly');
END;
$$;


-- Valid ISBN-13 (starts with 978) → type flagged as ISBN-13 not EAN-13
CREATE OR REPLACE FUNCTION tests.test_05_normalize_isbn13()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    SELECT * INTO r FROM dirac.normalize_barcode('9780140328721');
    RETURN NEXT is(r.normalized,    '09780140328721',   'ISBN-13: normalized to 14 digits');
    RETURN NEXT is(r.barcode_type,  'ISBN-13',          'ISBN-13: type detected from 978 prefix');
END;
$$;


-- Non-digit characters (dashes, spaces) are stripped before processing.
-- Manual data entry and copy-paste often introduce these.
CREATE OR REPLACE FUNCTION tests.test_06_normalize_strips_non_digits()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE r RECORD;
BEGIN
    -- Dashes stripped: '4006-3813-33931' → '4006381333931' → '04006381333931'
    SELECT * INTO r FROM dirac.normalize_barcode('4006-3813-33931');
    RETURN NEXT is(r.normalized,    '04006381333931',   'Dashes stripped before normalizing');

    -- Spaces stripped
    SELECT * INTO r FROM dirac.normalize_barcode('4006 381333931');
    RETURN NEXT is(r.normalized,    '04006381333931',   'Spaces stripped before normalizing');
END;
$$;


-- The critical deduplication insight:
-- UPC-A '012345678905' (12 digits) and EAN-13 '0012345678905' (13 digits)
-- represent the same product and must produce the same canonical form.
-- GS1 guarantees that left-zero-padding preserves the check digit, so both
-- normalise to '00012345678905'.
CREATE OR REPLACE FUNCTION tests.test_07_normalize_cross_format_canonical()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_upca  CHAR(14);
    v_ean13 CHAR(14);
BEGIN
    SELECT normalized INTO v_upca  FROM dirac.normalize_barcode('012345678905');
    SELECT normalized INTO v_ean13 FROM dirac.normalize_barcode('0012345678905');
    RETURN NEXT is(v_upca, v_ean13,
        'UPC-A and its EAN-13 equivalent normalise to the same canonical barcode');
END;
$$;


-- A barcode with a wrong check digit must be rejected immediately.
-- This is the first line of defence against garbage data.
CREATE OR REPLACE FUNCTION tests.test_08_normalize_rejects_bad_check_digit()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
BEGIN
    -- '4006381333932' — last digit changed from 1 to 2
    RETURN NEXT throws_ok(
        $sql$ SELECT * FROM dirac.normalize_barcode('4006381333932') $sql$,
        'Barcode with wrong check digit raises an exception'
    );
END;
$$;


-- Unsupported lengths (e.g. 9 digits) must be rejected with a clear error.
CREATE OR REPLACE FUNCTION tests.test_09_normalize_rejects_bad_length()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
BEGIN
    RETURN NEXT throws_ok(
        $sql$ SELECT * FROM dirac.normalize_barcode('123456789') $sql$,
        'Barcode with unsupported digit length raises an exception'
    );
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- upsert_product tests
-- ─────────────────────────────────────────────────────────────────────────────

-- Inserting a new product returns a valid (non-null) product id,
-- and the product + barcode rows actually exist in the tables.
CREATE OR REPLACE FUNCTION tests.test_10_upsert_creates_product()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE v_id BIGINT;
BEGIN
    v_id := dirac.upsert_product('Coca-Cola 500ml', 'Soft drink', '4006381333931');

    RETURN NEXT ok(v_id IS NOT NULL, 'upsert_product returns a non-null id');

    RETURN NEXT ok(
        EXISTS(SELECT 1 FROM dirac.product WHERE id = v_id AND name = 'Coca-Cola 500ml'),
        'Product row created with correct name'
    );

    RETURN NEXT ok(
        EXISTS(SELECT 1 FROM dirac.barcode
               WHERE product_id = v_id
                 AND normalized_barcode = '04006381333931'
                 AND barcode_type = 'EAN-13'),
        'Barcode row created with correct normalized form and type'
    );
END;
$$;


-- Calling upsert with the exact same barcode a second time must return
-- the same product_id — no duplicate product row is created.
CREATE OR REPLACE FUNCTION tests.test_11_upsert_deduplicates_same_barcode()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_id1 BIGINT;
    v_id2 BIGINT;
BEGIN
    v_id1 := dirac.upsert_product('Coca-Cola 500ml', 'Soft drink', '4006381333931');
    v_id2 := dirac.upsert_product('Coca-Cola 500ml', 'Soft drink', '4006381333931');

    RETURN NEXT is(v_id2, v_id1, 'Second upsert with same barcode returns existing product id');

    RETURN NEXT is(
        (SELECT COUNT(*)::INT FROM dirac.product WHERE id = v_id1),
        1,
        'Exactly one product row exists after two identical upserts'
    );
END;
$$;


-- The core deduplication scenario: the same physical product arrives from
-- two different sources, one with a UPC-A barcode and one with the equivalent
-- EAN-13. They must map to the same product record.
CREATE OR REPLACE FUNCTION tests.test_12_upsert_cross_format_dedup()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_id_upca  BIGINT;
    v_id_ean13 BIGINT;
BEGIN
    -- Source A sends UPC-A
    v_id_upca  := dirac.upsert_product('Test Product', 'From source A', '012345678905');
    -- Source B sends the EAN-13 encoding of the same barcode
    v_id_ean13 := dirac.upsert_product('Test Product', 'From source B', '0012345678905');

    RETURN NEXT is(v_id_ean13, v_id_upca,
        'UPC-A and equivalent EAN-13 deduplicate to the same product');
END;
$$;


-- When a duplicate is found, the first-seen name and description are kept.
-- We do not overwrite with data from subsequent sources because different
-- sources use different names for the same product (data flickering risk).
CREATE OR REPLACE FUNCTION tests.test_13_upsert_preserves_first_seen_name()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE v_id BIGINT;
BEGIN
    v_id := dirac.upsert_product('Original Name', 'Original Desc', '4006381333931');
    PERFORM dirac.upsert_product('Different Name', 'Different Desc', '4006381333931');

    RETURN NEXT is(
        (SELECT name FROM dirac.product WHERE id = v_id),
        'Original Name',
        'First-seen product name is preserved when the same barcode is upserted again'
    );

    RETURN NEXT is(
        (SELECT description FROM dirac.product WHERE id = v_id),
        'Original Desc',
        'First-seen product description is preserved on duplicate upsert'
    );
END;
$$;


-- Passing an invalid barcode to upsert_product must raise an exception.
-- The product table must remain untouched.
CREATE OR REPLACE FUNCTION tests.test_14_upsert_rejects_invalid_barcode()
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
BEGIN
    -- Bad check digit
    RETURN NEXT throws_ok(
        $sql$ SELECT dirac.upsert_product('Bad Product', NULL, '4006381333932') $sql$,
        'upsert_product rejects a barcode with a bad check digit'
    );

    -- Unsupported length
    RETURN NEXT throws_ok(
        $sql$ SELECT dirac.upsert_product('Bad Product', NULL, '12345') $sql$,
        'upsert_product rejects a barcode with an unsupported length'
    );
END;
$$;

-- +goose StatementEnd


-- +goose Down
-- +goose StatementBegin
DROP SCHEMA IF EXISTS tests CASCADE;
-- +goose StatementEnd

-- +goose StatementBegin
DROP EXTENSION IF EXISTS pgtap;
-- +goose StatementEnd
