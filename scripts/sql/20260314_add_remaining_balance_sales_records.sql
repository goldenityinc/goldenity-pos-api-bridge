ALTER TABLE sales_records
ADD COLUMN IF NOT EXISTS remaining_balance NUMERIC(14,2);

DO $$
DECLARE
  method_col TEXT;
  amount_col TEXT;
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'payment_method'
    ) THEN 'payment_method'
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'payment_type'
    ) THEN 'payment_type'
    ELSE NULL
  END INTO method_col;

  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'total_price'
    ) THEN 'total_price'
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'total_amount'
    ) THEN 'total_amount'
    ELSE NULL
  END INTO amount_col;

  IF method_col IS NULL OR amount_col IS NULL THEN
    RAISE EXCEPTION 'Kolom payment/amount untuk sales_records tidak ditemukan. method_col=%, amount_col=%', method_col, amount_col;
  END IF;

  EXECUTE format(
    'UPDATE sales_records
     SET remaining_balance = COALESCE(%I::numeric, 0)
     WHERE UPPER(COALESCE(%I::text, '''')) = ''KAS BON''
       AND remaining_balance IS NULL',
    amount_col,
    method_col
  );

  EXECUTE format(
    'UPDATE sales_records
     SET remaining_balance = 0
     WHERE UPPER(COALESCE(%I::text, '''')) <> ''KAS BON''
       AND remaining_balance IS NULL',
    method_col
  );
END $$;

CREATE OR REPLACE FUNCTION set_sales_records_remaining_balance()
RETURNS TRIGGER AS $$
DECLARE
  method_col TEXT := TG_ARGV[0];
  amount_col TEXT := TG_ARGV[1];
  method_value TEXT;
  amount_value NUMERIC := 0;
  current_balance NUMERIC;
BEGIN
  method_value := UPPER(COALESCE(to_jsonb(NEW) ->> method_col, ''));

  IF to_jsonb(NEW) ? amount_col THEN
    amount_value := COALESCE((to_jsonb(NEW) ->> amount_col)::numeric, 0);
  END IF;

  current_balance := CASE
    WHEN to_jsonb(NEW) ? 'remaining_balance' THEN (to_jsonb(NEW) ->> 'remaining_balance')::numeric
    ELSE NULL
  END;

  IF method_value = 'KAS BON' THEN
    IF current_balance IS NULL THEN
      NEW := jsonb_populate_record(NEW, jsonb_set(to_jsonb(NEW), '{remaining_balance}', to_jsonb(amount_value)));
    END IF;
  ELSE
    NEW := jsonb_populate_record(NEW, jsonb_set(to_jsonb(NEW), '{remaining_balance}', to_jsonb(COALESCE(current_balance, 0))));
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_sales_records_remaining_balance ON sales_records;

DO $$
DECLARE
  method_col TEXT;
  amount_col TEXT;
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'payment_method'
    ) THEN 'payment_method'
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'payment_type'
    ) THEN 'payment_type'
    ELSE NULL
  END INTO method_col;

  SELECT CASE
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'total_price'
    ) THEN 'total_price'
    WHEN EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'sales_records'
        AND column_name = 'total_amount'
    ) THEN 'total_amount'
    ELSE NULL
  END INTO amount_col;

  IF method_col IS NULL OR amount_col IS NULL THEN
    RAISE EXCEPTION 'Kolom payment/amount untuk sales_records tidak ditemukan. method_col=%, amount_col=%', method_col, amount_col;
  END IF;

  EXECUTE format(
    'CREATE TRIGGER trg_set_sales_records_remaining_balance
     BEFORE INSERT OR UPDATE OF %I, %I, remaining_balance
     ON sales_records
     FOR EACH ROW
     EXECUTE FUNCTION set_sales_records_remaining_balance(%L, %L)',
    method_col,
    amount_col,
    method_col,
    amount_col
  );
END $$;
