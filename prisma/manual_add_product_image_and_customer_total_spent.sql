DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'products'
  ) THEN
    ALTER TABLE public.products
    ADD COLUMN IF NOT EXISTS image_url TEXT;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  phone TEXT,
  total_spent DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE customers
ADD COLUMN IF NOT EXISTS total_spent DOUBLE PRECISION NOT NULL DEFAULT 0;