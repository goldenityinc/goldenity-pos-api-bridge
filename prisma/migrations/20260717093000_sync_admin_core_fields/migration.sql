-- Sync bridge schema with Admin Core additive fields.
ALTER TABLE IF EXISTS "tenants"
  ADD COLUMN IF NOT EXISTS "qris_image_url" TEXT,
  ADD COLUMN IF NOT EXISTS "allow_pay_at_cashier" BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS "is_payment_proof_mandatory" BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE IF EXISTS "branches"
  ADD COLUMN IF NOT EXISTS "branch_code" TEXT,
  ADD COLUMN IF NOT EXISTS "qris_image_url" TEXT,
  ADD COLUMN IF NOT EXISTS "is_active" BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS "is_main_branch" BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS "is_blind_close_enabled" BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE IF EXISTS "products"
  ADD COLUMN IF NOT EXISTS "is_stock_tracked" BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE IF EXISTS "transactions"
  ADD COLUMN IF NOT EXISTS "notes" TEXT;

ALTER TABLE IF EXISTS "sales_records"
  ADD COLUMN IF NOT EXISTS "notes" TEXT;

ALTER TABLE IF EXISTS "sales_record_items"
  ADD COLUMN IF NOT EXISTS "notes" TEXT;

ALTER TABLE IF EXISTS "users"
  ADD COLUMN IF NOT EXISTS "branch_id" BIGINT;
