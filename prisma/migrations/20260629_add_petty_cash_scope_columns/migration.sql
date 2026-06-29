ALTER TABLE "petty_cash_logs"
ADD COLUMN IF NOT EXISTS "branch_id" BIGINT,
ADD COLUMN IF NOT EXISTS "shift_id" BIGINT,
ADD COLUMN IF NOT EXISTS "user_name" TEXT;

CREATE INDEX IF NOT EXISTS "petty_cash_logs_tenant_branch_user_created_at_idx"
ON "petty_cash_logs" ("tenant_id", "branch_id", "user_id", "created_at");

CREATE INDEX IF NOT EXISTS "petty_cash_logs_shift_id_idx"
ON "petty_cash_logs" ("shift_id");
