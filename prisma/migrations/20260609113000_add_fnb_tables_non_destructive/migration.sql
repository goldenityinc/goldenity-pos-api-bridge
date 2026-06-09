-- Non-destructive F&B table migration for production (Bridge DB).
-- Apply with `prisma db execute`. Never reset schema.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'TableStatus') THEN
    CREATE TYPE "TableStatus" AS ENUM ('AVAILABLE', 'OCCUPIED', 'RESERVED');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'OrderType') THEN
    CREATE TYPE "OrderType" AS ENUM ('WALK_IN', 'PRE_ORDER', 'DINE_IN', 'TAKEAWAY', 'DELIVERY');
  ELSE
    IF NOT EXISTS (
      SELECT 1
      FROM pg_enum e
      JOIN pg_type t ON t.oid = e.enumtypid
      WHERE t.typname = 'OrderType'
        AND e.enumlabel = 'DINE_IN'
    ) THEN
      ALTER TYPE "OrderType" ADD VALUE 'DINE_IN';
    END IF;
    IF NOT EXISTS (
      SELECT 1
      FROM pg_enum e
      JOIN pg_type t ON t.oid = e.enumtypid
      WHERE t.typname = 'OrderType'
        AND e.enumlabel = 'TAKEAWAY'
    ) THEN
      ALTER TYPE "OrderType" ADD VALUE 'TAKEAWAY';
    END IF;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'OrderStatus') THEN
    CREATE TYPE "OrderStatus" AS ENUM (
      'PENDING',
      'PENDING_PAYMENT',
      'PREPARING',
      'READY_FOR_PICKUP',
      'COMPLETED',
      'CANCELLED'
    );
  ELSE
    IF NOT EXISTS (
      SELECT 1
      FROM pg_enum e
      JOIN pg_type t ON t.oid = e.enumtypid
      WHERE t.typname = 'OrderStatus'
        AND e.enumlabel = 'PENDING_PAYMENT'
    ) THEN
      ALTER TYPE "OrderStatus" ADD VALUE 'PENDING_PAYMENT';
    END IF;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS "tables" (
  "id" BIGSERIAL PRIMARY KEY,
  "tenant_id" TEXT NOT NULL,
  "table_number" TEXT NOT NULL,
  "capacity" INTEGER NOT NULL,
  "status" "TableStatus" NOT NULL DEFAULT 'AVAILABLE',
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "tables_tenant_id_idx" ON "tables"("tenant_id");
CREATE UNIQUE INDEX IF NOT EXISTS "tables_tenant_id_table_number_key"
  ON "tables"("tenant_id", "table_number");

ALTER TABLE "sales_records"
  ADD COLUMN IF NOT EXISTS "table_id" BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sales_records'
      AND column_name = 'order_type'
  ) THEN
    ALTER TABLE "sales_records"
      ADD COLUMN "order_type" "OrderType";
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sales_records'
      AND column_name = 'order_type'
      AND udt_name = 'text'
  ) THEN
    ALTER TABLE "sales_records"
      ALTER COLUMN "order_type" TYPE "OrderType"
      USING (
        CASE
          WHEN UPPER(COALESCE("order_type", '')) IN ('WALK_IN','PRE_ORDER','DINE_IN','TAKEAWAY','DELIVERY')
            THEN UPPER("order_type")::"OrderType"
          ELSE NULL
        END
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "sales_records_table_id_idx" ON "sales_records"("table_id");
