-- Safe additive migration: add product availability without destructive changes.
ALTER TABLE "products"
ADD COLUMN IF NOT EXISTS "is_available" BOOLEAN DEFAULT true;
