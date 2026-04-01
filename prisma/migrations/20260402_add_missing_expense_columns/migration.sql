-- 🔧 FIX: Add missing expense columns to support void/cancel functionality
-- The Expense table was missing critical fields that are required for:
-- - Tracking expense status (active, void, etc)
-- - Recording void reason and timestamp
-- - Categorizing and describing expenses

-- Add missing columns if they don't exist
ALTER TABLE expenses
ADD COLUMN IF NOT EXISTS expense_number VARCHAR(255) UNIQUE,
ADD COLUMN IF NOT EXISTS title VARCHAR(255) DEFAULT 'Pengeluaran',
ADD COLUMN IF NOT EXISTS description TEXT,
ADD COLUMN IF NOT EXISTS payment_method VARCHAR(50) DEFAULT 'Cash',
ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'active',
ADD COLUMN IF NOT EXISTS void_reason TEXT,
ADD COLUMN IF NOT EXISTS voided_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS attachment_url VARCHAR(255),
ADD COLUMN IF NOT EXISTS category_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS reference_id VARCHAR(255);

-- Create indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_expenses_tenant_id ON expenses(tenant_id);
CREATE INDEX IF NOT EXISTS idx_expenses_status ON expenses(status);
CREATE INDEX IF NOT EXISTS idx_expenses_created_at ON expenses(created_at);
CREATE INDEX IF NOT EXISTS idx_expenses_reference_id ON expenses(reference_id);

-- Add constraint to ensure status is valid
ALTER TABLE expenses
ADD CONSTRAINT chk_expense_status CHECK (status IN ('active', 'void', 'cancelled', 'archived'));
