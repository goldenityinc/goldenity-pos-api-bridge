ALTER TABLE order_history_items
ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_order_history_items_is_archived
ON order_history_items (is_archived);
