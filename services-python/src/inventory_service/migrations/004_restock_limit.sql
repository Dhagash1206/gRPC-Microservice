-- Restocking adds to available_quantity; keep it far from the int4 limit.
ALTER TABLE products ADD CONSTRAINT products_available_quantity_max CHECK (available_quantity <= 1000000);
