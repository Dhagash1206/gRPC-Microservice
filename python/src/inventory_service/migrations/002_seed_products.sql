INSERT INTO products (sku, name, unit_price_cents, available_quantity) VALUES
    ('KEYBOARD', 'Mechanical Keyboard', 4999, 10),
    ('MOUSE', 'Wireless Mouse', 1999, 25),
    ('MONITOR', '27in Monitor', 19999, 3),
    ('LAPTOP', 'Pro Laptop', 149999, 5)
ON CONFLICT (sku) DO NOTHING;
