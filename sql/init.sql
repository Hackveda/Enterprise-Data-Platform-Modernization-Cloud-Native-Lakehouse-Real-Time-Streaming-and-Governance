CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGSERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  country TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
  amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO customers(full_name,email,country) VALUES
('Asha Rao','asha@example.com','IN'),
('Ben Wong','ben@example.com','SG'),
('Carla Smith','carla@example.com','US')
ON CONFLICT DO NOTHING;

INSERT INTO orders(customer_id,amount,status)
SELECT 1, 1299.00, 'CREATED' WHERE NOT EXISTS (SELECT 1 FROM orders WHERE order_id=1);
INSERT INTO orders(customer_id,amount,status)
SELECT 2, 2499.50, 'PAID' WHERE NOT EXISTS (SELECT 1 FROM orders WHERE order_id=2);
