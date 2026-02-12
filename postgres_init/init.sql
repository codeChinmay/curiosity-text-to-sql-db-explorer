-- Create tables
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    total_amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

-- Insert sample data
INSERT INTO users (username, email) VALUES 
('alice', 'alice@example.com'),
('bob', 'bob@example.com'),
('charlie', 'charlie@example.com');

INSERT INTO products (name, price, stock) VALUES 
('Laptop', 999.99, 10),
('Mouse', 25.50, 100),
('Keyboard', 50.00, 50),
('Monitor', 200.00, 20);

INSERT INTO orders (user_id, total_amount, status) VALUES 
(1, 1025.49, 'completed'),
(2, 50.00, 'processing'),
(1, 200.00, 'pending');

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES 
(1, 1, 1, 999.99),
(1, 2, 1, 25.50),
(2, 3, 1, 50.00),
(3, 4, 1, 200.00);
