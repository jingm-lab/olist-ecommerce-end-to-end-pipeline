CREATE TABLE customers (
	customer_id VARCHAR(50) PRIMARY KEY,
	customer_unique_id VARCHAR(50) NOT NULL,
	customer_zip_code_prefix CHAR(5) NOT NULL,
	customer_city VARCHAR(50) NOT NULL,
	customer_state CHAR(2) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE sellers (
	seller_id VARCHAR(50) NOT NULL PRIMARY KEY,
	seller_zip_code_prefix CHAR(5) NOT NULL,
	seller_city VARCHAR(50) NOT NULL,
	seller_state CHAR(2) NOT NULL,
	is_active BOOLEAN DEFAULT TRUE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE products (
	product_id VARCHAR(50) NOT NULL PRIMARY KEY,
	product_category_name VARCHAR(50),
	product_name_length INT,
	product_description_length INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT,
	is_active BOOLEAN DEFAULT TRUE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE geolocations (
	geolocation_zip_code_prefix CHAR(5) NOT NULL,
	geolocation_lat DOUBLE PRECISION NOT NULL,
	geolocation_lng DOUBLE PRECISION NOT NULL,
	geolocation_city VARCHAR(50) NOT NULL,
	geolocation_state CHAR(2) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE orders (
	order_id VARCHAR(50) NOT NULL PRIMARY KEY,
	customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id),
	order_status VARCHAR(25) NOT NULL,
	order_purchase_timestamp TIMESTAMP NOT NULL,
	order_approved_at TIMESTAMP,
	order_delivered_carrier_date TIMESTAMP,
	order_delivered_customer_date TIMESTAMP,
	order_estimated_delivery_date TIMESTAMP NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE order_payments (
	order_id VARCHAR(50) NOT NULL REFERENCES orders(order_id),
	payment_sequential INT NOT NULL,
	payment_type VARCHAR(50) NOT NULL DEFAULT 'credit_card',
	payment_installments INT NOT NULL,
	payment_value NUMERIC(8,2) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_items (
	order_id VARCHAR(50) NOT NULL REFERENCES orders(order_id),
	order_item_id INT NOT NULL,
	product_id VARCHAR(50) NOT NULL REFERENCES products(product_id),
	seller_id VARCHAR(50) NOT NULL REFERENCES sellers(seller_id),
	shipping_limit_date TIMESTAMP NOT NULL,
	price NUMERIC(8,2) NOT NULL,
	freight_value NUMERIC(8,2) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_reviews (
	review_id VARCHAR(50) NOT NULL,
	order_id VARCHAR(50) NOT NULL REFERENCES orders(order_id),
	review_score INT NOT NULL,
	review_creation_date TIMESTAMP NOT NULL,
	review_answer_timestamp TIMESTAMP NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	PRIMARY KEY (review_id, order_id)
);

CREATE TABLE product_category_name_translation (
	product_category_name VARCHAR(50) NOT NULL,
	product_category_name_english VARCHAR(50) NOT NULL
);