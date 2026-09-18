-- NS BI Backend
-- Esquema consolidado para PostgreSQL, gerado a partir de app/models.py.
-- Execute somente em um banco vazio. Em ambientes existentes, use:
--   alembic upgrade head

BEGIN;

CREATE TABLE carriers (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_carriers_mercos_id ON carriers (mercos_id);

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    parent_mercos_id VARCHAR(80),
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_categories_mercos_id ON categories (mercos_id);
CREATE INDEX ix_categories_parent_mercos_id ON categories (parent_mercos_id);

CREATE TABLE commercial_policies (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_commercial_policies_mercos_id ON commercial_policies (mercos_id);

CREATE TABLE crm_attendances (
    id SERIAL PRIMARY KEY,
    customer_mercos_id VARCHAR(80) NOT NULL,
    seller_name VARCHAR(200),
    status VARCHAR(30) NOT NULL,
    claimed_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,
    outcome VARCHAR(20),
    sale_value NUMERIC(14, 2),
    order_number VARCHAR(80),
    ai_analysis JSON,
    ai_analysis_at TIMESTAMP WITH TIME ZONE,
    ai_priority_score DOUBLE PRECISION,
    ai_priority_reason TEXT,
    ai_priority_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uq_crm_attendances_customer UNIQUE (customer_mercos_id)
);
CREATE INDEX ix_crm_attendances_ai_priority_score ON crm_attendances (ai_priority_score);
CREATE INDEX ix_crm_attendances_customer_mercos_id ON crm_attendances (customer_mercos_id);
CREATE INDEX ix_crm_attendances_status ON crm_attendances (status);
CREATE INDEX ix_crm_attendances_status_finished ON crm_attendances (status, finished_at);

CREATE TABLE customer_segments (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_customer_segments_mercos_id ON customer_segments (mercos_id);

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    document VARCHAR(30),
    city VARCHAR(120),
    state VARCHAR(5),
    email VARCHAR(300),
    phone VARCHAR(40),
    emails JSON NOT NULL,
    phones JSON NOT NULL,
    addresses JSON NOT NULL,
    contact_profile JSON NOT NULL,
    segment_mercos_id VARCHAR(80),
    created_at_source TIMESTAMP WITH TIME ZONE,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_customers_mercos_id ON customers (mercos_id);
CREATE INDEX ix_customers_segment_mercos_id ON customers (segment_mercos_id);

CREATE TABLE export_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(120) NOT NULL,
    report VARCHAR(50) NOT NULL,
    format VARCHAR(10) NOT NULL,
    status VARCHAR(30) NOT NULL,
    filters JSON NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    rows INTEGER NOT NULL,
    error TEXT
);
CREATE INDEX ix_export_runs_started_at ON export_runs (started_at);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_mercos_id VARCHAR(80) NOT NULL,
    position INTEGER NOT NULL,
    mercos_item_id VARCHAR(80),
    product_mercos_id VARCHAR(80),
    code VARCHAR(100),
    name VARCHAR(400) NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL,
    list_unit_price NUMERIC(18, 2),
    unit_price NUMERIC(18, 2) NOT NULL,
    discount NUMERIC(18, 2) NOT NULL,
    total NUMERIC(18, 2) NOT NULL,
    excluded BOOLEAN NOT NULL,
    raw JSON NOT NULL,
    CONSTRAINT uq_order_items_order_position UNIQUE (order_mercos_id, position),
    CONSTRAINT uq_order_items_order_mercos_item UNIQUE (order_mercos_id, mercos_item_id)
);
CREATE INDEX ix_order_items_excluded ON order_items (excluded);
CREATE INDEX ix_order_items_mercos_item_id ON order_items (mercos_item_id);
CREATE INDEX ix_order_items_order_mercos_id ON order_items (order_mercos_id);
CREATE INDEX ix_order_items_product_mercos_id ON order_items (product_mercos_id);

CREATE TABLE order_types (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_order_types_mercos_id ON order_types (mercos_id);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    number VARCHAR(100) NOT NULL,
    customer_mercos_id VARCHAR(80),
    seller_mercos_id VARCHAR(80),
    status VARCHAR(50) NOT NULL,
    issued_at TIMESTAMP WITH TIME ZONE,
    total NUMERIC(18, 2) NOT NULL,
    discount NUMERIC(18, 2) NOT NULL,
    order_type_mercos_id VARCHAR(80),
    payment_condition_mercos_id VARCHAR(80),
    price_table_mercos_id VARCHAR(80),
    carrier_mercos_id VARCHAR(80),
    commercial_policy_mercos_id VARCHAR(80),
    gross_total NUMERIC(18, 2),
    net_total NUMERIC(18, 2),
    discount_value NUMERIC(18, 2),
    discount_percent NUMERIC(9, 4),
    item_count INTEGER,
    sku_count INTEGER,
    shipping_method_id VARCHAR(80),
    shipping_method VARCHAR(200),
    shipping_cost NUMERIC(18, 2),
    shipment_status VARCHAR(80),
    tracking_code VARCHAR(200),
    tracking_url TEXT,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    estimated_delivery VARCHAR(120),
    shipment_integrator VARCHAR(160),
    distribution_center_id VARCHAR(80),
    shipping_city VARCHAR(120),
    shipping_state VARCHAR(10),
    source_created_at TIMESTAMP WITH TIME ZONE,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE INDEX ix_orders_carrier_mercos_id ON orders (carrier_mercos_id);
CREATE INDEX ix_orders_commercial_policy_mercos_id ON orders (commercial_policy_mercos_id);
CREATE INDEX ix_orders_customer_mercos_id ON orders (customer_mercos_id);
CREATE INDEX ix_orders_delivered_at ON orders (delivered_at);
CREATE INDEX ix_orders_distribution_center_id ON orders (distribution_center_id);
CREATE INDEX ix_orders_issued_at ON orders (issued_at);
CREATE UNIQUE INDEX ix_orders_mercos_id ON orders (mercos_id);
CREATE INDEX ix_orders_number ON orders (number);
CREATE INDEX ix_orders_order_type_mercos_id ON orders (order_type_mercos_id);
CREATE INDEX ix_orders_payment_condition_mercos_id ON orders (payment_condition_mercos_id);
CREATE INDEX ix_orders_price_table_mercos_id ON orders (price_table_mercos_id);
CREATE INDEX ix_orders_seller_mercos_id ON orders (seller_mercos_id);
CREATE INDEX ix_orders_shipped_at ON orders (shipped_at);
CREATE INDEX ix_orders_shipment_integrator ON orders (shipment_integrator);
CREATE INDEX ix_orders_shipment_status ON orders (shipment_status);
CREATE INDEX ix_orders_shipping_city ON orders (shipping_city);
CREATE INDEX ix_orders_shipping_method ON orders (shipping_method);
CREATE INDEX ix_orders_shipping_method_id ON orders (shipping_method_id);
CREATE INDEX ix_orders_shipping_state ON orders (shipping_state);
CREATE INDEX ix_orders_status ON orders (status);
CREATE INDEX ix_orders_tracking_code ON orders (tracking_code);

CREATE TABLE payment_conditions (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_payment_conditions_mercos_id ON payment_conditions (mercos_id);

CREATE TABLE price_tables (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL
);
CREATE UNIQUE INDEX ix_price_tables_mercos_id ON price_tables (mercos_id);

CREATE TABLE product_prices (
    id SERIAL PRIMARY KEY,
    product_mercos_id VARCHAR(80) NOT NULL,
    price_table_mercos_id VARCHAR(80) NOT NULL,
    price NUMERIC(18, 2) NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    raw JSON NOT NULL,
    CONSTRAINT uq_product_prices_product_table
        UNIQUE (product_mercos_id, price_table_mercos_id)
);
CREATE INDEX ix_product_prices_price_table_mercos_id ON product_prices (price_table_mercos_id);
CREATE INDEX ix_product_prices_product_mercos_id ON product_prices (product_mercos_id);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL,
    code VARCHAR(100),
    name VARCHAR(400) NOT NULL,
    category_id VARCHAR(80),
    category_mercos_id VARCHAR(80),
    unit VARCHAR(30),
    list_price NUMERIC(18, 2) NOT NULL,
    minimum_price NUMERIC(18, 2),
    stock NUMERIC(18, 4) NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    created_at_source TIMESTAMP WITH TIME ZONE,
    active BOOLEAN NOT NULL,
    raw JSON NOT NULL
);
CREATE INDEX ix_products_category_mercos_id ON products (category_mercos_id);
CREATE INDEX ix_products_code ON products (code);
CREATE UNIQUE INDEX ix_products_mercos_id ON products (mercos_id);

CREATE TABLE sellers (
    id SERIAL PRIMARY KEY,
    mercos_id VARCHAR(80) NOT NULL UNIQUE,
    name VARCHAR(300) NOT NULL,
    active BOOLEAN NOT NULL,
    raw JSON NOT NULL
);

CREATE TABLE sync_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    resource VARCHAR(50) NOT NULL,
    mode VARCHAR(20) NOT NULL,
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    cursor_before TEXT,
    cursor_after TEXT,
    pages INTEGER NOT NULL,
    received INTEGER NOT NULL,
    persisted INTEGER NOT NULL,
    failed INTEGER NOT NULL,
    details JSON NOT NULL,
    error TEXT
);
CREATE INDEX ix_sync_runs_resource_started_at ON sync_runs (resource, started_at);

CREATE TABLE sync_states (
    resource VARCHAR(50) PRIMARY KEY,
    cursor TEXT,
    last_success_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(30) NOT NULL,
    records INTEGER NOT NULL,
    error TEXT,
    lease_token VARCHAR(36),
    heartbeat_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE tray_entities (
    id SERIAL PRIMARY KEY,
    resource VARCHAR(80) NOT NULL,
    source_id VARCHAR(120) NOT NULL,
    payload JSON NOT NULL,
    source_updated_at TIMESTAMP WITH TIME ZONE,
    synced_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uq_tray_entities_resource_source UNIQUE (resource, source_id)
);
CREATE INDEX ix_tray_entities_resource ON tray_entities (resource);

COMMIT;
