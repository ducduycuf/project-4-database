-- 1. Total revenue per month
select  date_trunc('month', order_date) as month,
        sum(subtotal) as total_revenue
from order_items
group by month
order by month;

-- 2. Orders filtered by seller and date
select * from orders
where seller_id = 15
and order_date between '2025-11-01' and '2025-11-30'
order by order_date;

-- 3. Filter data in order_item by product_id
select * from order_items
where product_id = 112;

-- 4. Find order with highest total_amount
select order_id, total_amount
from orders
order by total_amount desc
limit 1;

-- 5. List products with highest quantity sold
select order_item_id, order_id, product_id, quantity
from order_items
where quantity = (select max(quantity) from order_items);

-- 6. Orders by Seller in October
select * from orders
where extract(month from order_date) = 10
order by seller_id;

-- 7. Revenue per Product per Month
select  date_trunc('month', order_date) as month, 
        product_id, 
        sum(subtotal) as revenue
from order_items
group by month, product_id;

-- or
select  product_id,
        date_trunc('month', order_date) as month,
        sum(subtotal) as revenue
from order_items
group by product_id, month;

-- 8. Products Sold per Seller
with cte as (
    select order_id, count(product_id) as total_products
    from order_items
    group by order_id
)
select seller_id, sum(total_products) as products_sold
from orders
join cte on cte.order_id = orders.order_id
group by seller_id
order by seller_id;

-------------------------------------
-- create partitioned parent orders table
create table orders_partitioned (
    order_id bigint,
    order_date timestamp,
    seller_id int,
    status varchar(50),
    total_amount decimal(12,2),
    created_at timestamp
) partition by range (order_date);

-- create partitioned parent order_items table
create table order_items_partitioned (
    order_item_id bigint,
    order_id bigint,
    product_id int,
    order_date timestamp,
    quantity int,
    unit_price decimal(12,2),
    subtotal decimal(12,2),
    created_at timestamp
) partition by range (order_date);

-- partitions of orders table
DO $$
DECLARE
    start_date DATE := '2024-11-01';
    end_date DATE := '2025-12-01'; -- exclusive upper bound
    current DATE;
BEGIN
    current := start_date;
    WHILE current < end_date LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS orders_%s PARTITION OF orders_partitioned
             FOR VALUES FROM (''%s'') TO (''%s'')',
            to_char(current, 'YYYY_MM'),
            current,
            current + INTERVAL '1 month'
        );
        current := current + INTERVAL '1 month';
    END LOOP;
END $$;

-- partitions of orders_items table
DO $$
DECLARE
    start_date DATE := '2024-11-01';
    end_date DATE := '2025-12-01'; -- exclusive upper bound
    current DATE;
BEGIN
    current := start_date;
    WHILE current < end_date LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS order_items_%s PARTITION OF order_items_partitioned
             FOR VALUES FROM (''%s'') TO (''%s'')',
            to_char(current, 'YYYY_MM'),
            current,
            current + INTERVAL '1 month'
        );
        current := current + INTERVAL '1 month';
    END LOOP;
END $$;

-- insert data into partitions of orders table
INSERT INTO orders_partitioned (order_id, order_date, seller_id, status, total_amount, created_at)
SELECT order_id, order_date, seller_id, status, total_amount, created_at
FROM orders;

-- insert data into partitions of order_items table
INSERT INTO order_items_partitioned (order_item_id, order_id, product_id, order_date, quantity, unit_price, subtotal, created_at)
SELECT order_item_id, order_id, product_id, order_date, quantity, unit_price, subtotal, created_at
FROM order_items;

-- create index for each partitions of order_items table
DO $$
DECLARE
    start_date DATE := '2024-11-01';
    end_date DATE := '2025-12-01';
    current DATE;
BEGIN
    current := start_date;
    WHILE current < end_date LOOP
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_order_items_%s_product_id
             ON order_items_%s(product_id)',
            to_char(current, 'YYYY_MM'),
            to_char(current, 'YYYY_MM')
        );
        current := current + INTERVAL '1 month';
    END LOOP;
END $$;

-------------------------------------------
-- Requirements 3
-- 1. Monthly Revenue Report
CREATE OR REPLACE FUNCTION fn_monthly_revenue_report(
    start_date DATE,
    end_date   DATE
)
RETURNS TABLE (
    month          DATE,
    total_orders   BIGINT,
    total_quantity BIGINT,
    total_revenue  NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        date_trunc('month', oi.order_date)::date AS month,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.subtotal) AS total_revenue
    FROM order_items_partitioned oi
    WHERE oi.order_date BETWEEN start_date AND end_date
    GROUP BY 1
    ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- 2. Daily Revenue Report
CREATE OR REPLACE FUNCTION fn_daily_revenue_report(
    start_date DATE,
    end_date   DATE,
    product_list int[] DEFAULT NULL
)
RETURNS TABLE (
    date          DATE,
    total_orders   BIGINT,
    total_quantity BIGINT,
    total_revenue  NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        oi.order_date::date AS date,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.subtotal) AS total_revenue
    FROM order_items_partitioned oi
    WHERE oi.order_date BETWEEN start_date AND end_date
      AND (product_list IS NULL OR oi.product_id = ANY(product_list))
    GROUP BY 1
    ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- 3. Seller Performance Report
create or REPLACE FUNCTION fn_seller_performance_report(
    start_date date,
    end_date date,
    brand_id_list int[] DEFAULT NULL,
    category_id_list int[] DEFAULT NULL
)
RETURNS table (
    seller_id      INT,
    total_orders   BIGINT,
    total_quantity BIGINT,
    total_revenue  NUMERIC
)
as $$
BEGIN
    RETURN QUERY
    with cte as (
        select order_id, sum(quantity) as total_amount, sum(subtotal) as total_revenue
        from order_items_partitioned oi
        where order_date between start_date and end_date
        group by order_id
    )
    select o.seller_id, count(o.order_id) as total_orders, sum(cte.total_amount) as total_quantity, sum(cte.total_revenue) as total_revenue
    from orders_partitioned o
    join cte on o.order_id = cte.order_id
    join products p on cte.product_id = p.product_id
    where o.order_date between start_date AND end_date
    group by seller_id
    order by seller_id;
END;
$$ LANGUAGE plpgsql;

-- 4. Top Products per Brand
-- Identify top products for each brand by quantity sold.
create or REPLACE FUNCTION fn_top_products_per_brand(
    start_date DATE,
    end_date DATE,
    seller_list INT[] DEFAULT NULL
)
RETURNS TABLE (
    brand_id INT,
    brand_name TEXT,
    product_id INT,
    product_name TEXT,
    total_quantity BIGINT,
    total_revenue NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    with cte as (
        select p.brand_id,p.brand_name, oi.product_id, pr.product_name,
            SUM(oi.quantity) AS total_quantity,
            SUM(oi.subtotal) AS total_revenue,
            oi.order_date
        from order_items_partitioned oi
        JOIN products pr ON oi.product_id = pr.product_id
        JOIN brands p ON pr.brand_id = p.brand_id
        where oi.order_date BETWEEN start_date AND end_date
        GROUP BY p.brand_id, p.brand_name, oi.product_id, pr.product_name, oi.order_date
    ),
    ranked AS (
        select brand_id, brand_name, product_id, product_name, total_quantity, total_revenue,
            ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY total_quantity DESC) AS rank
        FROM cte
    )
    SELECT brand_id,brand_name, product_id, product_name, total_quantity, total_revenue
    FROM ranked
    WHERE rank = 1
    ORDER BY brand_id;
END;
$$ LANGUAGE plpgsql;

-- 5. Orders Status Summary
-- Count orders per status (completed, pending, cancelled).
CREATE OR REPLACE FUNCTION fn_order_status_summary(
    start_date DATE,
    end_date   DATE,
    seller_list INT[] DEFAULT NULL,
    category_list INT[] DEFAULT NULL
)
RETURNS TABLE (
    status        TEXT,
    total_orders  BIGINT,
    total_revenue NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    select status, count(order_id) as total_orders 
    from orders_partitioned o
    join products p on o.order_id = p.order_id
    where o.order_date = p.order_date
        AND (seller_list IS NULL OR o.seller_id = ANY(seller_list))
        AND (category_list IS NULL OR p.category_id = ANY(category_list))
    group by status
    order by total_orders;
END;
$$ LANGUAGE plpgsql;