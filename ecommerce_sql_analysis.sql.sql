--- Q1 ---
SELECT
    user_city,
    COUNT(DISTINCT user_id) AS users_cnt
FROM users_sql_project
GROUP BY user_city
ORDER BY users_cnt DESC;



--- Q2 ---
SELECT
    order_id
FROM order_items_sql_project
GROUP BY order_id
ORDER BY SUM(quantity) DESC
LIMIT 1;



--- Q3 ---
SELECT
    COUNT(DISTINCT order_id) AS orders_cnt
FROM payments_sql_project
WHERE payment_method IN ('Картка', 'Банківський переказ')
  AND payment_status <> 'Відхилено';



--- Q4 ---
SELECT
    user_id,
    COUNT(*) AS orders_cnt
FROM orders_sql_project
GROUP BY user_id
HAVING COUNT(*) >= 5
ORDER BY orders_cnt DESC;



--- Q5 ---
SELECT
    SUM(oi.quantity) AS total_quantity,
    COUNT(DISTINCT oi.order_id) AS orders_cnt
FROM order_items_sql_project AS oi
JOIN products_sql_project AS p
    ON oi.product_id = p.product_id
WHERE p.product_brand = 'DigitalUA';



--- Q6 ---
SELECT
    order_id,
    tracking_number,
    COALESCE(TO_CHAR(shipment_date, 'YYYY-MM-DD'), 'в роботі') AS shipment_status,
    delivery_date
FROM shipments_sql_project
ORDER BY order_id;



--- Q7 ---
SELECT
    CASE
        WHEN user_age < 25 THEN 'молоді'
        WHEN user_age BETWEEN 25 AND 44 THEN 'середній вік'
        ELSE 'старший вік'
    END AS age_group,
    COUNT(*) AS users_cnt
FROM users_sql_project
GROUP BY
    CASE
        WHEN user_age < 25 THEN 'молоді'
        WHEN user_age BETWEEN 25 AND 44 THEN 'середній вік'
        ELSE 'старший вік'
    END
ORDER BY users_cnt DESC;



--- Q8 ---
SELECT
    user_city,
    COUNT(DISTINCT loyalty_status) AS loyalty_status_cnt
FROM users_sql_project
GROUP BY user_city
HAVING COUNT(DISTINCT loyalty_status) >= 3
ORDER BY loyalty_status_cnt ASC;



--- Q9 ---
SELECT
    *
FROM users_sql_project
WHERE user_email LIKE '%@gmail.com';



--- Q10 ---
SELECT
    courier,
    AVG(delivery_date - shipment_date) AS avg_delivery_days
FROM shipments_sql_project
WHERE shipment_date IS NOT NULL
  AND delivery_date IS NOT NULL
GROUP BY courier
ORDER BY avg_delivery_days ASC;