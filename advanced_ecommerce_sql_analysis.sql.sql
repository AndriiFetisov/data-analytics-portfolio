/* =========================
   Q1 — Аналіз витрат користувачів
   ========================= */
SELECT
  o.user_id,
  SUM(oi.quantity * pr.product_price) AS total_spent
FROM orders_sql_project AS o
JOIN payments_sql_project AS pay
  ON o.order_id = pay.order_id
JOIN order_items_sql_project AS oi
  ON o.order_id = oi.order_id
JOIN products_sql_project AS pr
  ON oi.product_id = pr.product_id
WHERE pay.payment_status = 'Оплачено'
GROUP BY o.user_id
ORDER BY total_spent DESC;


/* =========================
   Q2 — Об'єднання даних з різних каналів (FIX: user_id IS NOT NULL)
   ========================= */
SELECT
  t.user_id,
  t.order_date,
  t.order_id
FROM (
  SELECT
    o.user_id,
    o.order_date,
    o.order_id
  FROM orders_sql_project AS o
  WHERE o.user_id IS NOT NULL

  UNION ALL

  SELECT
    so.user_id,
    so.order_date,
    so.store_order_id AS order_id
  FROM store_orders AS so
  WHERE so.user_id IS NOT NULL
) AS t
ORDER BY t.user_id, t.order_date, t.order_id;


/* =========================
   Q3 — Пошук товарів в обох каналах
   ========================= */
SELECT DISTINCT
  oi.product_id
FROM order_items_sql_project AS oi
WHERE oi.product_id IN (
  SELECT soi.product_id
  FROM store_order_items AS soi
)
ORDER BY oi.product_id;


/* =========================
   Q4 — Визначення активних покупців
   FIX: >2 одиниць КОЖНОГО товару (по product_id) в обох каналах
   ========================= */
WITH online_active AS (
  SELECT
    o.user_id,
    oi.product_id
  FROM orders_sql_project AS o
  JOIN order_items_sql_project AS oi
    ON o.order_id = oi.order_id
  WHERE o.user_id IS NOT NULL
  GROUP BY o.user_id, oi.product_id
  HAVING SUM(oi.quantity) > 2
),
offline_active AS (
  SELECT
    so.user_id,
    soi.product_id
  FROM store_orders AS so
  JOIN store_order_items AS soi
    ON so.store_order_id = soi.store_order_id
  WHERE so.user_id IS NOT NULL
  GROUP BY so.user_id, soi.product_id
  HAVING SUM(soi.quantity) > 2
)
SELECT DISTINCT
  oa.user_id
FROM online_active AS oa
JOIN offline_active AS fa
  ON fa.user_id = oa.user_id
 AND fa.product_id = oa.product_id
ORDER BY oa.user_id;


/* =========================
   Q5 — Розрахунок середнього чека онлайн
   ========================= */
SELECT
  AVG(t.order_total) AS avg_online_check
FROM (
  SELECT
    o.order_id,
    SUM(oi.quantity * pr.product_price) AS order_total
  FROM orders_sql_project AS o
  JOIN payments_sql_project AS pay
    ON o.order_id = pay.order_id
  JOIN order_items_sql_project AS oi
    ON o.order_id = oi.order_id
  JOIN products_sql_project AS pr
    ON oi.product_id = pr.product_id
  WHERE pay.payment_status = 'Оплачено'
  GROUP BY o.order_id
) AS t;


/* =========================
   Q6 — Статистика покупок по каналах
   ========================= */
SELECT
  t.channel,
  t.product_id,
  t.total_quantity,
  t.total_orders
FROM (
  SELECT
    'online' AS channel,
    oi.product_id,
    SUM(oi.quantity) AS total_quantity,
    COUNT(DISTINCT oi.order_id) AS total_orders
  FROM order_items_sql_project AS oi
  GROUP BY oi.product_id

  UNION ALL

  SELECT
    'offline' AS channel,
    soi.product_id,
    SUM(soi.quantity) AS total_quantity,
    COUNT(DISTINCT soi.store_order_id) AS total_orders
  FROM store_order_items AS soi
  GROUP BY soi.product_id
) AS t
ORDER BY t.channel, t.product_id;


/* =========================
   Q7 — Визначення найпопулярніших товарів
   ========================= */
SELECT
  t.product_id,
  COUNT(DISTINCT t.user_id) AS users_count
FROM (
  SELECT
    oi.product_id,
    o.user_id
  FROM order_items_sql_project AS oi
  JOIN orders_sql_project AS o
    ON oi.order_id = o.order_id
  WHERE o.user_id IS NOT NULL

  UNION ALL

  SELECT
    soi.product_id,
    so.user_id
  FROM store_order_items AS soi
  JOIN store_orders AS so
    ON soi.store_order_id = so.store_order_id
  WHERE so.user_id IS NOT NULL
) AS t
GROUP BY t.product_id
ORDER BY users_count DESC
LIMIT 3;


/* =========================
   Q8 — Порівняння середніх чеків (онлайн vs офлайн)
   (у вас была опечатка: oi.quantity * p.product_price — оставляю как pr.product_price)
   ========================= */
SELECT
  t.channel,
  t.avg_check
FROM (
  SELECT
    'online' AS channel,
    AVG(online_orders.order_total) AS avg_check
  FROM (
    SELECT
      o.order_id,
      SUM(oi.quantity * pr.product_price) AS order_total
    FROM orders_sql_project AS o
    JOIN payments_sql_project AS pay
      ON o.order_id = pay.order_id
    JOIN order_items_sql_project AS oi
      ON o.order_id = oi.order_id
    JOIN products_sql_project AS pr
      ON oi.product_id = pr.product_id
    WHERE pay.payment_status = 'Оплачено'
    GROUP BY o.order_id
  ) AS online_orders

  UNION ALL

  SELECT
    'offline' AS channel,
    AVG(offline_orders.order_total) AS avg_check
  FROM (
    SELECT
      so.store_order_id,
      SUM(soi.quantity * pr.product_price) AS order_total
    FROM store_orders AS so
    JOIN store_order_items AS soi
      ON so.store_order_id = soi.store_order_id
    JOIN products_sql_project AS pr
      ON soi.product_id = pr.product_id
    GROUP BY so.store_order_id
  ) AS offline_orders
) AS t
ORDER BY t.avg_check;


/* =========================
   Q9 — Пошук клієнтів з дорогими онлайн-покупками
   FIX: сравниваем ЦЕНУ товара (product_price), а не order_total
   ========================= */
WITH offline_avg_check AS (
  SELECT AVG(x.order_total) AS avg_offline_check
  FROM (
    SELECT
      so.store_order_id,
      SUM(soi.quantity * pr.product_price) AS order_total
    FROM store_orders AS so
    JOIN store_order_items AS soi
      ON so.store_order_id = soi.store_order_id
    JOIN products_sql_project AS pr
      ON soi.product_id = pr.product_id
    GROUP BY so.store_order_id
  ) AS x
),
online_items AS (
  SELECT
    o.user_id,
    oi.product_id,
    pr.product_price
  FROM orders_sql_project AS o
  JOIN payments_sql_project AS pay
    ON o.order_id = pay.order_id
  JOIN order_items_sql_project AS oi
    ON o.order_id = oi.order_id
  JOIN products_sql_project AS pr
    ON oi.product_id = pr.product_id
  WHERE pay.payment_status = 'Оплачено'
    AND o.user_id IS NOT NULL
)
SELECT DISTINCT
  oi.user_id
FROM online_items AS oi
CROSS JOIN offline_avg_check AS oac
WHERE oi.product_price > oac.avg_offline_check
ORDER BY oi.user_id;


/* =========================
   Q10 — Аналіз великих сум замовлень по місяцях
   FIX: avg_check + кількість покупців із чеком вище середнього (по місяцю)
   ========================= */
WITH all_paid_orders AS (
  -- online paid
  SELECT
    o.user_id,
    o.order_date,
    o.order_id AS any_order_id,
    SUM(oi.quantity * pr.product_price) AS order_total
  FROM orders_sql_project AS o
  JOIN payments_sql_project AS pay
    ON o.order_id = pay.order_id
   AND pay.payment_status = 'Оплачено'
  JOIN order_items_sql_project AS oi
    ON o.order_id = oi.order_id
  JOIN products_sql_project AS pr
    ON oi.product_id = pr.product_id
  WHERE o.user_id IS NOT NULL
  GROUP BY o.user_id, o.order_date, o.order_id

  UNION ALL

  -- offline
  SELECT
    so.user_id,
    so.order_date,
    so.store_order_id AS any_order_id,
    SUM(soi.quantity * pr.product_price) AS order_total
  FROM store_orders AS so
  JOIN store_order_items AS soi
    ON so.store_order_id = soi.store_order_id
  JOIN products_sql_project AS pr
    ON soi.product_id = pr.product_id
  WHERE so.user_id IS NOT NULL
  GROUP BY so.user_id, so.order_date, so.store_order_id
),
orders_by_month AS (
  SELECT
    DATE_TRUNC('month', order_date)::date AS month,
    user_id,
    any_order_id,
    order_total
  FROM all_paid_orders
),
monthly_avg AS (
  SELECT
    month,
    AVG(order_total) AS avg_check
  FROM orders_by_month
  GROUP BY month
)
SELECT
  obm.month,
  ma.avg_check,
  COUNT(DISTINCT CASE WHEN obm.order_total > ma.avg_check THEN obm.user_id END) AS buyers_above_avg
FROM orders_by_month AS obm
JOIN monthly_avg AS ma
  ON ma.month = obm.month
GROUP BY obm.month, ma.avg_check
ORDER BY obm.month;
