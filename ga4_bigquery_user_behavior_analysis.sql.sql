-- ============================================================
-- Домашнее задание: GA4 BigQuery (events_*)
-- Dataset: bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*
-- ============================================================


-- ============================================================
-- 1) Просмотр REPEATED-полей для выбранного пользователя (1 строка)
-- ============================================================
WITH picked_row AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    event_params,
    user_properties,
    items
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(items) i
    WHERE i.item_name IS NOT NULL
      AND i.item_name != ''
      AND LOWER(i.item_name) != 'not set'
  )
  ORDER BY event_timestamp
  LIMIT 1
)
SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  event_name,
  event_params,
  user_properties,
  items
FROM picked_row;


-- ============================================================
-- 2) Размеры массивов (ARRAY_LENGTH) для этой же строки
-- ============================================================
WITH picked_row AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    event_params,
    user_properties,
    items
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(items) i
    WHERE i.item_name IS NOT NULL
      AND i.item_name != ''
      AND LOWER(i.item_name) != 'not set'
  )
  ORDER BY event_timestamp
  LIMIT 1
)
SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  event_name,
  event_params,
  user_properties,
  items,
  ARRAY_LENGTH(event_params) AS event_params_len,
  ARRAY_LENGTH(user_properties) AS user_properties_len,
  ARRAY_LENGTH(items) AS items_len
FROM picked_row;


-- ============================================================
-- 3) Разворот event_params для этой же строки (UNNEST), сортировка по key
-- ============================================================
WITH picked_row AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_params
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(items) i
    WHERE i.item_name IS NOT NULL
      AND i.item_name != ''
      AND LOWER(i.item_name) != 'not set'
  )
  ORDER BY event_timestamp
  LIMIT 1
)
SELECT
  pr.user_pseudo_id,
  pr.event_name,
  ep.key,
  ep.value.string_value,
  ep.value.int_value,
  ep.value.double_value
FROM picked_row pr
CROSS JOIN UNNEST(pr.event_params) ep
ORDER BY ep.key;


-- ============================================================
-- 4) Частота параметров (key) за 2021 год по UNNEST(event_params)
-- ============================================================
SELECT
  ep.key,
  COUNT(*) AS key_occurrences
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
CROSS JOIN UNNEST(e.event_params) ep
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
GROUP BY ep.key
ORDER BY key_occurrences DESC;


-- ============================================================
-- 5) Разворот items из events_20210131
-- ============================================================
SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  i.item_id,
  i.item_name,
  i.item_category,
  i.price,
  i.quantity
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` e
CROSS JOIN UNNEST(e.items) i;


-- ============================================================
-- 6) Сводная таблица по товарам (items) из events_20210131
--    - появлений в событиях
--    - суммарный quantity
--    - суммарный доход price*quantity
-- ============================================================
SELECT
  i.item_id,
  i.item_name,
  COUNT(*) AS appearances_in_events,
  SUM(i.quantity) AS total_quantity,
  SUM(i.price * i.quantity) AS total_revenue
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` e
CROSS JOIN UNNEST(e.items) i
GROUP BY i.item_id, i.item_name
ORDER BY total_revenue DESC;


-- ============================================================
-- 7) Фильтрация по значению внутри ARRAY items через EXISTS (events_20210131)
--    Вернуть только события, где item_category = 'Apparel'
-- ============================================================
SELECT
  e.user_pseudo_id,
  TIMESTAMP_MICROS(e.event_timestamp) AS event_datetime,
  e.event_name,
  e.items
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` e
WHERE EXISTS (
  SELECT 1
  FROM UNNEST(e.items) i
  WHERE i.item_category = 'Apparel'
);


-- ============================================================
-- 8) Партиции через _TABLE_SUFFIX: по дате
--    - уникальные пользователи
--    - события
--    - события purchase
-- ============================================================
SELECT
  _TABLE_SUFFIX AS event_date,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  COUNT(*) AS events_count,
  COUNTIF(event_name = 'purchase') AS purchase_events_count
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY event_date
ORDER BY event_date;


-- ============================================================
-- 9) Ранжирование пользователей по расходам (items за весь период)
--    + RANK, DENSE_RANK, ROW_NUMBER
--    Top-20
-- ============================================================
WITH user_spend AS (
  SELECT
    e.user_pseudo_id,
    SUM(i.price * i.quantity) AS total_spend
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  CROSS JOIN UNNEST(e.items) i
  GROUP BY e.user_pseudo_id
),
ranked AS (
  SELECT
    user_pseudo_id,
    total_spend,
    RANK() OVER (ORDER BY total_spend DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY total_spend DESC) AS dense_rnk,
    ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS row_num
  FROM user_spend
)
SELECT *
FROM ranked
ORDER BY total_spend DESC
LIMIT 20;


-- ============================================================
-- 10) Нумерация событий в сессии (events_20210131)
--     ga_session_id из event_params
--     найти самый частый event_name как старт сессии
-- ============================================================
WITH base AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    (
      SELECT ep.value.int_value
      FROM UNNEST(event_params) ep
      WHERE ep.key = 'ga_session_id'
      LIMIT 1
    ) AS ga_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
),
numbered AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    event_name,
    event_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
    ) AS event_number_in_session
  FROM base
  WHERE ga_session_id IS NOT NULL
),
session_starts AS (
  SELECT event_name
  FROM numbered
  WHERE event_number_in_session = 1
)
SELECT
  event_name,
  COUNT(*) AS starts_count
FROM session_starts
GROUP BY event_name
ORDER BY starts_count DESC
LIMIT 1;
