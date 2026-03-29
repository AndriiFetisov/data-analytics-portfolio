WITH
-- 1) Clean users table: parse signup_datetime (text) -> signup_date (date)
users_clean AS (
  SELECT
    u.user_id,
    u.promo_signup_flag,
    u.signup_source,
    u.signup_device,

    -- normalize date part: trim, take part before space (date), replace . and / with -
    regexp_replace(split_part(trim(u.signup_datetime), ' ', 1), '[./]', '-', 'g') AS signup_date_norm,

    -- parse normalized date string into date
    CASE
      WHEN regexp_replace(split_part(trim(u.signup_datetime), ' ', 1), '[./]', '-', 'g')
           ~ '^\d{1,2}-\d{1,2}-\d{4}$'
        THEN to_date(
          regexp_replace(split_part(trim(u.signup_datetime), ' ', 1), '[./]', '-', 'g'),
          'DD-MM-YYYY'
        )
      WHEN regexp_replace(split_part(trim(u.signup_datetime), ' ', 1), '[./]', '-', 'g')
           ~ '^\d{1,2}-\d{1,2}-\d{2}$'
        THEN to_date(
          regexp_replace(split_part(trim(u.signup_datetime), ' ', 1), '[./]', '-', 'g'),
          'DD-MM-YY'
        )
      ELSE NULL
    END AS signup_date
  FROM cohort_users_raw u
),

-- 2) Clean events table: parse event_datetime (text) -> event_date (date)
events_clean AS (
  SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    e.revenue,

    regexp_replace(split_part(trim(e.event_datetime), ' ', 1), '[./]', '-', 'g') AS event_date_norm,

    CASE
      WHEN regexp_replace(split_part(trim(e.event_datetime), ' ', 1), '[./]', '-', 'g')
           ~ '^\d{1,2}-\d{1,2}-\d{4}$'
        THEN to_date(
          regexp_replace(split_part(trim(e.event_datetime), ' ', 1), '[./]', '-', 'g'),
          'DD-MM-YYYY'
        )
      WHEN regexp_replace(split_part(trim(e.event_datetime), ' ', 1), '[./]', '-', 'g')
           ~ '^\d{1,2}-\d{1,2}-\d{2}$'
        THEN to_date(
          regexp_replace(split_part(trim(e.event_datetime), ' ', 1), '[./]', '-', 'g'),
          'DD-MM-YY'
        )
      ELSE NULL
    END AS event_date
  FROM cohort_events_raw e
),

-- 3) Join + derive cohort/activity months and month_offset, apply required filters
joined_clean AS (
  SELECT
    u.user_id,
    u.promo_signup_flag,

    -- cohort month (month of signup)
    date_trunc('month', u.signup_date)::date AS cohort_month,

    -- activity month (month of event)
    date_trunc('month', e.event_date)::date AS activity_month,

    -- month offset between activity_month and cohort_month
    (
      (EXTRACT(YEAR FROM date_trunc('month', e.event_date))::int - EXTRACT(YEAR FROM date_trunc('month', u.signup_date))::int) * 12
      + (EXTRACT(MONTH FROM date_trunc('month', e.event_date))::int - EXTRACT(MONTH FROM date_trunc('month', u.signup_date))::int)
    ) AS month_offset

  FROM users_clean u
  JOIN events_clean e
    ON e.user_id = u.user_id

  WHERE
    -- remove users without signup date
    u.signup_date IS NOT NULL
    -- remove events without event date
    AND e.event_date IS NOT NULL
    -- remove events with NULL type
    AND e.event_type IS NOT NULL
    -- remove test events
    AND e.event_type <> 'test_event'
    -- observation window: activity months Jan–Jun 2025
    AND date_trunc('month', e.event_date)::date BETWEEN DATE '2025-01-01' AND DATE '2025-06-01'
)

-- 4) Final aggregation for cohort table
SELECT
  promo_signup_flag,
  cohort_month,
  month_offset,
  COUNT(DISTINCT user_id) AS users_total
FROM joined_clean
GROUP BY
  promo_signup_flag,
  cohort_month,
  month_offset
ORDER BY
  promo_signup_flag,
  cohort_month,
  month_offset;
