WITH facebook_data AS (
  SELECT
    'Facebook Ads' AS media_source,
    fbd.ad_date,
    fc.campaign_name,
    fa.adset_name,
    fbd.spend,
    fbd.impressions,
    fbd.clicks,
    fbd.value
  FROM facebook_ads_basic_daily fbd
  LEFT JOIN facebook_adset fa ON fbd.adset_id = fa.adset_id
  LEFT JOIN facebook_campaign fc ON fbd.campaign_id = fc.campaign_id
),
all_data AS (
  SELECT * FROM facebook_data
  UNION ALL
  SELECT
    'Google Ads' AS media_source,
    gabd.ad_date,
    gabd.campaign_name,
    NULL AS adset_name,
    gabd.spend,
    gabd.impressions,
    gabd.clicks,
    gabd.value
  FROM google_ads_basic_daily gabd
)
SELECT
  ad_date,
  media_source,
  campaign_name,
  adset_name,
  SUM(spend) AS total_spend,
  SUM(impressions) AS total_impressions,
  SUM(clicks) AS total_clicks,
  SUM(value) AS total_value
FROM all_data
GROUP BY ad_date, media_source, campaign_name, adset_name
ORDER BY ad_date, media_source, campaign_name;