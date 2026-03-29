select ad_date, campaign_id, value, spend,
round (100*cast(value as numeric)/cast(spend as numeric), 2) as romi
from facebook_ads_basic_daily
where spend>0;

select ad_date, campaign_id,
sum(clicks) as total_clicks,
sum(impressions) as total_impressions,
sum(value) as total_value,
sum(spend) as total_spend,
round(100*cast(sum(value) as numeric)/cast(sum(spend) as numeric)/cast (sum(spend) as numeric), 2) as romi,
sum(spend)/sum(clicks) as CPC,
sum(spend)/sum(impressions) as CPM,
sum(impressions)/sum(clicks) as CTR
from facebook_ads_basic_daily
where spend>0 and clicks>0
group by ad_date, campaign_id;
