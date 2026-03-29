with google_facebook_stat as(
select ad_date, 'Facebook' as media_source, spend, impressions, reach, clicks, leads, value
from facebook_ads_basic_daily
where leads >0
union all
select ad_date, 'Google' as media_source, spend, impressions, reach, clicks, leads, value
from google_ads_basic_daily),
sum_by_two_campaigns as(
select ad_date, media_source,
sum(spend) as total_spend,
sum(impressions) as total_impressions,
sum(reach) as total_reach,
sum(clicks) as total_clicks,
sum(leads) as total_leads,
sum(value) as total_value
from google_facebook_stat
where ad_date is not null
group by ad_date , media_source)
select * from sum_by_two_campaigns
order by ad_date, media_source;






