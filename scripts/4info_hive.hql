add jar s3://adhaven-mrscripts-prd/java/serde/json-serde-1.3-jar-with-dependencies.jar;
add jar s3://adhaven-mrscripts-prd/java/brickhouse/brickhouse-0.6.0.jar;
create temporary function collect as 'brickhouse.udf.collect.CollectUDAF';

set hive.map.aggr.hash.percentmemory = 0.125;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set mapreduce.map.java.opts=-Xmx1024m;
set mapreduce.reduce.java.opts=-Xmx1024m;

set hive.exec.parallel=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;
set mapreduce.input.fileinputformat.split.maxsize=4000000;
set mapreduce.input.fileinputformat.split.minsize=1000000;

set hive.auto.convert.join=false;

DROP TABLE adhaven_request_log;
CREATE EXTERNAL TABLE IF NOT EXISTS adhaven_request_log
(
request_ts STRING,
placement_id BIGINT,
media_type_key STRING,
ip STRING,
latitude DOUBLE,
longitude DOUBLE,
seller_member_id STRING,
zip STRING,
dma STRING,
state STRING,
gender STRING,
age_group_range STRING,
consumer_id STRING,
tag_id STRING,
zip_from_publisher STRING,
viewability_score STRING,
viewability_source STRING,
income STRING,
country_abbr STRING,
response_type SMALLINT,
iswifi_y_n STRING,
device_browser STRING,
device_os STRING,
device_model STRING,
device_os_ver STRING,
device_pointing_method STRING,
street_name STRING,
street_number STRING,
radius_km DOUBLE,
keyword_list STRING,
user_id STRING,
page_url STRING,
device_brand STRING,
carrier STRING,
isp STRING,
date_of_request STRING,
hour_of_request STRING,
hour_key STRING,
date_key STRING,
request_id STRING,
total_adServe_time BIGINT,
consumer_lookup_time BIGINT,
consumer_latLng2Loc_lookup_time BIGINT,
consumer_IP2Loc_lookup_time BIGINT,
time_zone_offset BIGINT,
consumer_id_method BIGINT,
ate STRING,
device_type STRING,
bid_price_floor DOUBLE,
ad_width INT,
ad_height INT,
bid_id STRING,
location_type INT,
app_id STRING,
site_id STRING,
mraid_ver INT,
marketing_name STRING,
device_brand_request STRING,
device_model_request STRING,
request_category STRING,
request_source INT,
home_away INT,
ISO_lang STRING,
video_max_duration INT,
video_min_duration INT,
clear_device_id STRING,
assigned_HH_id STRING,
assigned_HH_source INT,
assigned_HH_DMA STRING,
boost_flag STRING,
app_name STRING,
site_name STRING,
publisher_id STRING,
publisher_name STRING,
app_bundle_id STRING,
store_url STRING,
secure STRING,
video_protocols STRING,
video_skippable INT,
video_streaming_position INT,
ip_home_away INT,
reason_not_processed INT
)
COMMENT 'Request Log'
PARTITIONED by (dt STRING, svr STRING, hh STRING, min STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${S3_INPUT_BUCKET}/${S3_REQUEST_LOG}/';

ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0201', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0202', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0203', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0204', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0205', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0206', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0207', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0208', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0209', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0210', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0211', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0212', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0213', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0214', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0215', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0216', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0217', hh='${HH}', min='00'); 
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0218', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0219', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0220', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0221', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0222', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0223', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0224', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0225', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0226', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0227', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0228', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0229', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0230', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0231', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0232', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0233', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0234', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0235', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0236', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0237', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0238', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0239', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0240', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0241', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0242', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0243', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0244', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0245', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0246', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0247', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0248', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0249', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0250', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0251', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0252', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0253', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0254', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0255', hh='${HH}', min='00');

ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0256', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0257', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0258', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0259', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0260', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0261', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0262', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0263', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0264', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0265', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0266', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0267', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0268', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0269', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0270', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0271', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0272', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0273', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0274', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0275', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0276', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0277', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0278', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0279', hh='${HH}', min='00');
ALTER TABLE adhaven_request_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0280', hh='${HH}', min='00');

--Create Adhaven Event Log

DROP TABLE IF EXISTS adhaven_event_log;

CREATE EXTERNAL TABLE IF NOT EXISTS adhaven_event_log
(
  event_ts STRING,
  delivered_receipt_id STRING,
  gesture_ad_id INT,
  gesture_type STRING,
  ad_id INT,
  ad_group_id INT,
  ad_group_payment_type STRING,
  consumer_id STRING,
  request_type STRING,
  placement_id BIGINT,
  cpu DOUBLE,
  conversion_id INT,
  ad_group_client_impression_tracked STRING,
  banner_id INT,
  banner_details_id INT,
  video_id INT,
  video_details_id INT,
  html5_id INT,
  html5_details_id INT,
  date_of_event STRING, 
  hour_of_event STRING,
  hour_key STRING,
  date_key STRING,
  request_id STRING,
  allocation DOUBLE,
  rtb_bid_price DOUBLE,
  rtb_auction_price DOUBLE,
  rtb_auction_unit INT,
  rtb_bid_id STRING,
  rtb_bid_impression_id STRING,
  is_behavioral_targeted INT,
  assigned_hhid STRING,
  assigned_hh_dma STRING,
  parent_adgroup_id INT, 
  external_hhid STRING,
  matched_deal_id STRING,
  matched_datasource_segments STRING,
  matched_device_segments STRING,
  referer_url STRING,
  viewability_tag_added STRING
  
) 
COMMENT 'Event Log' 
PARTITIONED by (dt STRING, svr STRING, hh STRING, min STRING)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${S3_INPUT_BUCKET}/${S3_EVENT_LOG}/';

--Add partition to adhaven event log  

ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0101', hh='${HH}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0102', hh='${HH}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0103', hh='${HH}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0104', hh='${HH}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0105', hh='${HH}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT}', svr='0106', hh='${HH}', min='00'); 

ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0101', hh='${HH_NEXT}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0102', hh='${HH_NEXT}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0103', hh='${HH_NEXT}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0104', hh='${HH_NEXT}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0105', hh='${HH_NEXT}', min='00'); 
ALTER TABLE adhaven_event_log ADD IF NOT EXISTS PARTITION (dt='${DT_NEXT}', svr='0106', hh='${HH_NEXT}', min='00'); 


DROP TABLE IF EXISTS CONSOLIDATE_REQUEST_EVENT_LOG;

CREATE EXTERNAL TABLE CONSOLIDATE_REQUEST_EVENT_LOG(
request_ts STRING,
placement_id BIGINT,
media_type_key STRING,
ip STRING,
latitude DOUBLE,
longitude DOUBLE,
zip STRING,
dma STRING,
state STRING,
consumer_id STRING,
country_abbr STRING,
iswifi_y_n STRING,
device_os STRING,
device_model STRING,
device_os_ver STRING,
street_name STRING,
device_brand STRING,
carrier STRING,
isp STRING,
date_of_request STRING,
hour_of_request STRING,
request_id STRING,
time_zone_offset BIGINT,
consumer_id_method BIGINT,
ate STRING,
device_type STRING,
bid_price_floor DOUBLE,
ad_width INT,
ad_height INT,
location_type INT,
app_id STRING,
site_id STRING,
mraid_ver INT,
marketing_name STRING,
device_brand_request STRING,
device_model_request STRING,
request_category STRING,
request_source INT,
home_away INT,
ISO_lang STRING,
video_max_duration INT,
video_min_duration INT,
assigned_HH_id STRING,
assigned_HH_source INT,
assigned_HH_DMA STRING,
boost_flag STRING,
app_name STRING,
site_name STRING,
publisher_id STRING,
publisher_name STRING,
app_bundle_id STRING,
store_url STRING,
secure STRING,
video_protocols STRING,
video_skippable INT,
video_streaming_position INT,
ip_home_away INT,
reason_not_processed INT,
seller_member_id STRING,
tag_id STRING,
viewability_score STRING,
viewability_source STRING,
events array<struct<
  event_ts: STRING,
  gesture_type: STRING,
  ad_group_id: INT,
  ad_group_payment_type: STRING,
  cpu: DOUBLE,
  conversion_id: INT,
  banner_id: INT,
  banner_details_id: INT,
  video_id: INT,
  video_details_id: INT,
  native_id: INT,
  native_details_id: INT,
  date_of_event: STRING, 
  hour_of_event: STRING,
  rtb_bid_price: DOUBLE,
  rtb_auction_price: DOUBLE,
  rtb_auction_unit: INT,
  rtb_bid_id: STRING,
  rtb_bid_impression_id: STRING,
  is_behavioral_targeted: INT,
  parent_adgroup_id: INT, 
  external_hhid: STRING,
  matched_deal_id: STRING,
  matched_datasource_segments: STRING,
  matched_device_segments: STRING,
  referer_url: STRING,
  viewability_tag_added: STRING
  >>
)
COMMENT 'IMPRESSION ROLLUP'
PARTITIONED by (y STRING, m STRING, d STRING, h STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '${S3_INPUT_BUCKET}/${S3_CONSOLIDATED_REQ_EVENT_LOG}/';


SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;
SET mapred.reduce.tasks=10;


INSERT OVERWRITE TABLE CONSOLIDATE_REQUEST_EVENT_LOG PARTITION(y='${y}', m='${m}', d='${d}', h='${HH}')
select r.request_ts,r.placement_id ,r.media_type_key, r.ip,latitude , longitude ,zip,dma,state, r.consumer_id,country_abbr,iswifi_y_n,device_os,
device_model,device_os_ver,street_name,device_brand,carrier,isp,date_of_request,hour_of_request,r.request_id,time_zone_offset ,
consumer_id_method ,ate,device_type,bid_price_floor,ad_width ,ad_height ,location_type ,app_id ,site_id ,mraid_ver ,marketing_name,device_brand_request,device_model_request,
request_category,request_source ,home_away ,ISO_lang,video_max_duration,video_min_duration,assigned_HH_id,assigned_HH_source,assigned_HH_DMA,boost_flag,
app_name,site_name,publisher_id,publisher_name,app_bundle_id,store_url,secure,video_protocols,video_skippable,video_streaming_position, ip_home_away, reason_not_processed,
r.seller_member_id,r.tag_id,r.viewability_score,r.viewability_source,
event_data.events
from 
(
select  request_id,
 collect(named_struct( 'event_ts',e.event_ts, 'gesture_type',e.gesture_type, 'ad_group_id',e.ad_group_id ,  'ad_group_payment_type',e.ad_group_payment_type,  
  'cpu',e.cpu ,  'conversion_id',e.conversion_id ,  'banner_id',e.banner_id ,  'banner_details_id', e.banner_details_id , 'video_id', e.video_id ,  'video_details_id',e.video_details_id ,  
  'native_id',e.html5_id ,  'native_details_id',e.html5_details_id ,  'date_of_event',e.date_of_event,   'hour_of_event',e.hour_of_event, 
  'rtb_bid_price',e.rtb_bid_price , 'rtb_auction_price', e.rtb_auction_price ,  'rtb_auction_unit',e.rtb_auction_unit ,  'rtb_bid_id',e.rtb_bid_id,  'rtb_bid_impression_id',e.rtb_bid_impression_id,  
  'is_behavioral_targeted',e.is_behavioral_targeted, 'parent_adgroup_id', e.parent_adgroup_id,'external_hhid', e.external_hhid,  'matched_deal_id', e.matched_deal_id,
  'matched_datasource_segments', e.matched_datasource_segments,'matched_device_segments', e.matched_device_segments,'referer_url',e.referer_url, 'viewability_tag_added',e.viewability_tag_added)) as events from adhaven_event_log e
 group by request_id
) event_data
join adhaven_request_log r 
on event_data.request_id=r.request_id
where r.request_id <> '' and (boost_flag='0' or boost_flag='1');


-- COPY THE SAME DATA TO METAMARKETS
DROP TABLE IF EXISTS MM_CONSOLIDATE_REQUEST_EVENT_LOG;

CREATE EXTERNAL TABLE MM_CONSOLIDATE_REQUEST_EVENT_LOG(
request_ts STRING,
placement_id BIGINT,
media_type_key STRING,
ip STRING,
latitude DOUBLE,
longitude DOUBLE,
zip STRING,
dma STRING,
state STRING,
consumer_id STRING,
country_abbr STRING,
iswifi_y_n STRING,
device_os STRING,
device_model STRING,
device_os_ver STRING,
street_name STRING,
device_brand STRING,
carrier STRING,
isp STRING,
date_of_request STRING,
hour_of_request STRING,
request_id STRING,
time_zone_offset BIGINT,
consumer_id_method BIGINT,
ate STRING,
device_type STRING,
bid_price_floor DOUBLE,
ad_width INT,
ad_height INT,
location_type INT,
app_id STRING,
site_id STRING,
mraid_ver INT,
marketing_name STRING,
device_brand_request STRING,
device_model_request STRING,
request_category STRING,
request_source INT,
home_away INT,
ISO_lang STRING,
video_max_duration INT,
video_min_duration INT,
assigned_HH_id STRING,
assigned_HH_source INT,
assigned_HH_DMA STRING,
boost_flag STRING,
app_name STRING,
site_name STRING,
publisher_id STRING,
publisher_name STRING,
app_bundle_id STRING,
store_url STRING,
secure STRING,
video_protocols STRING,
video_skippable INT,
video_streaming_position INT,
ip_home_away INT,
reason_not_processed INT,
seller_member_id STRING,
tag_id STRING,
viewability_score STRING,
viewability_source STRING,
events array<struct<
  event_ts: STRING,
  gesture_type: STRING,
  ad_group_id: INT,
  ad_group_payment_type: STRING,
  cpu: DOUBLE,
  conversion_id: INT,
  banner_id: INT,
  banner_details_id: INT,
  video_id: INT,
  video_details_id: INT,
  native_id: INT,
  native_details_id: INT,
  date_of_event: STRING,
  hour_of_event: STRING,
  rtb_bid_price: DOUBLE,
  rtb_auction_price: DOUBLE,
  rtb_auction_unit: INT,
  rtb_bid_id: STRING,
  rtb_bid_impression_id: STRING,
  is_behavioral_targeted: INT,
  parent_adgroup_id: INT,
  external_hhid: STRING,
  matched_deal_id: STRING,
  matched_datasource_segments: STRING,
  matched_device_segments: STRING,
  referer_url: STRING,
  viewability_tag_added: STRING
  >>
)
COMMENT 'IMPRESSION ROLLUP'
PARTITIONED by (y STRING, m STRING, d STRING, h STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '${S3_MM_LOG_BUCKET}/${S3_MM_CONSOLIDATE_REQ_EVENT_LOG}/';

INSERT OVERWRITE TABLE MM_CONSOLIDATE_REQUEST_EVENT_LOG PARTITION(y='${y}', m='${m}', d='${d}', h='${HH}')
SELECT request_ts, placement_id, media_type_key, ip,latitude , longitude ,zip,dma,state, consumer_id,country_abbr,iswifi_y_n,device_os,
       device_model,device_os_ver,street_name,device_brand,carrier,isp,date_of_request,hour_of_request, request_id, time_zone_offset ,
       consumer_id_method ,ate,device_type,bid_price_floor,ad_width ,ad_height ,location_type ,app_id ,site_id ,mraid_ver ,marketing_name,device_brand_request,device_model_request,
       request_category,request_source ,home_away ,ISO_lang,video_max_duration,video_min_duration,assigned_HH_id,assigned_HH_source,assigned_HH_DMA,boost_flag,
       app_name,site_name,publisher_id,publisher_name,app_bundle_id,store_url,secure, video_protocols, video_skippable, video_streaming_position, ip_home_away, reason_not_processed,seller_member_id,
       tag_id,viewability_score,viewability_source,
       events
FROM CONSOLIDATE_REQUEST_EVENT_LOG;
