-- Shared BI views created on an independent DuckDB snapshot before publication.
create or replace view bi.v_store_review_summary as
select s.store_key, s.canonical_store_id,
       count(r.logical_review_id)::bigint as attached_review_count,
       count(r.review_rating)::bigint as rated_collected_review_count,
       avg(r.review_rating) as collected_review_mean_rating,
       count(r.logical_review_id) filter (where r.review_rating=1)::bigint as one_star_reviews,
       count(r.logical_review_id) filter (where r.review_rating=2)::bigint as two_star_reviews,
       count(r.logical_review_id) filter (where r.review_rating=3)::bigint as three_star_reviews,
       count(r.logical_review_id) filter (where r.review_rating=4)::bigint as four_star_reviews,
       count(r.logical_review_id) filter (where r.review_rating=5)::bigint as five_star_reviews
from dimensional.dim_store s left join dimensional.fact_review r using(store_key)
group by s.store_key,s.canonical_store_id;

create or replace view bi.v_store_overview as
select f.*, s.canonical_store_id,s.store_name,s.address,s.latitude,s.longitude,
       s.is_identity_human_verified,c.category_business_key,c.category_name,
       a.area_business_key,a.area_name,
       case when f.accepted_link_flag=1 then 'Overture + Google' else 'Overture' end as sources,
       r.attached_review_count,r.rated_collected_review_count,r.collected_review_mean_rating,
       case when r.attached_review_count>0 then 1 else 0 end as has_attached_reviews_flag
from dimensional.fact_store_snapshot f
join dimensional.dim_store s using(store_key)
join dimensional.dim_category c using(category_key)
join dimensional.dim_area a using(area_key)
join bi.v_store_review_summary r using(store_key,canonical_store_id);

create or replace view bi.v_review_overview as
select r.*,d.full_date,d.year,d.quarter,d.month,
       s.canonical_store_id,s.store_name,a.area_name,c.category_name,f.verification_status,
       case when r.store_key is null then 'UNMATCHED_CANONICAL' else 'ATTACHED' end as attachment_status
from dimensional.fact_review r
left join dimensional.dim_date d on d.date_key=r.review_date_key
left join dimensional.dim_store s using(store_key)
left join dimensional.fact_store_snapshot f on f.store_key=r.store_key and f.gold_release_id=r.association_gold_release_id
left join dimensional.dim_area a using(area_key)
left join dimensional.dim_category c using(category_key);

-- Audit reporting, not another business fact. Global release denominators are explicit.
-- One row per area, current Gold release and pinned demographic reference.
-- Provider proposal figures remain reference denominators, never certified area/census.
create or replace view bi.v_area_market as
with catalog as (
    select area_key,gold_release_id,sum(store_count)::bigint as observed_store_count,
           sum(accepted_link_flag)::bigint as accepted_link_count,
           sum(source_only_flag)::bigint as source_only_count
    from dimensional.fact_store_snapshot group by area_key,gold_release_id
)
select a.area_business_key,a.area_name,c.gold_release_id,
       coalesce(c.observed_store_count,0)::bigint as observed_store_count,
       coalesce(c.accepted_link_count,0)::bigint as accepted_link_count,
       coalesce(c.source_only_count,0)::bigint as source_only_count,
       a.reference_population,a.reference_area_km2,a.reference_year,
       coalesce(c.observed_store_count,0)/nullif(a.reference_area_km2,0) as observed_stores_per_km2,
       coalesce(c.observed_store_count,0)*1000.0/nullif(a.reference_population,0) as observed_stores_per_1000_residents,
       a.reference_population/nullif(a.reference_area_km2,0) as reference_residents_per_km2,
       a.reference_population_basis,a.reference_area_basis,a.reference_quality_status,
       a.reference_source_version,a.reference_release_id,a.reference_source_url,a.reference_record_url,
       'OBSERVED_CATALOG_NOT_MARKET_COMPLETENESS'::text as catalog_scope
from dimensional.dim_area a left join catalog c using(area_key);

create or replace view bi.v_data_quality as
with stores as (select * from bi.v_store_overview),
     reviews as (select * from dimensional.fact_review),
     audit as (select payload::json as p from dimensional.release_audit)
select 'canonical_records'::text as metric, count(*)::double precision as value,'STORE'::text as grain from stores
union all select 'accepted_links',sum(accepted_link_flag)::double precision,'STORE' from stores
union all select 'source_only_records',sum(source_only_flag)::double precision,'STORE' from stores
union all select 'source_mapping_rows',count(*)::double precision,'SOURCE_MAPPING' from dimensional.bridge_store_source
union all select 'overture_source_mappings',count(*)::double precision,'SOURCE_MAPPING' from dimensional.bridge_store_source where source='overture'
union all select 'google_source_mappings',count(*)::double precision,'SOURCE_MAPPING' from dimensional.bridge_store_source where source='google'
union all select 'nonaccepted_candidate_pairs',(p->>'unresolved_mapping_count')::double precision,'CANDIDATE_PAIR' from audit
union all select 'er_candidate_pairs',(p->>'er_candidate_pairs')::double precision,'CANDIDATE_PAIR' from audit
union all select 'er_review_required_pairs',(p->'er_decision_counts'->>'REVIEW_REQUIRED')::double precision,'CANDIDATE_PAIR' from audit
union all select 'er_non_match_pairs',(p->'er_decision_counts'->>'NON_MATCH')::double precision,'CANDIDATE_PAIR' from audit
union all select 'er_insufficient_data_pairs',(p->'er_decision_counts'->>'INSUFFICIENT_DATA')::double precision,'CANDIDATE_PAIR' from audit
union all select 'logical_reviews',count(*)::double precision,'LOGICAL_REVIEW' from reviews
union all select 'attached_reviews',count(*)::double precision,'LOGICAL_REVIEW' from reviews where store_key is not null
union all select 'unmatched_canonical_reviews',count(*)::double precision,'LOGICAL_REVIEW' from reviews where store_key is null
union all select 'category_provisional',count(*)::double precision,'STORE' from stores where gold_category_evidence_status='PROVISIONAL_SOURCE_AGREEMENT'
union all select 'category_pending',count(*)::double precision,'STORE' from stores where gold_category_evidence_status='PENDING'
union all select 'stores_with_reviews',sum(has_attached_reviews_flag)::double precision,'STORE' from stores
union all select 'review_coverage_pct',100.0*sum(has_attached_reviews_flag)/nullif(count(*),0),'STORE_PERCENT' from stores
union all select 'review_attachment_pct',100.0*count(store_key)/nullif(count(*),0),'LOGICAL_REVIEW_PERCENT' from reviews
union all select 'analysis_category_unknown',count(*)::double precision,'STORE' from stores where category_business_key='unknown'
union all select 'human_verified_links',count(*)::double precision,'STORE' from stores where is_identity_human_verified
union all select 'unknown_review_dates',count(*)::double precision,'LOGICAL_REVIEW' from reviews where review_date_key is null
union all select 'missing_or_invalid_review_ratings',count(*)::double precision,'LOGICAL_REVIEW' from reviews where review_rating is null
union all select 'missing_store_addresses',count(*)::double precision,'STORE' from stores where address is null or trim(address)=''
union all select 'google_places',(p->>'google_places_count')::double precision,'SOURCE_IDENTITY' from audit
union all select 'google_available',(p->>'google_available')::double precision,'SOURCE_IDENTITY' from audit
union all select 'google_incomplete',(p->>'google_incomplete')::double precision,'SOURCE_IDENTITY' from audit
union all select 'google_missing',(p->>'google_missing')::double precision,'SOURCE_IDENTITY' from audit;


create or replace view bi.v_catalog_profile as
-- One existing canonical store. Presentation labels/flags do not change Gold.
select s.*,
       case category_business_key
         when 'restaurant' then 'Nhà hàng / quán ăn'
         when 'cafe_coffee' then 'Cà phê'
         when 'bakery_dessert' then 'Bánh / tráng miệng'
         when 'beverage' then 'Đồ uống'
         when 'fast_food' then 'Đồ ăn nhanh'
         when 'bar' then 'Bar'
         when 'other_food' then 'F&B khác'
         when 'unknown' then 'Chưa rõ loại hình'
         else category_name end as category_label,
       case when google_rating is null then 'Chưa có điểm'
         when google_rating<3 then '1 đến dưới 3'
         when google_rating<4 then '3 đến dưới 4'
         when google_rating<4.5 then '4 đến dưới 4,5'
         else '4,5 đến 5' end as google_rating_band
from bi.v_store_overview s;


create or replace view bi.v_review_profile as
-- One logical review, retaining unassociated reviews and unknown dates/ratings.
select logical_review_id,review_count,review_rating,full_date,attachment_status,
       case when attachment_status='ATTACHED' then 'Đã gắn địa điểm'
            else 'Chưa gắn địa điểm' end as attachment_label
from bi.v_review_overview;


create or replace view bi.v_review_kpi as
-- One current-release summary. Mean is over logical reviews, never Place means.
with reviews as (
    select count(*)::bigint as logical_reviews,
           count(review_rating)::bigint as rated_reviews,
           avg(review_rating) as review_sample_mean,
           count(*) filter (where attachment_status='ATTACHED')::bigint as attached_reviews,
           count(*) filter (where attachment_status<>'ATTACHED')::bigint as unmatched_reviews
    from bi.v_review_overview
), stores as (
    select count(*)::bigint as catalog_records,
           sum(has_attached_reviews_flag)::bigint as stores_with_reviews
    from bi.v_store_overview
)
select reviews.*,stores.*,
       100.0*stores_with_reviews/nullif(catalog_records,0) as store_review_coverage_pct
from reviews cross join stores;
