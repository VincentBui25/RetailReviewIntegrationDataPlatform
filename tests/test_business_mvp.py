import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Polygon

from fnb_platform.gold import business_mvp as mvp


def facts_fixture():
    stores=[dict(store_id='a',identity_status='AUTO_ACCEPTED',category_validation_status='PROVISIONAL_SOURCE_AGREEMENT'),
        dict(store_id='b',identity_status='SOURCE_ONLY',category_validation_status='PENDING')]
    mappings=[dict(store_id=k,source='overture',source_id_kind='overture_id',source_id=k) for k in ('a','b')]
    mappings.append(dict(store_id='a',source='google',source_id_kind='data_id',source_id='ga'))
    links=[dict(overture_id='a',google_id_kind='data_id',google_identity_value='ga',effective_status='AUTO_ACCEPTED'),
        dict(overture_id='b',google_id_kind='data_id',google_identity_value='gb',effective_status='PENDING')]
    attached=[dict(source='google_maps',source_review_id='r1',store_id='a')]
    unmatched=[dict(source='google_maps',source_review_id='r2')]
    metrics=dict(canonical_store_count=2,source_mapping_rows=3,accepted_mapping_count=1,
        unresolved_mapping_count=1,source_only_store_count=1,attached_reviews=1,unmatched_reviews=1,current_logical_reviews=2)
    return stores,mappings,links,attached,unmatched,metrics


def test_gold_facts_keep_independent_grains_and_original_category_status():
    facts=mvp.gold_facts(*facts_fixture())
    assert facts['source_contribution']==dict(overture=2,google=1)
    assert facts['accepted_links']==1 and facts['unresolved_links']==1 and facts['source_only_stores']==1
    assert facts['gold_category_status_counts']==dict(PROVISIONAL_SOURCE_AGREEMENT=1,PENDING=1)


@pytest.mark.parametrize('damage',['duplicate_source_mapping','review_fanout','wrong_summary'])
def test_gold_facts_fail_closed_on_duplicates_or_reconciliation_mismatch(damage):
    stores,mappings,links,attached,unmatched,metrics=facts_fixture()
    if damage=='duplicate_source_mapping':mappings.append(dict(mappings[0]))
    if damage=='review_fanout':unmatched.append(dict(attached[0]))
    if damage=='wrong_summary':metrics['accepted_mapping_count']=2
    with pytest.raises(ValueError):mvp.gold_facts(stores,mappings,links,attached,unmatched,metrics)


def test_spatial_membership_includes_edge_and_preserves_outside_and_missing():
    rows=[dict(store_id='inside',latitude=.5,longitude=.5),
          dict(store_id='edge',latitude=0.,longitude=.5),
          dict(store_id='outside',latitude=2.,longitude=2.),
          dict(store_id='missing',latitude=None,longitude=None)]
    polygon=Polygon([(0,0),(1,0),(1,1),(0,1)])
    result=mvp.assign_area(rows,polygon,dict(admin_area_id='fixture')).to_pylist()
    assert [r['spatial_status'] for r in result]==['INSIDE_BOUNDARY','INSIDE_BOUNDARY','OUTSIDE_BOUNDARY','MISSING_COORDINATES']
    assert len({r['store_id'] for r in result})==4


def test_missing_boundary_does_not_invent_area_or_density(tmp_path):
    shape,metadata=mvp.boundary(tmp_path/'missing.json',tmp_path/'metadata.json')
    assert shape is None and metadata['density_status']=='NO_TRUSTED_AREA_DENOMINATOR'


def test_real_dbt_projection_is_conservative_and_preserves_store_grain(tmp_path):
    stores=[];normal=[]
    cases=[('single','SOURCE_ONLY','unknown',['restaurant'],[],None),
           ('disagree','AUTO_ACCEPTED','unknown',['restaurant'],[],'SUBCATEGORY_DIFFERENCE'),
           ('known','AUTO_ACCEPTED','restaurant',['restaurant'],[],'CONSISTENT'),
           ('dq','SOURCE_ONLY','unknown',['restaurant'],['WARNING'],None),
           ('multi','SOURCE_ONLY','unknown',['restaurant','bar'],[],None)]
    for key,status,category,groups,flags,relation in cases:
        stores.append(dict(store_id=key,overture_id='ov-'+key,canonical_category=category,
            identity_status=status,google_identity_value='google-'+key if status=='AUTO_ACCEPTED' else None,
            latitude=10.,longitude=106.,is_identity_human_verified=False,category_relation=relation,
            category_validation_status='PENDING' if category=='unknown' else 'PROVISIONAL_SOURCE_AGREEMENT'))
        normal.append(dict(source='overture',source_id='ov-'+key,normalized_categories=groups,
            dq_flags=flags,source_provenance_json=json.dumps(dict(fixture=key))))
    area=mvp.assign_area(stores,None,{})
    paths={}
    for name,table in [('gold_stores',pa.Table.from_pylist(stores)),
            ('category_normalization',pa.Table.from_pylist(normal)),('store_area',area)]:
        path=tmp_path/(name+'.parquet');pq.write_table(table,path);paths[name+'_path']=str(path)
    env=dict(os.environ,FNB_DBT_DATABASE=str(tmp_path/'test.duckdb'),DBT_SEND_ANONYMOUS_USAGE_STATS='false')
    command=[sys.executable,'-B','-c','from dbt.cli.main import cli; cli()','build',
        '--project-dir',str(mvp.PROJECT),'--profiles-dir',str(mvp.PROJECT),
        '--target-path',str(tmp_path/'target'),'--log-path',str(tmp_path/'logs'),'--vars',json.dumps(paths)]
    result=subprocess.run(command,env=env,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,(result.stdout+result.stderr)[-3500:]
    con=duckdb.connect(str(tmp_path/'test.duckdb'),read_only=True)
    try:
        rows=con.execute('select store_id,analysis_category,analysis_category_basis from business_analytics.mart_store_business').fetchall()
        assert {r[0]:r[1] for r in rows}==dict(single='restaurant',disagree='unknown',known='restaurant',dq='unknown',multi='unknown')
        assert next(r for r in rows if r[0]=='single')[2]=='OVERTURE_SOURCE_PROVISIONAL'
        assert con.execute('select sum(store_count) from business_analytics.mart_area_catalog').fetchone()[0]==5
        assert con.execute('select count(*) from business_analytics.mart_area_catalog where trusted_area_km2 is not null or observed_stores_per_km2 is not null').fetchone()[0]==0
    finally:
        con.close()
