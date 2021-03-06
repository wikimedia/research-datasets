
#%%
pv_actor = (spark.sql('SELECT * FROM wmf.pageview_actor'))
pv_actor.printSchema()

#%%

time_cols = ['year', 'month', 'hour', 'hour']
geo_cols = [
    F.col('geocoded_data').getItem('continent').alias('continent'),
    F.col('geocoded_data').getItem('country').alias('country'),
    F.col('geocoded_data').getItem('subdivision').alias('subdivision')
]

trace_cols = [
    F.col('pageview_info').getItem('project').alias('project'), 
    F.col('pageview_info').getItem('page_title').alias('trace'),
]
            
page_views = (pv_actor
    .where(F.col('year')==2020)
    .where(F.col('month')==12)
    .where(F.col('hour')==12)
    # .where(F.col('hour')==12)
    .where(F.col('normalized_host.project_family')=='wikipedia')
    .where(F.col('agent_type')=='user')
    .where(F.col('access_method')!='mobile app')
    .where(F.col('namespace_id').isin([0, 1]))
    .select(time_cols + geo_cols + trace_cols)  
    )
# %%
# minute = F.udf(lambda ts: ts.minute, 'int')
df = (page_views
    .groupBy('hour', 'country')
    .count()
    .orderBy(['hour', 'country'])
    .toPandas())

# %%
no_america = df.loc[p['A3']!='USA',:]
animate_pageviews(no_america,'hour')