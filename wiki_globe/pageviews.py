
#%%
pv_actor = (spark.sql('SELECT * FROM wmf.pageview_actor'))
pv_actor.printSchema()

#%%

time_cols = ['year', 'month', 'day', 'hour']
geo_cols = [
    F.col('geocoded_data').getItem('continent').alias('continent'),
    F.col('geocoded_data').getItem('country').alias('country'),
    F.col('geocoded_data').getItem('subdivision').alias('subdivision')
]

trace_cols = [
    F.col('pageview_info').getItem('project').alias('project'), 
    F.col('pageview_info').getItem('page_title').alias('page_title'),
    F.col('pageview_info').getItem('page_id').alias('page_id'),
]
        
page_views = (pv_actor
    .where(F.col('year')==2021)
    .where(F.col('month')==5)
    .where(F.col('day')==12)
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
    .cache()

by_country = df.groupBy('country').max('count').alias('value')

df = df
    .orderBy(['hour', 'country'])
    .toPandas())

# %%
no_america = df.loc[p['A3']!='USA',:]
animate_choropleth(no_america,'hour', 'PageViews by country - hour ', show=True)