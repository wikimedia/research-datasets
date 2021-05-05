"""
Pageview data for COVID pages:
- pageviews bucketed by week and country
- k-anomity threshold of 100 
- example: In the 13th week of 2020, the page 'Pandémie_de_covid-19' on fr.wikipedia was visited a 1008 from readers in Belgium 
- list of covid pages: https://docs.google.com/spreadsheets/d/1dx1gYkvLlMQ7mNTlCtfv5zQLw7eu8BN70ZJbqs-L8Fw/edit#gid=616397870
- as a control bucket, we include the sum of pageviews to all non-covid pages, also bucketed by bucketed by week/country with a threshold of 100 
"""

#%%

page_cols = [
    F.col('page.project').alias('project'), 
    F.col('page.title').alias('title'), 
    F.col('page.is_covid').alias('is_covid')] 


@F.udf(returnType=sessions.schema['session'].dataType)
def only_covid_and_control(contains_covid, session):
    """
    if there are covid page views in the session, only retain the covid views
    if this is a control session, retain them all 
    """
    if contains_covid:
        return [cp for cp in session if cp.is_covid]
    else:
        return session
            
@F.udf(returnType='string')
def make_control(trace, is_covid):
    return trace if is_covid else 'control'        


covid_with_control = (sessions
    .withColumn('week', extract_week('year', 'month', 'day'))
    .withColumn('session', only_covid_and_control('contains_covid', 'session'))
    .select(all_time_geography_columns + [ F.explode('session').alias('page')])
    .select(all_time_geography_columns + page_cols)
    .withColumn('trace', make_control('title', 'is_covid'))
    .drop('wikiid'))

buckets = ['year', 'week', 'country']
dataset = (k_anonymous_dataset(covid_with_control, 100, buckets)
    .select(buckets + ['project', F.col('trace').alias('title'), F.col('count').alias('views')])
    .cache())

#%% 
do_it = False
if do_it:
    (dataset
        .coalesce(1)
        .write.mode("overwrite").csv('covid/datasets/covid_pageviews', compression='none', sep='\t'))
    dataset.printSchema()

# %%

pvs = (T.StructType()
    .add("year", T.IntegerType(), True)
    .add("week", T.IntegerType(), True)
    .add("country", T.StringType(), True)
    .add("project", T.StringType(), True)
    .add("title", T.StringType(), True)
    .add("views", T.IntegerType(), True))
dataset = spark.read.csv('covid/datasets/covid_pageviews', schema=pvs, sep='\t').cache()

# %%

(dataset
    # .where(F.col("title")=="Pandémie_de_Covid-19")
    .where(F.col("week")==13)
    .where(F.col("country")=="Belgium")
    .orderBy("views", ascending=False)
).limit(30).toPandas()

# +----+----+-------+------------+--------------------+-----+
# |year|week|country|     project|               title|views|
# +----+----+-------+------------+--------------------+-----+
# |2020|  13|Belgium|fr.wikipedia|Pandémie_de_covid-19| 1000|
# +----+----+-------+------------+--------------------+-----+

# %%


print(dataset.count())
# 134508
print(dataset.select("country").distinct().count())
# 108
print(dataset.select("project").distinct().count())
# 168


# %%

(dataset
    .groupBy("country")
    .count()
    .orderBy("count",ascending=False)
    .show()
)
# %%

(dataset
    .groupBy("week")
    .agg(
        F.count("*").alias("datapoints"),
        F.countDistinct("country").alias("countries"),
        F.countDistinct("project").alias("wikipedias"),
        F.countDistinct("title").alias("articles")
    )    
    .orderBy("week")
    .toPandas()
    .plot(x="week",y=["datapoints","wikipedias","articles"],subplots=True)
)

# %%

def plot_top_pages(country, topn=10):
    top_pages = (dataset
        .where(F.col('country')==country)
        .where(F.col('views')<20000)
        .groupBy('title')
        .sum('views')
        .orderBy('sum(views)',ascending=False)        
        .limit(topn)
        .collect())
    pv = (dataset
#         .where(F.col('project')=='en.wikipedia')
        .where(F.col('title').isin([r.title for r in top_pages]))
        .where(F.col('country')==country )
        .where(F.col('title')!='control')
        .groupby(['week','title'])
        .agg(F.sum('views').alias('views'))
        .orderBy('week')
    ).toPandas()
    pivoted = pv.pivot(index='week', columns='title', values='views')
    # pivoted = pivoted/pivoted.max(axis=0)
    pivoted.plot()
# %%
plot_top_pages('Italy')
# %%
import matplotlib.pyplot as plt
plt.rcParams['figure.figsize'] = [12, 8]

# %%
