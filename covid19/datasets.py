"""
Generate covid dataset for public release
"""

#%%

sessions = spark.read.parquet('covid/covid_sessions')

#%%

all_time_geography_columns = ['year', 'month', 'week', 'day', 'continent', 'country']

@F.udf(returnType=T.IntegerType())
def extract_week(year, month, day):
    return datetime.date(year, month, day).isocalendar()[1]


def k_anonymous_dataset(input_df, k_threshold, time_geo_columns):
    """
    bucket, count, and apply threshold
    """
    return (input_df
        .groupby(['project', 'trace'] + time_geo_columns)
        .agg(
            F.count('trace').alias('count')
        )
        .where(F.col('count')>k_threshold)
        .orderBy(time_geo_columns)
        .select(time_geo_columns + ['project', 'trace', 'count']))


#%%
"""
Pageview data for COVID pages:
- pageviews bucketed by week and country
- k-anomity threshold of 100 
- example: In the 13th week of 2020, the page 'Pandémie_de_covid-19' on fr.wikipedia was visited a 1008 from readers in Belgium 
- list of covid pages: https://docs.google.com/spreadsheets/d/1dx1gYkvLlMQ7mNTlCtfv5zQLw7eu8BN70ZJbqs-L8Fw/edit#gid=616397870
- as a control bucket, we include the sum of pageviews to all non-covid pages, also bucketed by bucketed by week/country with a threshold of 100 
"""


make_trace = F.udf(lambda p,t: f'{p}/{t}', 'string')
page_cols = [
    F.col('page.project').alias('project'), 
    F.col('page.is_covid').alias('is_covid'), 
    make_trace('page.project', 'page.title').alias('trace')]


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
    .withColumn('trace', make_control('trace', 'is_covid')))

buckets = ['year', 'week', 'country']
dataset = k_anonymous_dataset(covid_with_control, 500, buckets).cache()

#%% 
do_it = False
if do_it:
    (dataset
        .coalesce(1)
        .write.mode("overwrite").csv('covid/datasets/covid_pageviews', compression='none', sep='\t'))
    dataset.printSchema()

# %%
"""
One-hop reader sessions that include a COVID page:
- number of occurences of visits to pages A -> B
- bucketed by month and country
- k-anomity threshold of 100
- example: In March of 2020, there were a 147 occurences of readers accessing the page `es.wikipedia/Coronavirus` followed by `es.wikipedia/Virus` from Argentina 
"""

n_hop_lengths = [2]

@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType(), True), True))
def extract_traces(session):
    traces = []
    # deduplicate repeated page, a->b->a is ok, a->a->b becomes is a->b
    session = [g[0] for g in itertools.groupby(session)]
    for n_hop_length in n_hop_lengths:
        for i in range(len(session)-n_hop_length+1):
            session_trace = session[i:i+n_hop_length]
            if len(session_trace) == n_hop_length:
                projs = set()
                trace = ''
                is_covid = []
                for page in session_trace:
                    trace += f"{page['project']}/{page['title']} -> "
                    projs.add(page['project'])
                    is_covid.append(page['is_covid'])
                proj = list(projs)[0] if len(projs)==1 else 'multiple_wikis' 
                traces.append((proj, any(is_covid),trace[:-4]))
    return traces

one_hop_traces = (sessions
    .withColumn('week', extract_week('year', 'month', 'day'))
    .select(all_time_geography_columns + [ F.explode(extract_traces('session')).alias('trace')])
    .select(all_time_geography_columns + [F.col('trace').getItem(0).alias('project'), F.col('trace').getItem(1).alias('is_covid').cast('boolean'), F.col('trace').getItem(2).alias('trace')])
    .filter(F.col('is_covid')))

split_trace = F.split(F.col('trace'), ' -> ')

buckets = ['year', 'month', 'country']
dataset = (k_anonymous_dataset(one_hop_traces, 100, buckets)
    .withColumn('from', split_trace.getItem(0))
    .withColumn('to', split_trace.getItem(1))
    .select(buckets + ['project', 'from', 'to', 'count'])
    .cache())
   
#%%
do_it = True
if do_it:
    (dataset
        .coalesce(1)
        .write.mode("overwrite").csv('covid/datasets/covid_one_hop', compression='none', sep='\t'))
    dataset.printSchema()
