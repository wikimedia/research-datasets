"""
Pageview data for COVID pages:
- pageviews bucketed by week and country
- k-anomity threshold of 100 
- example: In the 13th week of 2020, the page 'Pandémie_de_covid-19' on fr.wikipedia was visited a 1008 from readers in Belgium 
- list of covid pages: https://docs.google.com/spreadsheets/d/1dx1gYkvLlMQ7mNTlCtfv5zQLw7eu8BN70ZJbqs-L8Fw/edit#gid=616397870
- as a control bucket, we include the sum of pageviews to all non-covid pages, also bucketed by bucketed by week/country with a threshold of 100 
"""

#%%

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
