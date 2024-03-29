
#%%
# sessions = spark.read.parquet('covid/covid_sessions').cache()
sessions = spark.read.parquet('covid/covid_sessions_redirected').cache()

#%%

all_time_geography_columns = ['year', 'month', 'week', 'day', 'continent', 'country']

@F.udf(returnType=T.IntegerType())
def extract_week(year, month, day):
    import datetime
    return datetime.date(year, month, day).isocalendar()[1]


@F.udf(returnType=T.IntegerType())
def floor_to_100(count):
    import math
    return math.floor(count/100)*100

#%%

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
        .withColumn('count', floor_to_100('count'))
        .orderBy(time_geo_columns)
        .select(time_geo_columns + ['project', 'trace', 'count']))


#%%
# distinct page identifiers to join with datasets for additional columns
page_identifier_cols = [
    F.col('page.project').alias('project'), 
    F.col('page.title').alias('title'), 
    F.col('page.qid').alias('qid'), 
    F.col('page.page_id').alias('page_id'), 
    F.col('page.namespace_id').alias('namespace_id'), 
    F.col('page.is_covid').alias('is_covid')] 

distinct_covid_pages = (sessions
    .select(F.explode('session').alias('page'))
    .select(page_identifier_cols)
    .filter(F.col('is_covid'))
    .distinct()
    .cache())
