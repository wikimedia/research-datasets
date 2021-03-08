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

