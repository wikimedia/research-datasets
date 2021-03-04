"""
interactive notebook for k-anomimity analysis
"""


all_reading_traces = (sessions
    .withColumn('week', extract_week('year', 'month', 'day'))
    .select(all_time_geography_columns + [ F.explode(F.col('session')).alias('page')]))


# project|k_anonymity_bin|pageviews|distinct_pages|threshold_bin
def k_anonymity_bins(bins, time_geo_columns, filter_covid=None):

    if filter_covid is None:
        reading_traces = all_reading_traces
    else:
        reading_traces = (all_reading_traces
            .filter(F.col('page.is_covid')==filter_covid))

    reading_traces = reading_traces.select(all_time_geography_columns + ['page.qid', 'page.project']))

    # project, pageviews, distinct_pages
    by_project = (reading_traces
        .groupby('project')
        .agg(
            F.count('qid').alias('total_views'),
            F.countDistinct('qid').alias('total_pages')
        )
        .cache())


    @F.udf(returnType=T.ArrayType(T.StringType(), True))
    def bin_k_anonymity(pageviews):
        return [ f'{b}+' for b in bins if pageviews >= b ]

    counts = (reading_traces
        .groupby(['project', 'qid'] + time_geo_columns)
        .agg(
            F.count('qid').alias('count')
        )
        .withColumn('k_anonymity_bin', F.explode(bin_k_anonymity('count')))
        .groupby(['project', 'k_anonymity_bin'])
        .agg(
            F.sum('count').alias('views'),
            F.countDistinct('qid').alias('pages')
        )
        .cache())

    # project|30+_views|30+_pages|100+_views|100+_pages|500+_views|500+_pages|1000+_views|1000+_pages
    k_anonymity_agg = (counts
        .groupby('project')
        .pivot('k_anonymity_bin', [f'{b}+' for b in bins])
        .agg(
            # a tuple (project,k_anonymity_bin) is unique, so there is only 1 value for each
            F.first('views').alias('views'),
            F.first('pages').alias('pages'))
        .cache())

    joined = k_anonymity_agg.join(by_project, 'project')

    # add percentages
    for c in joined.columns:
        if c.endswith('views') and c!='total_views':
            joined = joined.withColumn(f'{c}_percentage', F.round(F.col(c)/F.col('total_views')*100, 2))
        if c.endswith('pages') and c!='total_pages':
            joined = joined.withColumn(f'{c}_percentage', F.round(F.col(c)/F.col('total_pages')*100, 2))

    return joined

#%%
bins = [30,100,500,1000]

bucket_combinations = list(itertools.product(time_buckets.keys(), geography_buckets.keys()))
unioned = None
for time_bucket, geography_bucket in bucket_combinations:
    name = f'{time_bucket} - {geography_bucket}'
    stats = (k_anonymity_bins(time_buckets[time_bucket] + geography_buckets[geography_bucket])
        .withColumn('time_geo_bucket', F.lit(name)))
    unioned = unioned.union(stats) if unioned is not None else stats


first = ['project', 'time_geo_bucket' ,F.col('total_views').alias('views'), F.col('total_pages').alias('pages')]
arranged_bins = []
for b in bins:
    arranged_bins += [
        F.col(f'{b}+_pages').alias(f'{b}+ pages'),
        F.col(f'{b}+_pages_percentage').alias(f'{b}+ pages (%)'),
        F.col(f'{b}+_views').alias(f'{b}+ views'),
        F.col(f'{b}+_views_percentage').alias(f'{b}+ views (%)')]

unioned = unioned.select(first+arranged_bins)

with pd.ExcelWriter('/home/fab/covid_k_anonymity_no_covid.xlsx', engine='xlsxwriter') as excel_writer:
    (unioned
        .toPandas()
        .to_excel(excel_writer, sheet_name=name, index=False))
