# %%
"""
Covid reader sessions bigrams 
- number of occurences of visits to pages A -> B
-   
- bucketed by month and country
- k-anomity threshold of 100
- example: In March of 2020, there were a 147 occurences of readers accessing the page `es.wikipedia/Coronavirus` followed by `es.wikipedia/Virus` from Argentina 
"""

# the extract_traces udf generates patterns of various 
# lenghts from a session. for the one hop covid dataset, 
# we only consider length 2.
n_gram_lengths = [2]
@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType(), True), True))
def extract_traces(session):
    import itertools
    traces = []
    # deduplicate repeated page, a->b->a is ok, a->a->b becomes is a->b
    session = [g[0] for g in itertools.groupby(session)]
    for n_hop_length in n_gram_lengths:
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

#%%

covid_bigrams = (sessions
    .withColumn('week', extract_week('year', 'month', 'day'))
    .select(all_time_geography_columns + [ F.explode(extract_traces('session')).alias('trace')])
    .select(all_time_geography_columns + [F.col('trace').getItem(0).alias('project'), F.col('trace').getItem(1).alias('is_covid').cast('boolean'), F.col('trace').getItem(2).alias('trace')])
    .filter(F.col('is_covid')))


split_trace = F.split(F.col('trace'), ' -> ')

buckets = ['year', 'month', 'country']
dataset = (k_anonymous_dataset(covid_bigrams, 100, buckets)
    .withColumn('from', split_trace.getItem(0))
    .withColumn('to', split_trace.getItem(1))
    .select(buckets + ['project', 'from', 'to', F.col('count').alias('views')])
    .cache())
   
#%%
do_it = False
if do_it:
    (dataset
        .coalesce(1)
        .write.mode("overwrite").csv('covid/datasets/covid_session_bigrams', compression='none', sep='\t'))
    dataset.printSchema()

# %%

print(dataset.count())
# 89163
print(dataset.select("country").distinct().count())
# 96
print(dataset.select("project").distinct().count())
# 56

# %%

(dataset
    .where(F.col("to")=="es.wikipedia/Orthocoronavirinae")
    .where(F.col("month")==3)
    .where(F.col("country")=="Chile")
    .orderBy('views',ascending=False)
).limit(20).toPandas()

