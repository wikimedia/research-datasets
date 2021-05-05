page_type = (T.StructType()
    .add("title", T.StringType(), True)
    .add("page_id", T.StringType(), True)
    .add("qid", T.StringType(), True)
    .add("project", T.StringType(), True)
    .add("namespace_id", T.StringType(), True)
    .add("is_covid", T.BooleanType(), True)
    .add("referer_class", T.StringType(), True))

session_schema =  (T.StructType()
    .add("session", T.ArrayType(page_type, True))
    .add("contains_covid", T.BooleanType(), True)
    .add(covid.schema["continent"])
    .add(covid.schema["country"])
    .add(covid.schema["timezone"])
    .add(covid.schema["year"])
    .add(covid.schema["month"])
    .add(covid.schema["day"]))
    
#%%

covidq="""select * from isaacj.covid19_sessions"""
covid = spark.sql(covidq)
covid.printSchema()

# root
#  |-- session_hash: string (nullable = true)
#  |-- continent: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- subdivision: string (nullable = true)
#  |-- timezone: string (nullable = true)
#  |-- hour: integer (nullable = true)
#  |-- project: string (nullable = true)
#  |-- namespace_id: integer (nullable = true)
#  |-- qid: string (nullable = true)
#  |-- page_id: integer (nullable = true)
#  |-- title: string (nullable = true)
#  |-- is_covid: boolean (nullable = true)
#  |-- referer: string (nullable = true)
#  |-- referer_class: string (nullable = true)
#  |-- access_method: string (nullable = true)
#  |-- last_access: string (nullable = true)
#  |-- min_btw_pvs: double (nullable = true)
#  |-- session_sequence: integer (nullable = true)
#  |-- year: integer (nullable = true)
#  |-- month: integer (nullable = true)
#  |-- day: integer (nullable = true)
#%%

max_session_length = 500

def construct_sessions(grouped_page_views):
    """
    We store a session in row, removing the need for any 
    IP/user agent based identifier. We store only a subset 
    of the originally retained covid reader session:
        - day granularity (previously hour)
        - country granularity (previously subdivision)
        - only referer class, no info about the referer itself
        - only retain sessions shorter than max_session_length
    """
    from datetime import datetime

    _, page_views = grouped_page_views

    pages = {}
    contains_covid = False
    continent, country, subdivision, timezone = (set(), set(), set(), set())
    dts = set()

    # the session_sequence is 1-indexed
    session_length = 1
    for row in page_views:
        page = Row(
            row['title'],
            row['page_id'],
            row['qid'],
            row['project'],
            row['namespace_id'],
            row['is_covid'],
            row['referer_class'])

        pages[row['session_sequence']] = page

        continent.add(row['continent'])
        country.add(row['country'])
        subdivision.add(row['subdivision'])
        timezone.add(row['timezone'])

        dts.add(datetime(year=row['year'], month=row['month'], day=row['day'], hour=row['hour']))

        if row['is_covid']:
            contains_covid = True

        session_length += 1
        if session_length > max_session_length:
            # if we have a session which is longer than the max, we throw away
            # the full session, since the the pageview iterator is not ordered
            # by the session_sequence
            return None

    if len(continent) != 1:
        return None
    if len(country) != 1:
        return None
    if len(timezone) != 1:
        return None

    dt = min(dts)

    # construct the list of page views in this session (of max length max_session_length)
    session = [ pages[i]  for i in range(1, session_length)]
    return Row(
        session,
        contains_covid,
        list(continent)[0],
        list(country)[0],
        list(timezone)[0],
        dt.year,
        dt.month,
        dt.day)

pvs = (covid
    .rdd
    .map(lambda r: (r['session_hash'], r))
    .groupByKey()
    .map(construct_sessions)
    .filter(lambda r: r is not None))


#%%
do_it = False
if do_it:
    sessions = spark.createDataFrame(pvs,session_schema).cache()
    sessions.write.mode('overwrite').save("covid/covid_sessions")

else:
    sessions = spark.read.parquet('covid/covid_sessions')

sessions.printSchema()

# root
#  |-- session: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- title: string (nullable = true)
#  |    |    |-- page_id: string (nullable = true)
#  |    |    |-- qid: string (nullable = true)
#  |    |    |-- project: string (nullable = true)
#  |    |    |-- namespace_id: string (nullable = true)
#  |    |    |-- is_covid: boolean (nullable = true)
#  |    |    |-- referer_class: string (nullable = true)
#  |-- contains_covid: boolean (nullable = true)
#  |-- continent: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- timezone: string (nullable = true)
#  |-- year: integer (nullable = true)
#  |-- month: integer (nullable = true)
#  |-- day: integer (nullable = true)

#%%

@F.udf(returnType='string')
def session_uuid():
    import uuid
    return str(uuid.uuid1())
    

exploded_sessions = (sessions
    # .withColumn('week', extract_week('year', 'month', 'day'))
    .withColumn('uuid', session_uuid())
    .withColumn('page', F.explode('session'))
    .select('*', 'page.*')
    .drop('session'))
    # .repartition(10000))

# from redirects import article_redirects
redirects = article_redirects('2021-01').cache()

rd_sessions = (exploded_sessions 
    .withColumn('wikiid', make_wiki_db('project'))
    .join(
        (redirects
            .withColumn('title_from', qq('title_from'))
            .withColumn('title_to', qq('title_to'))
        ), 
        (F.col('title') == F.col('title_from')) & (F.col('wikiid') == F.col('wiki_db')),
        how='leftouter')
    .withColumn('title', F.coalesce(F.col('title_to'), F.col('title')))
    .withColumn('page', F.struct(*[F.col(col) for col in exploded_sessions.select('page.*').columns])) 
    .drop(*exploded_sessions.select('page.*').columns)
    .drop('wikiid', 'wiki_db', 'title_from', 'title_to')
    # .repartition(6000)
    .groupBy('uuid')
    .agg(
        F.collect_list('page').alias('session'),
        F.first('contains_covid').alias('contains_covid'),
        F.first('continent').alias('continent'),
        F.first('country').alias('country'),
        F.first('timezone').alias('timezone'),
        F.first('year').alias('year'),
        F.first('month').alias('month'),
        F.first('day').alias('day'),
    )
    .drop('uuid')    
)
#%%

do_it = False
if do_it:
    sessions = spark.createDataFrame(rd_sessions,session_schema).cache()
    sessions.write.mode('overwrite').save("covid/covid_sessions_redirected")

else:
    sessions = spark.read.parquet('covid/covid_sessions_redirected').cache()

