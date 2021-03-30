import pyspark.sql.functions as F

@F.udf(returnType='string')
def oops(i):
    if i==10:
        print('oh oh, this might be a bad idea')
        xxx = list(range(10000000000))
        return 'never'
    return 'ok'

def read_data(spark):
     return (spark.createDataFrame([ (i%20,) for i in range(100)], ['i'])
         .groupBy('i')
         .count()
         .withColumn('oops?', oops('i')))
