#!/usr/bin/pyspark

from pyspark import SparkContext
from pyspark.sql import Row, SQLContext

sc = SparkContext( appName="Combine Tables" )
sqlCtx = SQLContext(sc)
bucket = 's3://ukpolice/police/'

#street = sc.textFile(bucket+"/*/*-street.csv") \
#		.map(lambda line: line.split(',')) 
#street = street.coalesce(1)
#street.saveAsTextFile('s3://ukpolice/street')
# street_df = sqlCtx.createDataFrame(street)
# street_df.registerTempTable('street')

#outcomes = sc.textFile(bucket+"/*/*-outcomes.csv") \
#		.map(lambda line: line.split(',')) 
#outcomes = outcomes.coalesce(1)
#outcomes.saveAsTextFile('s3://ukpolice/outcomes')
# outcomes_df = sqlCtx.createDataFrame(outcomes)
# outcomes_df.registerTempTable('out')

sands = sc.textFile(bucket+"/*/*-search.csv").map(lambda line: line.split(',')) 
sands = sands.coalesce(1)
sands.saveAsTextFile('s3://ukpolice/sands')
#sands_df = sqlCtx.createDataFrame(sands)
#sands_df.registerTempTable('search')
