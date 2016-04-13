#!/usr/bin/pyspark
​
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext

​
sc = SparkContext( appName="Combine Tables" )
sqlCtx = SQLContext(sc)
bucket = 's3://ukpolice/police/'

street = sc.textFile(bucket+"/2015-01/*-street.csv") \
		.map(lambda line: line.split(',')) \
    	.map(lambda line: Row(crime_id=line[0],month=line[1],reported_by=line[2],falls_within=line[3],longitude=line[4],latitude=line[5],location=line[6], lsoa_code=line[7],lsoa_name=line[8],crime_type=line[9],last_outcome_cat=line[10],context=line[11]))
street = street.coalesce(1)
street.saveAsTextFile('s3://ukpolice/street1')
# street_df = sqlCtx.createDataFrame(street)
# street_df.registerTempTable('street')

outcomes = sc.textFile(bucket+"/*/*-outcomes.csv") \
		.map(lambda line: line.split(',')) \
		.map(lambda line: Row(crime_id=line[0],month=line[1],reported_by=line[2],falls_within=line[3],longitude=line[4],latitude=line[5],location=line[6],lsoa_code=line[7],lsoa_name=line[8],outcome_type=line[9]))
outcomes = outcomes.coalesce(1)
outcomes.saveAsTextFile('s3://ukpolice/outcomes')
# outcomes_df = sqlCtx.createDataFrame(outcomes)
# outcomes_df.registerTempTable('out')

sands = sc.textFile(bucket+"/*/*-search.csv") \
		.map(lambda line: line.split(',')) \
		.map(lambda line: Row(type = line[0],Date=line[1],part_of_police_op=line[2],police_op=line[3],latitude=line[4],longitude=line[5],gender=line[6], age_range=line[7],ethnicity_self=line[8],ethnicity_officer=line[9],legislation=line[10],search_object=line[11],outcome=line[12],outcome_link_object=line[13],clothing_removal=line[14]))
sands = sands.coalesce(1)
sands.saveAsTextFile('s3://ukpolice/sands')
#sands_df = sqlCtx.createDataFrame(sands)
#sands_df.registerTempTable('search')
