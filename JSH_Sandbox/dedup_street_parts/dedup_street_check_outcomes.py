
#Packages
from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext

#Declare spark context environments
sc     = SparkContext( appName="Dedup Street Outcomes" )
sqlCtx = SQLContext(sc)

street = sc.textFile('s3://ukpolice/police/2015-12/2015-12-avon-and-somerset-street.csv')
#street = sc.textFile('s3://ukpolice/street_analysis_small_files')

print("crime_types:")
print(crime_types.sort())
outcome_types = sqlCtx.sql('select distinct Last_outcome_category \
                            from street_analysis').collect()
print("outcomes_types:")
print(outcome_types.sort())