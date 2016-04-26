#==========SETUP==========#

#Packages
from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext

#Declare spark context environments
sc     = SparkContext( appName="Dedup Street" )
sqlCtx = SQLContext(sc)

#Load street data
street = sc.textFile('s3://ukpolice/street.csv') 
#street = sc.textFile('s3://ukpolice/police/*/*-street.csv')
#street = sc.textFile('s3://ukpolice/police/2015-12/2015-12-avon-and-somerset-street.csv') 

#Breakup data into fields
streetMap = street.map(lambda line: line.split(',')) 

#==========STREET TABLE CREATION==========#

#Create dataframe from the street data already broken into fields.
df_street = sqlCtx.createDataFrame(streetMap)

#Rename columns of the data frame to names that mean something
df_street_with_names = df_street.toDF("Crime_ID","Month","Reported_by","Falls_within",
                                      "Longitude","Latitude","Location","LSOA_code","LSOA_name", 
                                      "Crime_type","Last_outcome_category","Context")

#Make a table from the dataframe so that it can be called from a SQL context
df_street_with_names.registerTempTable("street_wn")

#Keep only the variables that we want, save them in a new data frame.
df_street_pruned = sqlCtx.sql('select Crime_ID, Month, Longitude, Latitude, \
                                      LSOA_code, LSOA_name, Crime_type, Last_outcome_category \
                               from street_wn \
                               where Crime_ID!="Crime ID"')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_pruned.registerTempTable('street_pruned')
print("Number of records before deduping")
count = df_street_pruned.count()
print(count)


#==========STREET DUPLICATES REMOVAL==========#

#Some data are known duplicates (because they have the same Crime ID hash)
#Here we are selecting everything that doesn't have a duplicated Crime ID OR 
#that has a missing Crime ID (of which there are many).
df_street_nodupid = sqlCtx.sql('select * \
                                from street_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                                   from street_pruned \
                                                                   group by Crime_ID, Month \
                                                                   having count(Crime_ID)=1 or Crime_ID="") as b \
                                               ON (street_pruned.Crime_ID=b.Crime_ID and street_pruned.Month=b.Month)')
print("Number of records after seperating out duplicate Crime IDs")
count = df_street_nodupid.count()
print(count)

#Drop duplicates in the non-duplicate crime ID data as determined by having the same value in all variables.
#This seems the safest course of action for accuracy.
df_street_clean = df_street_nodupid.dropDuplicates(['Crime_ID','Month','Longitude','Latitude','LSOA_code','LSOA_name', 
                                                    'Crime_type','Last_outcome_category'])

#Make a table from the dataframe so that it can be called from a SQL context
df_street_clean.registerTempTable('street_clean')
print("Number of records after 1st dedup without crime ids")
count = df_street_clean.count()
print(count)

#Now subset from the original pruned file all of the records that have a non-missing Crime ID that is duplicated.
#Also create a variable called "filled" that checks to see if there is a value in every field.  
df_street_dirty = sqlCtx.sql('select *, CASE \
                                            WHEN Crime_ID              !="" AND \
                                                 Month                 !="" AND \
                                                 Longitude             !="" AND \
                                                 Latitude              !="" AND \
                                                 LSOA_code             !="" AND \
                                                 LSOA_name             !="" AND \
                                                 Crime_type            !="" AND \
                                                 Last_outcome_category !="" THEN 2 \
                                            WHEN Crime_ID              !="" AND \
                                                 Month                 !="" AND \
                                                 Longitude             !="" AND \
                                                 Latitude              !="" AND \
                                                 LSOA_code             !="" AND \
                                                 LSOA_name             !="" AND \
                                                 Crime_type            !="" AND \
                                                 Last_outcome_category  ="" THEN 1 \
                                            ELSE 0 \
                                        END AS filled \
                              from street_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                             from street_pruned \
                                                             group by Crime_ID, Month \
                                                             having count(Crime_ID)>=2 and Crime_ID!="") as b \
                                                 ON (street_pruned.Crime_ID=b.Crime_ID and street_pruned.Month=b.Month)')
print("Number of records with duplicate crime ids")
count = df_street_dirty.count()
print(count)

#Make a table from the dataframe so that it can be called from a SQL context
df_street_dirty.registerTempTable("street_dirty")

#Find maximum value for filled across all records within a given Crime ID.
#Keep records with the maximum value.
df_street_lessdirty = sqlCtx.sql('select street_dirty.* \
                                  from street_dirty LEFT OUTER JOIN (select Crime_ID, Month, \
                                                                            max(filled) AS maxfilled \
                                                                     from street_dirty \
                                                                     group by Crime_ID, Month) as b \
                                                    ON (street_dirty.Crime_ID=b.Crime_ID AND street_dirty.Month=b.Month) \
                                  where NOT ((b.maxfilled=2 AND street_dirty.filled!=2) OR \
                                             (b.maxfilled=1 AND street_dirty.filled!=1))')

#Drop the "filled" variable from the data frame.
df_street_nofill = df_street_lessdirty.drop('filled')
print("Number of records after removing drops with filled.")
count = df_street_nofill.count()
print(count)

#Any remaining duplicates, just drop whichever record is unhappily first
df_street_cleaned = df_street_nofill.dropDuplicates(['Crime_ID', 'Month'])
print("Number of records after removing remaining drops at random.")
count = df_street_cleaned.count()
print(count)

#Make a table from the dataframe so that it can be called from a SQL context
df_street_cleaned.registerTempTable('street_new_cleaned')

#Combine the cleaned data that was originally duplicated at the non-missing Crime ID/Month level with
#the cleaned data that contained many missings and singular Crime ID/Month combinations
df_street_analysis_all = sqlCtx.sql('select * \
                                 from street_clean \
                                 \
                                 UNION ALL \
                                 \
                                 select * \
                                 from street_new_cleaned')
print("Number of records after recombining files after some cleaning.")
count = df_street_analysis_all.count()
print(count)

#Make a table from the dataframe so that it can be called from a SQL context
df_street_analysis_all.registerTempTable("street_analysis_all")

df_street_analysis = sqlCtx.sql('select * \
                                 from street_analysis_all \
                                 where NOT (LSOA_code="" AND LSOA_name="" AND \
                                            Latitude="" AND Longitude="")')
print("Number of records after all cleaning.")
count = df_street_analysis.count()
print(count)

#Make a table from the dataframe so that it can be called from a SQL context
df_street_analysis.registerTempTable('street_analysis')

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_analysis   = df_street_analysis.rdd
#Make one file
rdd_street_analysis_1 = rdd_street_analysis.coalesce(1)
#Save
rdd_street_analysis_1.saveAsTextFile('s3://ukpolice/street_analysis_file')

crime_types = sqlCtx.sql('select distinct Crime_type \
                          from street_analysis').collect()
print("crime_types:")
crime_types = sorted(crime_types)
print(crime_types)

outcome_types = sqlCtx.sql('select distinct Last_outcome_category \
                            from street_analysis').collect()
print("outcomes_types:")
outcome_types = sorted(outcome_types)
print(outcome_types)

