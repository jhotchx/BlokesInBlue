## to install pandas:
## sudo pip install pandas


from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext
import pandas

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
#Drop duplicates in the non-duplicate crime ID data as determined by having the same value in all variables.
#This seems the safest course of action for accuracy.
df_street_nodupid = sqlCtx.sql('select * \
                                from street_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                                   from street_pruned \
                                                                   group by Crime_ID, Month \
                                                                   having count(Crime_ID)=1 or Crime_ID="") as b \
                                               ON (street_pruned.Crime_ID=b.Crime_ID and street_pruned.Month=b.Month)')

df_street_clean = df_street_nodupid.dropDuplicates(['Crime_ID','Month','Longitude','Latitude','LSOA_code','LSOA_name', 
                                                    'Crime_type','Last_outcome_category'])
#Make a table from the dataframe so that it can be called from a SQL context
df_street_clean.registerTempTable('street_clean')

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

#Any remaining duplicates, just drop whichever record is unhappily first
df_street_cleaned = df_street_nofill.dropDuplicates(['Crime_ID', 'Month'])

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

df_street_analysis_all.registerTempTable("street_analysis_all")

df_street_analysis = sqlCtx.sql('select * \
                                 from street_analysis_all \
                                 where NOT (LSOA_code="" AND LSOA_name="" AND \
                                            Latitude="" AND Longitude="")')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_analysis.registerTempTable('street_analysis')

df_street_analysis_year = sqlCtx.sql('select Crime_ID, SUBSTRING(Month,1,4) AS Year, Longitude, Latitude, LSOA_code, LSOA_name, Crime_type, Last_outcome_category from street_analysis')

total_crime_2011_lsoa = df_street_analysis_year.filter(df_street_analysis_year['Year']==2011).groupBy("LSOA_name").count()

tot_2011_lsoa = total_crime_2011_lsoa.toPandas()

tot_2011_lsoa.to_csv('/home/hadoop/total_crime_2011_lsoa.csv')

#Pull crosswalk file from s3
crosswalk_orig = sc.textFile('s3://ukpolice/LSOA_to_MSOA.csv') 

#Break csv into fields
xwalk = crosswalk_orig.map(lambda line: line.split(',')) 

#Turn the crosswalk into a data frame
df_xwalk = sqlCtx.createDataFrame(xwalk)

#Assign column headers to file
xwalk_with_header = df_xwalk.toDF("OA11CD","LSOA11CD","LSOA11NM","MSOA11CD",
                                  "MSOA11NM","LAD11CD","LAD11NM","LAD11NMW")

#Make a table from the dataframe so that it can be called from a SQL context
xwalk_with_header.registerTempTable("xwalk_header")

#Keep only the variables that we want, save them in a new data frame.
xwalk_simple = sqlCtx.sql('select LSOA11CD, LSOA11NM, MSOA11CD, MSOA11NM, LAD11CD, LAD11NM \
                           from xwalk_header \
                           where LSOA11CD!="LSOA11CD"')

#Drop duplicates from the crosswalk
xwalk_dedup = xwalk_simple.dropDuplicates(['LSOA11CD','LSOA11NM','MSOA11CD',
                                           'MSOA11NM','LAD11CD','LAD11NM'])

#Make a table from the dataframe so that it can be called from a SQL context
xwalk_dedup.registerTempTable("xwalk_dedup")

df_street_analysis_year.registerTempTable('street_analysis_year')

df_street_LAD = sqlCtx.sql('select street_analysis_year.*, xwalk_dedup.MSOA11CD as MSOA_code, \
                                                    xwalk_dedup.MSOA11NM as MSOA_name, \
                                                    xwalk_dedup.LAD11CD as LAD_code, \
                                                    xwalk_dedup.LAD11NM as LAD_name \
                                             from street_analysis_year LEFT OUTER JOIN xwalk_dedup \
                                                              ON (street_analysis_year.LSOA_code=xwalk_dedup.LSOA11CD AND \
                                                                  street_analysis_year.LSOA_name=xwalk_dedup.LSOA11NM)')

total_crime_2011_LAD = df_street_LAD.filter(df_street_LAD['Year']==2011).groupBy("LAD_name").count()

total_crime_LAD_year = df_street_LAD.groupBy("LAD_name","Year").count()

tot_LAD_year = total_crime_LAD_year.toPandas()

tot_LAD_year.to_csv('/home/hadoop/total_crime_LAD_year.csv')
tot_2011_LAD = total_crime_2011_LAD.toPandas()

tot_2011_LAD.to_csv('/home/hadoop/total_crime_2011_LAD.csv')

type_LAD_year = df_street_LAD.groupBy("LAD_name","Year","Crime_type").count()

crimtype_LAD_year = type_LAD_year.toPandas()
crimtype_LAD_year.to_csv('/home/hadoop/type_LAD_year.csv')
