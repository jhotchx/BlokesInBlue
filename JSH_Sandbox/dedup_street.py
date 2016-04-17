
from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext

sc = SparkContext( appName="Dedup Street" )
#fullstreet = sc.textFile('s3://ukpolice/street.csv') 
#fullstreetMap = fullstreet.map(lambda line: line.split(',')) 
street = sc.textFile('s3://ukpolice/police/2015-12/2015-12-avon-and-somerset-street.csv') 
streetMap = street.map(lambda line: line.split(',')) 

sqlCtx = SQLContext(sc)

#STREET TABLE CREATION
df_street = sqlCtx.createDataFrame(streetMap)
df_street_with_names = df_street.toDF("Crime_ID","Month","Reported_by","Falls_within",
                                      "Longitude","Latitude","Location","LSOA_code","LSOA_name", 
                                      "Crime_type","Last_outcome_category","Context")
df_street_with_names.registerTempTable("street_wn")
df_street_pruned = sqlCtx.sql('select Crime_ID, Month, Longitude, Latitude, \
                               LSOA_code, LSOA_name, Crime_type, Last_outcome_category \
                               from street_wn \
                               where Crime_ID!="Crime ID"')
df_street_pruned.registerTempTable('street_pruned')

#STREET DUPLICATES REMOVAL
df_street_nodupid = sqlCtx.sql('select * \
                                from street_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                                   from street_pruned \
                                                                   group by Crime_ID, Month \
                                                                   having count(Crime_ID)=1 or Crime_ID="") as b \
                                               ON (street_pruned.Crime_ID=b.Crime_ID and street_pruned.Month=b.Month)')
df_street_clean = df_street_nodupid.dropDuplicates(['Crime_ID','Month','Longitude','Latitude','LSOA_code','LSOA_name', 
                                                    'Crime_type'])
df_street_clean.registerTempTable('street_clean')
df_street_dirty = sqlCtx.sql('select *, CASE \
                                            WHEN Crime_ID   !="" AND \
                                                 Month      !="" AND \
                                                 Longitude  !="" AND \
                                                 Latitude   !="" AND \
                                                 LSOA_code  !="" AND \
                                                 LSOA_name  !="" AND \
                                                 Crime_type !="" THEN 1 \
                                            ELSE 0 \
                                        END AS filled \
                              from street_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                             from street_pruned \
                                                             group by Crime_ID, Month \
                                                             having count(Crime_ID)>=2 and Crime_ID!="") as b \
                                                 ON (street_pruned.Crime_ID=b.Crime_ID and street_pruned.Month=b.Month)')
df_street_dirty.registerTempTable("street_dirty")
df_street_lessdirty = sqlCtx.sql('select street_dirty.* \
                                  from street_dirty LEFT OUTER JOIN (select Crime_ID, Month, \
                                                                            min(filled) AS minfilled, \
                                                                            max(filled) AS maxfilled \
                                                                     from street_dirty \
                                                                     group by Crime_ID, Month) as b \
                                                    ON (street_dirty.Crime_ID=b.Crime_ID AND street_dirty.Month=b.Month) \
                                  where NOT (b.minfilled!=b.maxfilled AND street_dirty.filled=0)')
df_street_nofill = df_street_lessdirty.drop('filled')
df_street_cleaned = df_street_nofill.dropDuplicates(['Crime_ID', 'Month'])
df_street_cleaned.registerTempTable('street_new_cleaned')
df_street_analysis = sqlCtx.sql('select * \
                                 from street_clean \
                                 \
                                 UNION ALL \
                                 \
                                 select * \
                                 from street_new_cleaned')


