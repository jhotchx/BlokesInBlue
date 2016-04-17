#outcomes = sc.textFile('s3://ukpolice/outcomes.csv')
outcomes = sc.textFile('s3://ukpolice/police/2015-12/2015-12-avon-and-somerset-outcomes.csv') 
outcomesMap = outcomes.map(lambda line: line.split(','))

#OUTCOMES TABLE CREATION
df_outcomes = sqlCtx.createDataFrame(outcomesMap)
df_outcomes_with_names = df_outcomes.toDF("Crime_ID","Month","Reported_by","Falls_within",
                                          "Longitude","Latitude","Location","LSOA_code","LSOA_name", 
                                          "Outcome_type")
df_outcomes_with_names.registerTempTable("outcomes_wn")
df_outcomes_pre_pruned = sqlCtx.sql('select Crime_ID, Month, Longitude, Latitude, LSOA_code, LSOA_name, Outcome_type \
                                     from outcomes_wn \
                                     where Crime_ID!="Crime ID" \
                                     order by RAND()')
df_outcome_pruned = df_outcomes_pre_pruned.dropDuplicates(['Crime_ID', 'Month', 'Longitude', 'Latitude', 
                                                            LSOA_code, LSOA_name, Outcome_type])
df_outcomes_pruned.registerTempTable('outcomes_pruned')

#OUTCOMES DUPLICATES REMOVAL

df_outcomes_clean = sqlCtx.sql('select * \
                                from outcomes_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                                     from outcomes_pruned \
                                                                     group by Crime_ID, Month \
                                                                     having count(Crime_ID)=1) as b \
                                                     ON (outcomes_pruned.Crime_ID=b.Crime_ID and outcomes_pruned.Month=b.Month)')
df_outcomes_clean.registerTempTable('outcomes_clean')
df_outcomes_dirty = sqlCtx.sql('select *, CASE \
                                            WHEN Crime_ID     !="" AND \
                                                 Month        !="" AND \
                                                 Longitude    !="" AND \
                                                 Latitude     !="" AND \
                                                 LSOA_code    !="" AND \
                                                 LSOA_name    !="" AND \
                                                 Outcome_type !="" THEN 1 \
                                            ELSE 0 \
                                        END AS filled \
                                from outcomes_pruned LEFT SEMI JOIN (select Crime_ID, Month \
                                                                     from outcomes_pruned \
                                                                     group by Crime_ID, Month \
                                                                     having count(Crime_ID)>=2) as b \
                                                     ON (outcomes_pruned.Crime_ID=b.Crime_ID and outcomes_pruned.Month=b.Month) \
                                order by Crime_ID, Month')
df_outcomes_dirty.registerTempTable("outcomes_dirty")
df_outcomes_lessdirty = sqlCtx.sql('select outcomes_dirty.* \
                                    from outcomes_dirty LEFT OUTER JOIN (select Crime_ID, Month, \
                                                                              min(filled) AS minfilled, \
                                                                              max(filled) AS maxfilled \
                                                                         from outcomes_dirty \
                                                                         group by Crime_ID, Month) as b \
                                                        ON (outcomes_dirty.Crime_ID=b.Crime_ID AND outcomes_dirty.Month=b.Month) \
                                    where NOT (b.minfilled!=b.maxfilled AND outcomes_dirty.filled=0)')
df_outcomes_nofill = df_outcomes_lessdirty.drop('filled')
df_outcomes_cleaned = df_outcomes_nofill.dropDuplicates(['Crime_ID', 'Month'])
df_outcomes_cleaned.registerTempTable('outcomes_new_cleaned')
df_outcomes_analysis = sqlCtx.sql('select * \
                                   from outcomes_clean \
                                   \
                                   UNION ALL \
                                   \
                                   select * \
                                   from outcomes_new_cleaned')