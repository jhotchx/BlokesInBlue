
#==========SETUP==========#

#Packages
from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext

#Declare spark context environments
sc     = SparkContext( appName="Dedup Street" )
sqlCtx = SQLContext(sc)

#Load street data
#street = sc.textFile('s3://ukpolice/street.csv') 
#street = sc.textFile('s3://ukpolice/police/*/*-street.csv')
street = sc.textFile('s3://ukpolice/police/2015-12/2015-12-avon-and-somerset-street.csv') 

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
#rdd_street_analysis_1.saveAsTextFile('s3://ukpolice/street_analysis')

#==========FEATURE GENERATION==========#

df_street_analysis.registerTempTable('street_analysis_build')

crime_types = sqlCtx.sql('select distinct Crime_type \
                          from street_analysis').collect()
print("crime_types:")
print(crime_types)
outcome_types = sqlCtx.sql('select distinct Last_outcome_category \
                            from street_analysis').collect()
print("outcomes_types:")
print(outcome_types)

#Create Crime_type variables
df_street_add_features = sqlCtx.sql(' \
            select *, \
                   CASE WHEN Crime_type = ""                             THEN 1 ELSE 0 END as EMPTYNULLCrime, \
                   CASE WHEN Crime_type = "Anti-social behaviour"        THEN 1 ELSE 0 END as AntiSocialBehavior, \
                   CASE WHEN Crime_type = "Bicycle theft"                THEN 1 ELSE 0 END as BicycleTheft, \
                   CASE WHEN Crime_type = "Burglary"                     THEN 1 ELSE 0 END as Burglary, \
                   CASE WHEN Crime_type = "Criminal damage and arson"    THEN 1 ELSE 0 END as CriminalDamageArson, \
                   CASE WHEN Crime_type = "Drugs"                        THEN 1 ELSE 0 END as Drugs, \
                   CASE WHEN Crime_type = "Other crime"                  THEN 1 ELSE 0 END as OtherCrime, \
                   CASE WHEN Crime_type = "Other theft"                  THEN 1 ELSE 0 END as OtherTheft, \
                   CASE WHEN Crime_type = "Possession of weapons"        THEN 1 ELSE 0 END as PossessionWeapons, \
                   CASE WHEN Crime_type = "Public disorder and weapons"  THEN 1 ELSE 0 END as PublicDisorderWeapons, \
                   CASE WHEN Crime_type = "Public order"                 THEN 1 ELSE 0 END as PublicOrder, \
                   CASE WHEN Crime_type = "Robbery"                      THEN 1 ELSE 0 END as Robbery, \
                   CASE WHEN Crime_type = "Shoplifting"                  THEN 1 ELSE 0 END as Shoplifting, \
                   CASE WHEN Crime_type = "Theft from the person"        THEN 1 ELSE 0 END as TheftFromPerson, \
                   CASE WHEN Crime_type = "Vehicle crime"                THEN 1 ELSE 0 END as VehicleCrime, \
                   CASE WHEN Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffences \
                   CASE WHEN Crime_type = "Violent crime"                THEN 1 ELSE 0 END as ViolentCrime \
            from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Create last_outcome variables
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              THEN 1 ELSE 0 END as EMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    THEN 1 ELSE 0 END as ActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        THEN 1 ELSE 0 END as AwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  THEN 1 ELSE 0 END as CourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      THEN 1 ELSE 0 END as CourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    THEN 1 ELSE 0 END as DefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 THEN 1 ELSE 0 END as DefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   THEN 1 ELSE 0 END as FormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" THEN 1 ELSE 0 END as InvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              THEN 1 ELSE 0 END as LocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 THEN 1 ELSE 0 END as OffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                THEN 1 ELSE 0 END as OffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      THEN 1 ELSE 0 END as OffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     THEN 1 ELSE 0 END as OffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             THEN 1 ELSE 0 END as OffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             THEN 1 ELSE 0 END as OffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          THEN 1 ELSE 0 END as OffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 THEN 1 ELSE 0 END as OffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      THEN 1 ELSE 0 END as OffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          THEN 1 ELSE 0 END as OffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 THEN 1 ELSE 0 END as OffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       THEN 1 ELSE 0 END as OffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       THEN 1 ELSE 0 END as SuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   THEN 1 ELSE 0 END as UnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           THEN 1 ELSE 0 END as UnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#==========Create Interaction Variable with:

#Missing
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "" THEN 1 ELSE 0 END as EMPTYNULLCrimeUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Anti-social behavior
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Anti-social behaviour" THEN 1 ELSE 0 END as AntiSocialBehaviorUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Bicycle theft
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Bicycle theft" THEN 1 ELSE 0 END as BicycleTheftUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Burglary
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglarySuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Burglary" THEN 1 ELSE 0 END as BurglaryUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Criminal damage and arson
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Criminal damage and arson" THEN 1 ELSE 0 END as CriminalDamageArsonUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Drugs
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Drugs" THEN 1 ELSE 0 END as DrugsUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Other crime
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Other crime" THEN 1 ELSE 0 END as OtherCrimeUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Other theft
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Other theft" THEN 1 ELSE 0 END as OtherTheftUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Possession of weapons
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Possession of weapons" THEN 1 ELSE 0 END as PossessionWeaponsUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Public disorder and weapons
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Public disorder and weapons" THEN 1 ELSE 0 END as PublicDisorderWeaponsUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Public order
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Public order" THEN 1 ELSE 0 END as PublicOrderUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Robbery
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberySuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Robbery" THEN 1 ELSE 0 END as RobberyUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Shoplifting
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Shoplifting" THEN 1 ELSE 0 END as ShopliftingUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Theft from the person
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Theft from the person" THEN 1 ELSE 0 END as TheftFromPersonUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Vehicle crime
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Vehicle crime" THEN 1 ELSE 0 END as VehicleCrimeUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Violence and sexual offences
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Violence and sexual offences" THEN 1 ELSE 0 END as ViolenceSexualOffencesUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

#Violent crime
df_street_add_features = sqlCtx.sql('select *, \
                   CASE WHEN Last_outcome_category = ""                                              AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeEMPTYNULLOutcome, \
                   CASE WHEN Last_outcome_category = "Action to be taken by another organisation"    AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeActionToBeTakenOtherOrg, \
                   CASE WHEN Last_outcome_category = "Awaiting court outcome"                        AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeAwaitingCourtOutcome, \
                   CASE WHEN Last_outcome_category = "Court case unable to proceed"                  AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeCourtCaseUnableToProceed, \
                   CASE WHEN Last_outcome_category = "Court result unavailable"                      AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeCourtResultUnavailable, \
                   CASE WHEN Last_outcome_category = "Defendant found not guilty"                    AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeDefendantNotGuilty, \
                   CASE WHEN Last_outcome_category = "Defendant sent to Crown Court"                 AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeDefendantSentCrownCourt, \
                   CASE WHEN Last_outcome_category = "Formal action is not in the public interest"   AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeFormalActionNotPublicInterest, \
                   CASE WHEN Last_outcome_category = "Investigation complete; no suspect identified" AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeInvestigationCompleteNoSuspect, \
                   CASE WHEN Last_outcome_category = "Local resolution"                              AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeLocalResolution, \
                   CASE WHEN Last_outcome_category = "Offender deprived of property"                 AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffDeprivedProperty, \
                   CASE WHEN Last_outcome_category = "Offender fined"                                AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffFined, \
                   CASE WHEN Last_outcome_category = "Offender given a caution"                      AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenCaution, \
                   CASE WHEN Last_outcome_category = "Offender given a drugs possession warning"     AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenDrugsPossessionWarning, \
                   CASE WHEN Last_outcome_category = "Offender given absolute discharge"             AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenAbsoluteDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given community sentence"             AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenCommunitySentence, \
                   CASE WHEN Last_outcome_category = "Offender given conditional discharge"          AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenConditionalDischarge, \
                   CASE WHEN Last_outcome_category = "Offender given penalty notice"                 AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenPenaltyNotice, \
                   CASE WHEN Last_outcome_category = "Offender given suspended prison sentence"      AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffGivenSuspendedPrisonSentence, \
                   CASE WHEN Last_outcome_category = "Offender ordered to pay compensation"          AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffOrderedPayCompensation, \
                   CASE WHEN Last_outcome_category = "Offender otherwise dealt with"                 AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffOtherwiseDealtWith, \
                   CASE WHEN Last_outcome_category = "Offender sent to prison"                       AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeOffSentPrison, \
                   CASE WHEN Last_outcome_category = "Suspect charged as part of another case"       AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeSuspectChargedPartOfAnotherCase, \
                   CASE WHEN Last_outcome_category = "Unable to prosecute suspect"                   AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeUnableProsecuteSuspect, \
                   CASE WHEN Last_outcome_category = "Under investigation"                           AND Crime_type = "Violent crime" THEN 1 ELSE 0 END as ViolentCrimeUnderInvestigation \
             from street_analysis_build')
df_street_add_features.registerTempTable('street_analysis_build')

print("Number of records after adding variables.")
count = df_street_add_features.count()
print(count)

schemas = df_street_add_features.printSchema()
print(schemas)

#==========AGGREGATE BY LSOA AND MONTH==========#

df_street_agg_LSOA_month = sqlCtx.sql('select Month, LSOA_code, LSOA_name, count(LSOA_code) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_analysis_build \
                       \
                       group by Month, LSOA_code, LSOA_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_LSOA_month.registerTempTable("street_LSOA_month")

print("Number of records after aggregating to LSOA and month level.")
count = df_street_agg_LSOA_month.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_LSOA_month = df_street_agg_LSOA_month.rdd
#Make one file
rdd_street_agg_LSOA_month_1 = rdd_street_agg_LSOA_month.coalesce(1)
#Save
#rdd_street_agg_LSOA_month_1.saveAsTextFile('s3://ukpolice/street_LSOA_month_level')

#==========AGGREGATE BY LSOA NOMONTH==========#

df_street_agg_LSOA_nomonth = sqlCtx.sql('select LSOA_code, LSOA_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_LSOA_month\
                       \
                       group by LSOA_code, LSOA_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_LSOA_nomonth.registerTempTable("street_LSOA")

print("Number of records after aggregating to LSOA NoMonth level.")
count = df_street_agg_LSOA_nomonth.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_LSOA_nomonth = df_street_agg_LSOA_nomonth.rdd
#Make one file
rdd_street_agg_LSOA_nomonth_1 = rdd_street_agg_LSOA_nomonth.coalesce(1)
#Save
#rdd_street_agg_LSOA_nomonth_1.saveAsTextFile('s3://ukpolice/street_LSOA_nomonth_level')

#==========MERGE CROSSWALK==========#

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

#Perform merge
df_street_agg_LSOA_month_merge = sqlCtx.sql('select street_LSOA_month.*, xwalk_header.MSOA11CD as MSOA_code, \
                                                    xwalk_header.MSOA11NM as MSOA_name, \
                                                    xwalk_header.LAD11CD as LAD_code, \
                                                    xwalk_header.LAD11NM as LAD_name \
                                             from street_LSOA_month LEFT OUTER JOIN xwalk_header \
                                                              ON (street_LSOA_month.LSOA_code=xwalk_header.LSOA11CD AND \
                                                                  street_LSOA_month.LSOA_name=xwalk_header.LSOA11NM)')
print("Number of records that don't have a value for MSOA_code:")
count = df_street_agg_LSOA_month_merge.filter(df_street_agg_LSOA_month_merge.MSOA_code=="").count()
print(count)

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_LSOA_month_merge.registerTempTable("street_LSOA_month_merge")

#==========AGGREGATE BY MSOA AND MONTH==========#

df_street_agg_MSOA_month = sqlCtx.sql('select Month, MSOA_code, MSOA_name, LAD_code, LAD_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_LSOA_month_merge\
                       \
                       group by Month, MSOA_code, MSOA_name, LAD_code, LAD_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_MSOA_month.registerTempTable("street_MSOA_month")

print("Number of records after aggregating to MSOA and month level.")
count = df_street_agg_MSOA_month.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_MSOA_month = df_street_agg_MSOA_month.rdd
#Make one file
rdd_street_agg_MSOA_month_1 = rdd_street_agg_MSOA_month.coalesce(1)
#Save
#rdd_street_agg_MSOA_month_1.saveAsTextFile('s3://ukpolice/street_MSOA_month_level')

#==========AGGREGATE BY MSOA NOMONTH==========#

df_street_agg_MSOA_nomonth = sqlCtx.sql('select MSOA_code, MSOA_name, LAD_code, LAD_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_LSOA_month_merge\
                       \
                       group by MSOA_code, MSOA_name, LAD_code, LAD_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_MSOA_nomonth.registerTempTable("street_MSOA_nomonth")

print("Number of records after aggregating to MSOA and NoMonth level.")
count = df_street_agg_MSOA_nomonth.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_MSOA_nomonth = df_street_agg_MSOA_nomonth.rdd
#Make one file
rdd_street_agg_MSOA_nomonth_1 = rdd_street_agg_MSOA_nomonth.coalesce(1)
#Save
#rdd_street_agg_MSOA_nomonth_1.saveAsTextFile('s3://ukpolice/street_MSOA_nomonth_level')

#==========AGGREGATE BY LDA AND MONTH==========#

df_street_agg_LAD_month = sqlCtx.sql('select Month, LAD_code, LAD_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_MSOA_month\
                       \
                       group by Month, LAD_code, LAD_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_LAD_month.registerTempTable("street_LAD_month")

print("Number of records after aggregating to LAD and month level.")
count = df_street_agg_LAD_month.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_LAD_month = df_street_agg_LAD_month.rdd
#Make one file
rdd_street_agg_LAD_month_1 = rdd_street_agg_LAD_month.coalesce(1)
#Save
#rdd_street_agg_LAD_month_1.saveAsTextFile('s3://ukpolice/street_LAD_month_level')

#==========AGGREGATE BY LDA AND NOMONTH==========#

df_street_agg_LAD_nomonth = sqlCtx.sql('select LAD_code, LAD_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable           \
                       SUM(Drugs)                                             AS Drugs,                                             SUM(DefendantNotGuilty)                                    AS DefendantNotGuilty,              \
                       SUM(OtherCrime)                                        AS OtherCrime,                                        SUM(DefendantSentCrownCourt)                               AS DefendantSentCrownCourt,         \
                       SUM(OtherTheft)                                        AS OtherTheft,                                        SUM(FormalActionNotPublicInterest)                         AS FormalActionNotPublicInterest,   \
                       SUM(PossessionWeapons)                                 AS PossessionWeapons,                                 SUM(InvestigationCompleteNoSuspect)                        AS InvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeapons)                             AS PublicDisorderWeapons,                             SUM(LocalResolution)                                       AS LocalResolution,                 \
                       SUM(PublicOrder)                                       AS PublicOrder,                                       SUM(OffDeprivedProperty)                                   AS OffDeprivedProperty,             \
                       SUM(Robbery)                                           AS Robbery,                                           SUM(OffFined)                                              AS OffFined,                        \
                       SUM(Shoplifting)                                       AS Shoplifting,                                       SUM(OffGivenCaution)                                       AS OffGivenCaution,                 \
                       SUM(TheftFromPerson)                                   AS TheftFromPerson,                                   SUM(OffGivenDrugsPossessionWarning)                        AS OffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrime)                                      AS VehicleCrime,                                      SUM(OffGivenAbsoluteDischarge)                             AS OffGivenAbsoluteDischarge,       \
                       SUM(ViolenceSexualOffences)                            AS ViolenceSexualOffences,                            SUM(OffGivenCommunitySentence)                             AS OffGivenCommunitySentence,       \
                       SUM(ViolentCrime)                                      AS ViolentCrime                                       SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
                                                                                                                                    SUM(OffGivenPenaltyNotice)                                 AS OffGivenPenaltyNotice,           \
                                                                                                                                    SUM(OffGivenSuspendedPrisonSentence)                       AS OffGivenSuspendedPrisonSentence, \
                                                                                                                                    SUM(OffOrderedPayCompensation)                             AS OffOrderedPayCompensation,       \
                                                                                                                                    SUM(OffOtherwiseDealtWith)                                 AS OffOtherwiseDealtWith,           \
                                                                                                                                    SUM(OffSentPrison)                                         AS OffSentPrison,                   \
                                                                                                                                    SUM(SuspectChargedPartOfAnotherCase)                       AS SuspectChargedPartOfAnotherCase, \
                                                                                                                                    SUM(UnableProsecuteSuspect)                                AS UnableProsecuteSuspect,          \
                                                                                                                                    SUM(UnderInvestigation)                                    AS UnderInvestigation,              \
                       \
                       SUM(EMPTYNULLCrimeEMPTYNULLOutcome)                       AS EMPTYNULLCrimeEMPTYNULLOutcome,                \
                       SUM(EMPTYNULLCrimeActionToBeTakenOtherOrg)                AS EMPTYNULLCrimeActionToBeTakenOtherOrg,         \
                       SUM(EMPTYNULLCrimeAwaitingCourtOutcome)                   AS EMPTYNULLCrimeAwaitingCourtOutcome,            \
                       SUM(EMPTYNULLCrimeCourtCaseUnableToProceed)               AS EMPTYNULLCrimeCourtCaseUnableToProceed,        \
                       SUM(EMPTYNULLCrimeCourtResultUnavailable)                 AS EMPTYNULLCrimeCourtResultUnavailable,          \
                       SUM(EMPTYNULLCrimeDefendantNotGuilty)                     AS EMPTYNULLCrimeDefendantNotGuilty,              \
                       SUM(EMPTYNULLCrimeDefendantSentCrownCourt)                AS EMPTYNULLCrimeDefendantSentCrownCourt,         \
                       SUM(EMPTYNULLCrimeFormalActionNotPublicInterest)          AS EMPTYNULLCrimeFormalActionNotPublicInterest,   \
                       SUM(EMPTYNULLCrimeInvestigationCompleteNoSuspect)         AS EMPTYNULLCrimeInvestigationCompleteNoSuspect,  \
                       SUM(EMPTYNULLCrimeLocalResolution)                        AS EMPTYNULLCrimeLocalResolution,                 \
                       SUM(EMPTYNULLCrimeOffDeprivedProperty)                    AS EMPTYNULLCrimeOffDeprivedProperty,             \
                       SUM(EMPTYNULLCrimeOffFined)                               AS EMPTYNULLCrimeOffFined,                        \
                       SUM(EMPTYNULLCrimeOffGivenCaution)                        AS EMPTYNULLCrimeOffGivenCaution,                 \
                       SUM(EMPTYNULLCrimeOffGivenDrugsPossessionWarning)         AS EMPTYNULLCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(EMPTYNULLCrimeOffGivenAbsoluteDischarge)              AS EMPTYNULLCrimeOffGivenAbsoluteDischarge,       \
                       SUM(EMPTYNULLCrimeOffGivenCommunitySentence)              AS EMPTYNULLCrimeOffGivenCommunitySentence,       \
                       SUM(EMPTYNULLCrimeOffGivenConditionalDischarge)           AS EMPTYNULLCrimeOffGivenConditionalDischarge,    \
                       SUM(EMPTYNULLCrimeOffGivenPenaltyNotice)                  AS EMPTYNULLCrimeOffGivenPenaltyNotice,           \
                       SUM(EMPTYNULLCrimeOffGivenSuspendedPrisonSentence)        AS EMPTYNULLCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(EMPTYNULLCrimeOffOrderedPayCompensation)              AS EMPTYNULLCrimeOffOrderedPayCompensation,       \
                       SUM(EMPTYNULLCrimeOffOtherwiseDealtWith)                  AS EMPTYNULLCrimeOffOtherwiseDealtWith,           \
                       SUM(EMPTYNULLCrimeOffSentPrison)                          AS EMPTYNULLCrimeOffSentPrison,                   \
                       SUM(EMPTYNULLCrimeSuspectChargedPartOfAnotherCase)        AS EMPTYNULLCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(EMPTYNULLCrimeUnableProsecuteSuspect)                 AS EMPTYNULLCrimeUnableProsecuteSuspect,          \
                       SUM(EMPTYNULLCrimeUnderInvestigation)                     AS EMPTYNULLCrimeUnderInvestigation,              \
                       \
                       SUM(AntiSocialBehaviorEMPTYNULLOutcome)                   AS AntiSocialBehaviorEMPTYNULLOutcome,                SUM(BicycleTheftEMPTYNULLOutcome)                          AS BicycleTheftEMPTYNULLOutcome,                \
                       SUM(AntiSocialBehaviorActionToBeTakenOtherOrg)            AS AntiSocialBehaviorActionToBeTakenOtherOrg,         SUM(BicycleTheftActionToBeTakenOtherOrg)                   AS BicycleTheftActionToBeTakenOtherOrg,         \
                       SUM(AntiSocialBehaviorAwaitingCourtOutcome)               AS AntiSocialBehaviorAwaitingCourtOutcome,            SUM(BicycleTheftAwaitingCourtOutcome)                      AS BicycleTheftAwaitingCourtOutcome,            \
                       SUM(AntiSocialBehaviorCourtCaseUnableToProceed)           AS AntiSocialBehaviorCourtCaseUnableToProceed,        SUM(BicycleTheftCourtCaseUnableToProceed)                  AS BicycleTheftCourtCaseUnableToProceed,        \
                       SUM(AntiSocialBehaviorCourtResultUnavailable)             AS AntiSocialBehaviorCourtResultUnavailable,          SUM(BicycleTheftCourtResultUnavailable)                    AS BicycleTheftCourtResultUnavailable,          \
                       SUM(AntiSocialBehaviorDefendantNotGuilty)                 AS AntiSocialBehaviorDefendantNotGuilty,              SUM(BicycleTheftDefendantNotGuilty)                        AS BicycleTheftDefendantNotGuilty,              \
                       SUM(AntiSocialBehaviorDefendantSentCrownCourt)            AS AntiSocialBehaviorDefendantSentCrownCourt,         SUM(BicycleTheftDefendantSentCrownCourt)                   AS BicycleTheftDefendantSentCrownCourt,         \
                       SUM(AntiSocialBehaviorFormalActionNotPublicInterest)      AS AntiSocialBehaviorFormalActionNotPublicInterest,   SUM(BicycleTheftFormalActionNotPublicInterest)             AS BicycleTheftFormalActionNotPublicInterest,   \
                       SUM(AntiSocialBehaviorInvestigationCompleteNoSuspect)     AS AntiSocialBehaviorInvestigationCompleteNoSuspect,  SUM(BicycleTheftInvestigationCompleteNoSuspect)            AS BicycleTheftInvestigationCompleteNoSuspect,  \
                       SUM(AntiSocialBehaviorLocalResolution)                    AS AntiSocialBehaviorLocalResolution,                 SUM(BicycleTheftLocalResolution)                           AS BicycleTheftLocalResolution,                 \
                       SUM(AntiSocialBehaviorOffDeprivedProperty)                AS AntiSocialBehaviorOffDeprivedProperty,             SUM(BicycleTheftOffDeprivedProperty)                       AS BicycleTheftOffDeprivedProperty,             \
                       SUM(AntiSocialBehaviorOffFined)                           AS AntiSocialBehaviorOffFined,                        SUM(BicycleTheftOffFined)                                  AS BicycleTheftOffFined,                        \
                       SUM(AntiSocialBehaviorOffGivenCaution)                    AS AntiSocialBehaviorOffGivenCaution,                 SUM(BicycleTheftOffGivenCaution)                           AS BicycleTheftOffGivenCaution,                 \
                       SUM(AntiSocialBehaviorOffGivenDrugsPossessionWarning)     AS AntiSocialBehaviorOffGivenDrugsPossessionWarning,  SUM(BicycleTheftOffGivenDrugsPossessionWarning)            AS BicycleTheftOffGivenDrugsPossessionWarning,  \
                       SUM(AntiSocialBehaviorOffGivenAbsoluteDischarge)          AS AntiSocialBehaviorOffGivenAbsoluteDischarge,       SUM(BicycleTheftOffGivenAbsoluteDischarge)                 AS BicycleTheftOffGivenAbsoluteDischarge,       \
                       SUM(AntiSocialBehaviorOffGivenCommunitySentence)          AS AntiSocialBehaviorOffGivenCommunitySentence,       SUM(BicycleTheftOffGivenCommunitySentence)                 AS BicycleTheftOffGivenCommunitySentence,       \
                       SUM(AntiSocialBehaviorOffGivenConditionalDischarge)       AS AntiSocialBehaviorOffGivenConditionalDischarge,    SUM(BicycleTheftOffGivenConditionalDischarge)              AS BicycleTheftOffGivenConditionalDischarge,    \
                       SUM(AntiSocialBehaviorOffGivenPenaltyNotice)              AS AntiSocialBehaviorOffGivenPenaltyNotice,           SUM(BicycleTheftOffGivenPenaltyNotice)                     AS BicycleTheftOffGivenPenaltyNotice,           \
                       SUM(AntiSocialBehaviorOffGivenSuspendedPrisonSentence)    AS AntiSocialBehaviorOffGivenSuspendedPrisonSentence, SUM(BicycleTheftOffGivenSuspendedPrisonSentence)           AS BicycleTheftOffGivenSuspendedPrisonSentence, \
                       SUM(AntiSocialBehaviorOffOrderedPayCompensation)          AS AntiSocialBehaviorOffOrderedPayCompensation,       SUM(BicycleTheftOffOrderedPayCompensation)                 AS BicycleTheftOffOrderedPayCompensation,       \
                       SUM(AntiSocialBehaviorOffOtherwiseDealtWith)              AS AntiSocialBehaviorOffOtherwiseDealtWith,           SUM(BicycleTheftOffOtherwiseDealtWith)                     AS BicycleTheftOffOtherwiseDealtWith,           \
                       SUM(AntiSocialBehaviorOffSentPrison)                      AS AntiSocialBehaviorOffSentPrison,                   SUM(BicycleTheftOffSentPrison)                             AS BicycleTheftOffSentPrison,                   \
                       SUM(AntiSocialBehaviorSuspectChargedPartOfAnotherCase)    AS AntiSocialBehaviorSuspectChargedPartOfAnotherCase, SUM(BicycleTheftSuspectChargedPartOfAnotherCase)           AS BicycleTheftSuspectChargedPartOfAnotherCase, \
                       SUM(AntiSocialBehaviorUnableProsecuteSuspect)             AS AntiSocialBehaviorUnableProsecuteSuspect,          SUM(BicycleTheftUnableProsecuteSuspect)                    AS BicycleTheftUnableProsecuteSuspect,          \
                       SUM(AntiSocialBehaviorUnderInvestigation)                 AS AntiSocialBehaviorUnderInvestigation,              SUM(BicycleTheftUnderInvestigation)                        AS BicycleTheftUnderInvestigation,              \
                       \
                       SUM(BurglaryEMPTYNULLOutcome)                             AS BurglaryEMPTYNULLOutcome,                          SUM(CriminalDamageArsonEMPTYNULLOutcome)                   AS CriminalDamageArsonEMPTYNULLOutcome,                \
                       SUM(BurglaryActionToBeTakenOtherOrg)                      AS BurglaryActionToBeTakenOtherOrg,                   SUM(CriminalDamageArsonActionToBeTakenOtherOrg)            AS CriminalDamageArsonActionToBeTakenOtherOrg,         \
                       SUM(BurglaryAwaitingCourtOutcome)                         AS BurglaryAwaitingCourtOutcome,                      SUM(CriminalDamageArsonAwaitingCourtOutcome)               AS CriminalDamageArsonAwaitingCourtOutcome,            \
                       SUM(BurglaryCourtCaseUnableToProceed)                     AS BurglaryCourtCaseUnableToProceed,                  SUM(CriminalDamageArsonCourtCaseUnableToProceed)           AS CriminalDamageArsonCourtCaseUnableToProceed,        \
                       SUM(BurglaryCourtResultUnavailable)                       AS BurglaryCourtResultUnavailable,                    SUM(CriminalDamageArsonCourtResultUnavailable)             AS CriminalDamageArsonCourtResultUnavailable,          \
                       SUM(BurglaryDefendantNotGuilty)                           AS BurglaryDefendantNotGuilty,                        SUM(CriminalDamageArsonDefendantNotGuilty)                 AS CriminalDamageArsonDefendantNotGuilty,              \
                       SUM(BurglaryDefendantSentCrownCourt)                      AS BurglaryDefendantSentCrownCourt,                   SUM(CriminalDamageArsonDefendantSentCrownCourt)            AS CriminalDamageArsonDefendantSentCrownCourt,         \
                       SUM(BurglaryFormalActionNotPublicInterest)                AS BurglaryFormalActionNotPublicInterest,             SUM(CriminalDamageArsonFormalActionNotPublicInterest)      AS CriminalDamageArsonFormalActionNotPublicInterest,   \
                       SUM(BurglaryInvestigationCompleteNoSuspect)               AS BurglaryInvestigationCompleteNoSuspect,            SUM(CriminalDamageArsonInvestigationCompleteNoSuspect)     AS CriminalDamageArsonInvestigationCompleteNoSuspect,  \
                       SUM(BurglaryLocalResolution)                              AS BurglaryLocalResolution,                           SUM(CriminalDamageArsonLocalResolution)                    AS CriminalDamageArsonLocalResolution,                 \
                       SUM(BurglaryOffDeprivedProperty)                          AS BurglaryOffDeprivedProperty,                       SUM(CriminalDamageArsonOffDeprivedProperty)                AS CriminalDamageArsonOffDeprivedProperty,             \
                       SUM(BurglaryOffFined)                                     AS BurglaryOffFined,                                  SUM(CriminalDamageArsonOffFined)                           AS CriminalDamageArsonOffFined,                        \
                       SUM(BurglaryOffGivenCaution)                              AS BurglaryOffGivenCaution,                           SUM(CriminalDamageArsonOffGivenCaution)                    AS CriminalDamageArsonOffGivenCaution,                 \
                       SUM(BurglaryOffGivenDrugsPossessionWarning)               AS BurglaryOffGivenDrugsPossessionWarning,            SUM(CriminalDamageArsonOffGivenDrugsPossessionWarning)     AS CriminalDamageArsonOffGivenDrugsPossessionWarning,  \
                       SUM(BurglaryOffGivenAbsoluteDischarge)                    AS BurglaryOffGivenAbsoluteDischarge,                 SUM(CriminalDamageArsonOffGivenAbsoluteDischarge)          AS CriminalDamageArsonOffGivenAbsoluteDischarge,       \
                       SUM(BurglaryOffGivenCommunitySentence)                    AS BurglaryOffGivenCommunitySentence,                 SUM(CriminalDamageArsonOffGivenCommunitySentence)          AS CriminalDamageArsonOffGivenCommunitySentence,       \
                       SUM(BurglaryOffGivenConditionalDischarge)                 AS BurglaryOffGivenConditionalDischarge,              SUM(CriminalDamageArsonOffGivenConditionalDischarge)       AS CriminalDamageArsonOffGivenConditionalDischarge,    \
                       SUM(BurglaryOffGivenPenaltyNotice)                        AS BurglaryOffGivenPenaltyNotice,                     SUM(CriminalDamageArsonOffGivenPenaltyNotice)              AS CriminalDamageArsonOffGivenPenaltyNotice,           \
                       SUM(BurglaryOffGivenSuspendedPrisonSentence)              AS BurglaryOffGivenSuspendedPrisonSentence,           SUM(CriminalDamageArsonOffGivenSuspendedPrisonSentence)    AS CriminalDamageArsonOffGivenSuspendedPrisonSentence, \
                       SUM(BurglaryOffOrderedPayCompensation)                    AS BurglaryOffOrderedPayCompensation,                 SUM(CriminalDamageArsonOffOrderedPayCompensation)          AS CriminalDamageArsonOffOrderedPayCompensation,       \
                       SUM(BurglaryOffOtherwiseDealtWith)                        AS BurglaryOffOtherwiseDealtWith,                     SUM(CriminalDamageArsonOffOtherwiseDealtWith)              AS CriminalDamageArsonOffOtherwiseDealtWith,           \
                       SUM(BurglaryOffSentPrison)                                AS BurglaryOffSentPrison,                             SUM(CriminalDamageArsonOffSentPrison)                      AS CriminalDamageArsonOffSentPrison,                   \
                       SUM(BurglarySuspectChargedPartOfAnotherCase)              AS BurglarySuspectChargedPartOfAnotherCase,           SUM(CriminalDamageArsonSuspectChargedPartOfAnotherCase)    AS CriminalDamageArsonSuspectChargedPartOfAnotherCase, \
                       SUM(BurglaryUnableProsecuteSuspect)                       AS BurglaryUnableProsecuteSuspect,                    SUM(CriminalDamageArsonUnableProsecuteSuspect)             AS CriminalDamageArsonUnableProsecuteSuspect,          \
                       SUM(BurglaryUnderInvestigation)                           AS BurglaryUnderInvestigation,                        SUM(CriminalDamageArsonUnderInvestigation)                 AS CriminalDamageArsonUnderInvestigation,              \
                       \
                       SUM(DrugsEMPTYNULLOutcome)                                AS DrugsEMPTYNULLOutcome,                             SUM(OtherCrimeEMPTYNULLOutcome)                            AS OtherCrimeEMPTYNULLOutcome,                \
                       SUM(DrugsActionToBeTakenOtherOrg)                         AS DrugsActionToBeTakenOtherOrg,                      SUM(OtherCrimeActionToBeTakenOtherOrg)                     AS OtherCrimeActionToBeTakenOtherOrg,         \
                       SUM(DrugsAwaitingCourtOutcome)                            AS DrugsAwaitingCourtOutcome,                         SUM(OtherCrimeAwaitingCourtOutcome)                        AS OtherCrimeAwaitingCourtOutcome,            \
                       SUM(DrugsCourtCaseUnableToProceed)                        AS DrugsCourtCaseUnableToProceed,                     SUM(OtherCrimeCourtCaseUnableToProceed)                    AS OtherCrimeCourtCaseUnableToProceed,        \
                       SUM(DrugsCourtResultUnavailable)                          AS DrugsCourtResultUnavailable,                       SUM(OtherCrimeCourtResultUnavailable)                      AS OtherCrimeCourtResultUnavailable,          \
                       SUM(DrugsDefendantNotGuilty)                              AS DrugsDefendantNotGuilty,                           SUM(OtherCrimeDefendantNotGuilty)                          AS OtherCrimeDefendantNotGuilty,              \
                       SUM(DrugsDefendantSentCrownCourt)                         AS DrugsDefendantSentCrownCourt,                      SUM(OtherCrimeDefendantSentCrownCourt)                     AS OtherCrimeDefendantSentCrownCourt,         \
                       SUM(DrugsFormalActionNotPublicInterest)                   AS DrugsFormalActionNotPublicInterest,                SUM(OtherCrimeFormalActionNotPublicInterest)               AS OtherCrimeFormalActionNotPublicInterest,   \
                       SUM(DrugsInvestigationCompleteNoSuspect)                  AS DrugsInvestigationCompleteNoSuspect,               SUM(OtherCrimeInvestigationCompleteNoSuspect)              AS OtherCrimeInvestigationCompleteNoSuspect,  \
                       SUM(DrugsLocalResolution)                                 AS DrugsLocalResolution,                              SUM(OtherCrimeLocalResolution)                             AS OtherCrimeLocalResolution,                 \
                       SUM(DrugsOffDeprivedProperty)                             AS DrugsOffDeprivedProperty,                          SUM(OtherCrimeOffDeprivedProperty)                         AS OtherCrimeOffDeprivedProperty,             \
                       SUM(DrugsOffFined)                                        AS DrugsOffFined,                                     SUM(OtherCrimeOffFined)                                    AS OtherCrimeOffFined,                        \
                       SUM(DrugsOffGivenCaution)                                 AS DrugsOffGivenCaution,                              SUM(OtherCrimeOffGivenCaution)                             AS OtherCrimeOffGivenCaution,                 \
                       SUM(DrugsOffGivenDrugsPossessionWarning)                  AS DrugsOffGivenDrugsPossessionWarning,               SUM(OtherCrimeOffGivenDrugsPossessionWarning)              AS OtherCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(DrugsOffGivenAbsoluteDischarge)                       AS DrugsOffGivenAbsoluteDischarge,                    SUM(OtherCrimeOffGivenAbsoluteDischarge)                   AS OtherCrimeOffGivenAbsoluteDischarge,       \
                       SUM(DrugsOffGivenCommunitySentence)                       AS DrugsOffGivenCommunitySentence,                    SUM(OtherCrimeOffGivenCommunitySentence)                   AS OtherCrimeOffGivenCommunitySentence,       \
                       SUM(DrugsOffGivenConditionalDischarge)                    AS DrugsOffGivenConditionalDischarge,                 SUM(OtherCrimeOffGivenConditionalDischarge)                AS OtherCrimeOffGivenConditionalDischarge,    \
                       SUM(DrugsOffGivenPenaltyNotice)                           AS DrugsOffGivenPenaltyNotice,                        SUM(OtherCrimeOffGivenPenaltyNotice)                       AS OtherCrimeOffGivenPenaltyNotice,           \
                       SUM(DrugsOffGivenSuspendedPrisonSentence)                 AS DrugsOffGivenSuspendedPrisonSentence,              SUM(OtherCrimeOffGivenSuspendedPrisonSentence)             AS OtherCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(DrugsOffOrderedPayCompensation)                       AS DrugsOffOrderedPayCompensation,                    SUM(OtherCrimeOffOrderedPayCompensation)                   AS OtherCrimeOffOrderedPayCompensation,       \
                       SUM(DrugsOffOtherwiseDealtWith)                           AS DrugsOffOtherwiseDealtWith,                        SUM(OtherCrimeOffOtherwiseDealtWith)                       AS OtherCrimeOffOtherwiseDealtWith,           \
                       SUM(DrugsOffSentPrison)                                   AS DrugsOffSentPrison,                                SUM(OtherCrimeOffSentPrison)                               AS OtherCrimeOffSentPrison,                   \
                       SUM(DrugsSuspectChargedPartOfAnotherCase)                 AS DrugsSuspectChargedPartOfAnotherCase,              SUM(OtherCrimeSuspectChargedPartOfAnotherCase)             AS OtherCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(DrugsUnableProsecuteSuspect)                          AS DrugsUnableProsecuteSuspect,                       SUM(OtherCrimeUnableProsecuteSuspect)                      AS OtherCrimeUnableProsecuteSuspect,          \
                       SUM(DrugsUnderInvestigation)                              AS DrugsUnderInvestigation,                           SUM(OtherCrimeUnderInvestigation)                          AS OtherCrimeUnderInvestigation,              \
                       \
                       SUM(OtherTheftEMPTYNULLOutcome)                           AS OtherTheftEMPTYNULLOutcome,                        SUM(PossessionWeaponsEMPTYNULLOutcome)                     AS PossessionWeaponsEMPTYNULLOutcome,                \
                       SUM(OtherTheftActionToBeTakenOtherOrg)                    AS OtherTheftActionToBeTakenOtherOrg,                 SUM(PossessionWeaponsActionToBeTakenOtherOrg)              AS PossessionWeaponsActionToBeTakenOtherOrg,         \
                       SUM(OtherTheftAwaitingCourtOutcome)                       AS OtherTheftAwaitingCourtOutcome,                    SUM(PossessionWeaponsAwaitingCourtOutcome)                 AS PossessionWeaponsAwaitingCourtOutcome,            \
                       SUM(OtherTheftCourtCaseUnableToProceed)                   AS OtherTheftCourtCaseUnableToProceed,                SUM(PossessionWeaponsCourtCaseUnableToProceed)             AS PossessionWeaponsCourtCaseUnableToProceed,        \
                       SUM(OtherTheftCourtResultUnavailable)                     AS OtherTheftCourtResultUnavailable,                  SUM(PossessionWeaponsCourtResultUnavailable)               AS PossessionWeaponsCourtResultUnavailable,          \
                       SUM(OtherTheftDefendantNotGuilty)                         AS OtherTheftDefendantNotGuilty,                      SUM(PossessionWeaponsDefendantNotGuilty)                   AS PossessionWeaponsDefendantNotGuilty,              \
                       SUM(OtherTheftDefendantSentCrownCourt)                    AS OtherTheftDefendantSentCrownCourt,                 SUM(PossessionWeaponsDefendantSentCrownCourt)              AS PossessionWeaponsDefendantSentCrownCourt,         \
                       SUM(OtherTheftFormalActionNotPublicInterest)              AS OtherTheftFormalActionNotPublicInterest,           SUM(PossessionWeaponsFormalActionNotPublicInterest)        AS PossessionWeaponsFormalActionNotPublicInterest,   \
                       SUM(OtherTheftInvestigationCompleteNoSuspect)             AS OtherTheftInvestigationCompleteNoSuspect,          SUM(PossessionWeaponsInvestigationCompleteNoSuspect)       AS PossessionWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(OtherTheftLocalResolution)                            AS OtherTheftLocalResolution,                         SUM(PossessionWeaponsLocalResolution)                      AS PossessionWeaponsLocalResolution,                 \
                       SUM(OtherTheftOffDeprivedProperty)                        AS OtherTheftOffDeprivedProperty,                     SUM(PossessionWeaponsOffDeprivedProperty)                  AS PossessionWeaponsOffDeprivedProperty,             \
                       SUM(OtherTheftOffFined)                                   AS OtherTheftOffFined,                                SUM(PossessionWeaponsOffFined)                             AS PossessionWeaponsOffFined,                        \
                       SUM(OtherTheftOffGivenCaution)                            AS OtherTheftOffGivenCaution,                         SUM(PossessionWeaponsOffGivenCaution)                      AS PossessionWeaponsOffGivenCaution,                 \
                       SUM(OtherTheftOffGivenDrugsPossessionWarning)             AS OtherTheftOffGivenDrugsPossessionWarning,          SUM(PossessionWeaponsOffGivenDrugsPossessionWarning)       AS PossessionWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(OtherTheftOffGivenAbsoluteDischarge)                  AS OtherTheftOffGivenAbsoluteDischarge,               SUM(PossessionWeaponsOffGivenAbsoluteDischarge)            AS PossessionWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(OtherTheftOffGivenCommunitySentence)                  AS OtherTheftOffGivenCommunitySentence,               SUM(PossessionWeaponsOffGivenCommunitySentence)            AS PossessionWeaponsOffGivenCommunitySentence,       \
                       SUM(OtherTheftOffGivenConditionalDischarge)               AS OtherTheftOffGivenConditionalDischarge,            SUM(PossessionWeaponsOffGivenConditionalDischarge)         AS PossessionWeaponsOffGivenConditionalDischarge,    \
                       SUM(OtherTheftOffGivenPenaltyNotice)                      AS OtherTheftOffGivenPenaltyNotice,                   SUM(PossessionWeaponsOffGivenPenaltyNotice)                AS PossessionWeaponsOffGivenPenaltyNotice,           \
                       SUM(OtherTheftOffGivenSuspendedPrisonSentence)            AS OtherTheftOffGivenSuspendedPrisonSentence,         SUM(PossessionWeaponsOffGivenSuspendedPrisonSentence)      AS PossessionWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(OtherTheftOffOrderedPayCompensation)                  AS OtherTheftOffOrderedPayCompensation,               SUM(PossessionWeaponsOffOrderedPayCompensation)            AS PossessionWeaponsOffOrderedPayCompensation,       \
                       SUM(OtherTheftOffOtherwiseDealtWith)                      AS OtherTheftOffOtherwiseDealtWith,                   SUM(PossessionWeaponsOffOtherwiseDealtWith)                AS PossessionWeaponsOffOtherwiseDealtWith,           \
                       SUM(OtherTheftOffSentPrison)                              AS OtherTheftOffSentPrison,                           SUM(PossessionWeaponsOffSentPrison)                        AS PossessionWeaponsOffSentPrison,                   \
                       SUM(OtherTheftSuspectChargedPartOfAnotherCase)            AS OtherTheftSuspectChargedPartOfAnotherCase,         SUM(PossessionWeaponsSuspectChargedPartOfAnotherCase)      AS PossessionWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(OtherTheftUnableProsecuteSuspect)                     AS OtherTheftUnableProsecuteSuspect,                  SUM(PossessionWeaponsUnableProsecuteSuspect)               AS PossessionWeaponsUnableProsecuteSuspect,          \
                       SUM(OtherTheftUnderInvestigation)                         AS OtherTheftUnderInvestigation,                      SUM(PossessionWeaponsUnderInvestigation)                   AS PossessionWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicDisorderWeaponsEMPTYNULLOutcome)                AS PublicDisorderWeaponsEMPTYNULLOutcome,                \
                       SUM(PublicDisorderWeaponsActionToBeTakenOtherOrg)         AS PublicDisorderWeaponsActionToBeTakenOtherOrg,         \
                       SUM(PublicDisorderWeaponsAwaitingCourtOutcome)            AS PublicDisorderWeaponsAwaitingCourtOutcome,            \
                       SUM(PublicDisorderWeaponsCourtCaseUnableToProceed)        AS PublicDisorderWeaponsCourtCaseUnableToProceed,        \
                       SUM(PublicDisorderWeaponsCourtResultUnavailable)          AS PublicDisorderWeaponsCourtResultUnavailable,          \
                       SUM(PublicDisorderWeaponsDefendantNotGuilty)              AS PublicDisorderWeaponsDefendantNotGuilty,              \
                       SUM(PublicDisorderWeaponsDefendantSentCrownCourt)         AS PublicDisorderWeaponsDefendantSentCrownCourt,         \
                       SUM(PublicDisorderWeaponsFormalActionNotPublicInterest)   AS PublicDisorderWeaponsFormalActionNotPublicInterest,   \
                       SUM(PublicDisorderWeaponsInvestigationCompleteNoSuspect)  AS PublicDisorderWeaponsInvestigationCompleteNoSuspect,  \
                       SUM(PublicDisorderWeaponsLocalResolution)                 AS PublicDisorderWeaponsLocalResolution,                 \
                       SUM(PublicDisorderWeaponsOffDeprivedProperty)             AS PublicDisorderWeaponsOffDeprivedProperty,             \
                       SUM(PublicDisorderWeaponsOffFined)                        AS PublicDisorderWeaponsOffFined,                        \
                       SUM(PublicDisorderWeaponsOffGivenCaution)                 AS PublicDisorderWeaponsOffGivenCaution,                 \
                       SUM(PublicDisorderWeaponsOffGivenDrugsPossessionWarning)  AS PublicDisorderWeaponsOffGivenDrugsPossessionWarning,  \
                       SUM(PublicDisorderWeaponsOffGivenAbsoluteDischarge)       AS PublicDisorderWeaponsOffGivenAbsoluteDischarge,       \
                       SUM(PublicDisorderWeaponsOffGivenCommunitySentence)       AS PublicDisorderWeaponsOffGivenCommunitySentence,       \
                       SUM(PublicDisorderWeaponsOffGivenConditionalDischarge)    AS PublicDisorderWeaponsOffGivenConditionalDischarge,    \
                       SUM(PublicDisorderWeaponsOffGivenPenaltyNotice)           AS PublicDisorderWeaponsOffGivenPenaltyNotice,           \
                       SUM(PublicDisorderWeaponsOffGivenSuspendedPrisonSentence) AS PublicDisorderWeaponsOffGivenSuspendedPrisonSentence, \
                       SUM(PublicDisorderWeaponsOffOrderedPayCompensation)       AS PublicDisorderWeaponsOffOrderedPayCompensation,       \
                       SUM(PublicDisorderWeaponsOffOtherwiseDealtWith)           AS PublicDisorderWeaponsOffOtherwiseDealtWith,           \
                       SUM(PublicDisorderWeaponsOffSentPrison)                   AS PublicDisorderWeaponsOffSentPrison,                   \
                       SUM(PublicDisorderWeaponsSuspectChargedPartOfAnotherCase) AS PublicDisorderWeaponsSuspectChargedPartOfAnotherCase, \
                       SUM(PublicDisorderWeaponsUnableProsecuteSuspect)          AS PublicDisorderWeaponsUnableProsecuteSuspect,          \
                       SUM(PublicDisorderWeaponsUnderInvestigation)              AS PublicDisorderWeaponsUnderInvestigation,              \
                       \
                       SUM(PublicOrderEMPTYNULLOutcome)                          AS PublicOrderEMPTYNULLOutcome,                       SUM(RobberyEMPTYNULLOutcome)                               AS RobberyEMPTYNULLOutcome,                \
                       SUM(PublicOrderActionToBeTakenOtherOrg)                   AS PublicOrderActionToBeTakenOtherOrg,                SUM(RobberyActionToBeTakenOtherOrg)                        AS RobberyActionToBeTakenOtherOrg,         \
                       SUM(PublicOrderAwaitingCourtOutcome)                      AS PublicOrderAwaitingCourtOutcome,                   SUM(RobberyAwaitingCourtOutcome)                           AS RobberyAwaitingCourtOutcome,            \
                       SUM(PublicOrderCourtCaseUnableToProceed)                  AS PublicOrderCourtCaseUnableToProceed,               SUM(RobberyCourtCaseUnableToProceed)                       AS RobberyCourtCaseUnableToProceed,        \
                       SUM(PublicOrderCourtResultUnavailable)                    AS PublicOrderCourtResultUnavailable,                 SUM(RobberyCourtResultUnavailable)                         AS RobberyCourtResultUnavailable,          \
                       SUM(PublicOrderDefendantNotGuilty)                        AS PublicOrderDefendantNotGuilty,                     SUM(RobberyDefendantNotGuilty)                             AS RobberyDefendantNotGuilty,              \
                       SUM(PublicOrderDefendantSentCrownCourt)                   AS PublicOrderDefendantSentCrownCourt,                SUM(RobberyDefendantSentCrownCourt)                        AS RobberyDefendantSentCrownCourt,         \
                       SUM(PublicOrderFormalActionNotPublicInterest)             AS PublicOrderFormalActionNotPublicInterest,          SUM(RobberyFormalActionNotPublicInterest)                  AS RobberyFormalActionNotPublicInterest,   \
                       SUM(PublicOrderInvestigationCompleteNoSuspect)            AS PublicOrderInvestigationCompleteNoSuspect,         SUM(RobberyInvestigationCompleteNoSuspect)                 AS RobberyInvestigationCompleteNoSuspect,  \
                       SUM(PublicOrderLocalResolution)                           AS PublicOrderLocalResolution,                        SUM(RobberyLocalResolution)                                AS RobberyLocalResolution,                 \
                       SUM(PublicOrderOffDeprivedProperty)                       AS PublicOrderOffDeprivedProperty,                    SUM(RobberyOffDeprivedProperty)                            AS RobberyOffDeprivedProperty,             \
                       SUM(PublicOrderOffFined)                                  AS PublicOrderOffFined,                               SUM(RobberyOffFined)                                       AS RobberyOffFined,                        \
                       SUM(PublicOrderOffGivenCaution)                           AS PublicOrderOffGivenCaution,                        SUM(RobberyOffGivenCaution)                                AS RobberyOffGivenCaution,                 \
                       SUM(PublicOrderOffGivenDrugsPossessionWarning)            AS PublicOrderOffGivenDrugsPossessionWarning,         SUM(RobberyOffGivenDrugsPossessionWarning)                 AS RobberyOffGivenDrugsPossessionWarning,  \
                       SUM(PublicOrderOffGivenAbsoluteDischarge)                 AS PublicOrderOffGivenAbsoluteDischarge,              SUM(RobberyOffGivenAbsoluteDischarge)                      AS RobberyOffGivenAbsoluteDischarge,       \
                       SUM(PublicOrderOffGivenCommunitySentence)                 AS PublicOrderOffGivenCommunitySentence,              SUM(RobberyOffGivenCommunitySentence)                      AS RobberyOffGivenCommunitySentence,       \
                       SUM(PublicOrderOffGivenConditionalDischarge)              AS PublicOrderOffGivenConditionalDischarge,           SUM(RobberyOffGivenConditionalDischarge)                   AS RobberyOffGivenConditionalDischarge,    \
                       SUM(PublicOrderOffGivenPenaltyNotice)                     AS PublicOrderOffGivenPenaltyNotice,                  SUM(RobberyOffGivenPenaltyNotice)                          AS RobberyOffGivenPenaltyNotice,           \
                       SUM(PublicOrderOffGivenSuspendedPrisonSentence)           AS PublicOrderOffGivenSuspendedPrisonSentence,        SUM(RobberyOffGivenSuspendedPrisonSentence)                AS RobberyOffGivenSuspendedPrisonSentence, \
                       SUM(PublicOrderOffOrderedPayCompensation)                 AS PublicOrderOffOrderedPayCompensation,              SUM(RobberyOffOrderedPayCompensation)                      AS RobberyOffOrderedPayCompensation,       \
                       SUM(PublicOrderOffOtherwiseDealtWith)                     AS PublicOrderOffOtherwiseDealtWith,                  SUM(RobberyOffOtherwiseDealtWith)                          AS RobberyOffOtherwiseDealtWith,           \
                       SUM(PublicOrderOffSentPrison)                             AS PublicOrderOffSentPrison,                          SUM(RobberyOffSentPrison)                                  AS RobberyOffSentPrison,                   \
                       SUM(PublicOrderSuspectChargedPartOfAnotherCase)           AS PublicOrderSuspectChargedPartOfAnotherCase,        SUM(RobberySuspectChargedPartOfAnotherCase)                AS RobberySuspectChargedPartOfAnotherCase, \
                       SUM(PublicOrderUnableProsecuteSuspect)                    AS PublicOrderUnableProsecuteSuspect,                 SUM(RobberyUnableProsecuteSuspect)                         AS RobberyUnableProsecuteSuspect,          \
                       SUM(PublicOrderUnderInvestigation)                        AS PublicOrderUnderInvestigation,                     SUM(RobberyUnderInvestigation)                             AS RobberyUnderInvestigation,              \
                       \
                       SUM(ShopliftingEMPTYNULLOutcome)                          AS ShopliftingEMPTYNULLOutcome,                       SUM(TheftFromPersonEMPTYNULLOutcome)                       AS TheftFromPersonEMPTYNULLOutcome,                \
                       SUM(ShopliftingActionToBeTakenOtherOrg)                   AS ShopliftingActionToBeTakenOtherOrg,                SUM(TheftFromPersonActionToBeTakenOtherOrg)                AS TheftFromPersonActionToBeTakenOtherOrg,         \
                       SUM(ShopliftingAwaitingCourtOutcome)                      AS ShopliftingAwaitingCourtOutcome,                   SUM(TheftFromPersonAwaitingCourtOutcome)                   AS TheftFromPersonAwaitingCourtOutcome,            \
                       SUM(ShopliftingCourtCaseUnableToProceed)                  AS ShopliftingCourtCaseUnableToProceed,               SUM(TheftFromPersonCourtCaseUnableToProceed)               AS TheftFromPersonCourtCaseUnableToProceed,        \
                       SUM(ShopliftingCourtResultUnavailable)                    AS ShopliftingCourtResultUnavailable,                 SUM(TheftFromPersonCourtResultUnavailable)                 AS TheftFromPersonCourtResultUnavailable,          \
                       SUM(ShopliftingDefendantNotGuilty)                        AS ShopliftingDefendantNotGuilty,                     SUM(TheftFromPersonDefendantNotGuilty)                     AS TheftFromPersonDefendantNotGuilty,              \
                       SUM(ShopliftingDefendantSentCrownCourt)                   AS ShopliftingDefendantSentCrownCourt,                SUM(TheftFromPersonDefendantSentCrownCourt)                AS TheftFromPersonDefendantSentCrownCourt,         \
                       SUM(ShopliftingFormalActionNotPublicInterest)             AS ShopliftingFormalActionNotPublicInterest,          SUM(TheftFromPersonFormalActionNotPublicInterest)          AS TheftFromPersonFormalActionNotPublicInterest,   \
                       SUM(ShopliftingInvestigationCompleteNoSuspect)            AS ShopliftingInvestigationCompleteNoSuspect,         SUM(TheftFromPersonInvestigationCompleteNoSuspect)         AS TheftFromPersonInvestigationCompleteNoSuspect,  \
                       SUM(ShopliftingLocalResolution)                           AS ShopliftingLocalResolution,                        SUM(TheftFromPersonLocalResolution)                        AS TheftFromPersonLocalResolution,                 \
                       SUM(ShopliftingOffDeprivedProperty)                       AS ShopliftingOffDeprivedProperty,                    SUM(TheftFromPersonOffDeprivedProperty)                    AS TheftFromPersonOffDeprivedProperty,             \
                       SUM(ShopliftingOffFined)                                  AS ShopliftingOffFined,                               SUM(TheftFromPersonOffFined)                               AS TheftFromPersonOffFined,                        \
                       SUM(ShopliftingOffGivenCaution)                           AS ShopliftingOffGivenCaution,                        SUM(TheftFromPersonOffGivenCaution)                        AS TheftFromPersonOffGivenCaution,                 \
                       SUM(ShopliftingOffGivenDrugsPossessionWarning)            AS ShopliftingOffGivenDrugsPossessionWarning,         SUM(TheftFromPersonOffGivenDrugsPossessionWarning)         AS TheftFromPersonOffGivenDrugsPossessionWarning,  \
                       SUM(ShopliftingOffGivenAbsoluteDischarge)                 AS ShopliftingOffGivenAbsoluteDischarge,              SUM(TheftFromPersonOffGivenAbsoluteDischarge)              AS TheftFromPersonOffGivenAbsoluteDischarge,       \
                       SUM(ShopliftingOffGivenCommunitySentence)                 AS ShopliftingOffGivenCommunitySentence,              SUM(TheftFromPersonOffGivenCommunitySentence)              AS TheftFromPersonOffGivenCommunitySentence,       \
                       SUM(ShopliftingOffGivenConditionalDischarge)              AS ShopliftingOffGivenConditionalDischarge,           SUM(TheftFromPersonOffGivenConditionalDischarge)           AS TheftFromPersonOffGivenConditionalDischarge,    \
                       SUM(ShopliftingOffGivenPenaltyNotice)                     AS ShopliftingOffGivenPenaltyNotice,                  SUM(TheftFromPersonOffGivenPenaltyNotice)                  AS TheftFromPersonOffGivenPenaltyNotice,           \
                       SUM(ShopliftingOffGivenSuspendedPrisonSentence)           AS ShopliftingOffGivenSuspendedPrisonSentence,        SUM(TheftFromPersonOffGivenSuspendedPrisonSentence)        AS TheftFromPersonOffGivenSuspendedPrisonSentence, \
                       SUM(ShopliftingOffOrderedPayCompensation)                 AS ShopliftingOffOrderedPayCompensation,              SUM(TheftFromPersonOffOrderedPayCompensation)              AS TheftFromPersonOffOrderedPayCompensation,       \
                       SUM(ShopliftingOffOtherwiseDealtWith)                     AS ShopliftingOffOtherwiseDealtWith,                  SUM(TheftFromPersonOffOtherwiseDealtWith)                  AS TheftFromPersonOffOtherwiseDealtWith,           \
                       SUM(ShopliftingOffSentPrison)                             AS ShopliftingOffSentPrison,                          SUM(TheftFromPersonOffSentPrison)                          AS TheftFromPersonOffSentPrison,                   \
                       SUM(ShopliftingSuspectChargedPartOfAnotherCase)           AS ShopliftingSuspectChargedPartOfAnotherCase,        SUM(TheftFromPersonSuspectChargedPartOfAnotherCase)        AS TheftFromPersonSuspectChargedPartOfAnotherCase, \
                       SUM(ShopliftingUnableProsecuteSuspect)                    AS ShopliftingUnableProsecuteSuspect,                 SUM(TheftFromPersonUnableProsecuteSuspect)                 AS TheftFromPersonUnableProsecuteSuspect,          \
                       SUM(ShopliftingUnderInvestigation)                        AS ShopliftingUnderInvestigation,                     SUM(TheftFromPersonUnderInvestigation)                     AS TheftFromPersonUnderInvestigation,              \
                       \
                       SUM(VehicleCrimeEMPTYNULLOutcome)                         AS VehicleCrimeEMPTYNULLOutcome,                      SUM(ViolenceSexualOffencesEMPTYNULLOutcome)                AS ViolenceSexualOffencesEMPTYNULLOutcome,                \
                       SUM(VehicleCrimeActionToBeTakenOtherOrg)                  AS VehicleCrimeActionToBeTakenOtherOrg,               SUM(ViolenceSexualOffencesActionToBeTakenOtherOrg)         AS ViolenceSexualOffencesActionToBeTakenOtherOrg,         \
                       SUM(VehicleCrimeAwaitingCourtOutcome)                     AS VehicleCrimeAwaitingCourtOutcome,                  SUM(ViolenceSexualOffencesAwaitingCourtOutcome)            AS ViolenceSexualOffencesAwaitingCourtOutcome,            \
                       SUM(VehicleCrimeCourtCaseUnableToProceed)                 AS VehicleCrimeCourtCaseUnableToProceed,              SUM(ViolenceSexualOffencesCourtCaseUnableToProceed)        AS ViolenceSexualOffencesCourtCaseUnableToProceed,        \
                       SUM(VehicleCrimeCourtResultUnavailable)                   AS VehicleCrimeCourtResultUnavailable,                SUM(ViolenceSexualOffencesCourtResultUnavailable)          AS ViolenceSexualOffencesCourtResultUnavailable,          \
                       SUM(VehicleCrimeDefendantNotGuilty)                       AS VehicleCrimeDefendantNotGuilty,                    SUM(ViolenceSexualOffencesDefendantNotGuilty)              AS ViolenceSexualOffencesDefendantNotGuilty,              \
                       SUM(VehicleCrimeDefendantSentCrownCourt)                  AS VehicleCrimeDefendantSentCrownCourt,               SUM(ViolenceSexualOffencesDefendantSentCrownCourt)         AS ViolenceSexualOffencesDefendantSentCrownCourt,         \
                       SUM(VehicleCrimeFormalActionNotPublicInterest)            AS VehicleCrimeFormalActionNotPublicInterest,         SUM(ViolenceSexualOffencesFormalActionNotPublicInterest)   AS ViolenceSexualOffencesFormalActionNotPublicInterest,   \
                       SUM(VehicleCrimeInvestigationCompleteNoSuspect)           AS VehicleCrimeInvestigationCompleteNoSuspect,        SUM(ViolenceSexualOffencesInvestigationCompleteNoSuspect)  AS ViolenceSexualOffencesInvestigationCompleteNoSuspect,  \
                       SUM(VehicleCrimeLocalResolution)                          AS VehicleCrimeLocalResolution,                       SUM(ViolenceSexualOffencesLocalResolution)                 AS ViolenceSexualOffencesLocalResolution,                 \
                       SUM(VehicleCrimeOffDeprivedProperty)                      AS VehicleCrimeOffDeprivedProperty,                   SUM(ViolenceSexualOffencesOffDeprivedProperty)             AS ViolenceSexualOffencesOffDeprivedProperty,             \
                       SUM(VehicleCrimeOffFined)                                 AS VehicleCrimeOffFined,                              SUM(ViolenceSexualOffencesOffFined)                        AS ViolenceSexualOffencesOffFined,                        \
                       SUM(VehicleCrimeOffGivenCaution)                          AS VehicleCrimeOffGivenCaution,                       SUM(ViolenceSexualOffencesOffGivenCaution)                 AS ViolenceSexualOffencesOffGivenCaution,                 \
                       SUM(VehicleCrimeOffGivenDrugsPossessionWarning)           AS VehicleCrimeOffGivenDrugsPossessionWarning,        SUM(ViolenceSexualOffencesOffGivenDrugsPossessionWarning)  AS ViolenceSexualOffencesOffGivenDrugsPossessionWarning,  \
                       SUM(VehicleCrimeOffGivenAbsoluteDischarge)                AS VehicleCrimeOffGivenAbsoluteDischarge,             SUM(ViolenceSexualOffencesOffGivenAbsoluteDischarge)       AS ViolenceSexualOffencesOffGivenAbsoluteDischarge,       \
                       SUM(VehicleCrimeOffGivenCommunitySentence)                AS VehicleCrimeOffGivenCommunitySentence,             SUM(ViolenceSexualOffencesOffGivenCommunitySentence)       AS ViolenceSexualOffencesOffGivenCommunitySentence,       \
                       SUM(VehicleCrimeOffGivenConditionalDischarge)             AS VehicleCrimeOffGivenConditionalDischarge,          SUM(ViolenceSexualOffencesOffGivenConditionalDischarge)    AS ViolenceSexualOffencesOffGivenConditionalDischarge,    \
                       SUM(VehicleCrimeOffGivenPenaltyNotice)                    AS VehicleCrimeOffGivenPenaltyNotice,                 SUM(ViolenceSexualOffencesOffGivenPenaltyNotice)           AS ViolenceSexualOffencesOffGivenPenaltyNotice,           \
                       SUM(VehicleCrimeOffGivenSuspendedPrisonSentence)          AS VehicleCrimeOffGivenSuspendedPrisonSentence,       SUM(ViolenceSexualOffencesOffGivenSuspendedPrisonSentence) AS ViolenceSexualOffencesOffGivenSuspendedPrisonSentence, \
                       SUM(VehicleCrimeOffOrderedPayCompensation)                AS VehicleCrimeOffOrderedPayCompensation,             SUM(ViolenceSexualOffencesOffOrderedPayCompensation)       AS ViolenceSexualOffencesOffOrderedPayCompensation,       \
                       SUM(VehicleCrimeOffOtherwiseDealtWith)                    AS VehicleCrimeOffOtherwiseDealtWith,                 SUM(ViolenceSexualOffencesOffOtherwiseDealtWith)           AS ViolenceSexualOffencesOffOtherwiseDealtWith,           \
                       SUM(VehicleCrimeOffSentPrison)                            AS VehicleCrimeOffSentPrison,                         SUM(ViolenceSexualOffencesOffSentPrison)                   AS ViolenceSexualOffencesOffSentPrison,                   \
                       SUM(VehicleCrimeSuspectChargedPartOfAnotherCase)          AS VehicleCrimeSuspectChargedPartOfAnotherCase,       SUM(ViolenceSexualOffencesSuspectChargedPartOfAnotherCase) AS ViolenceSexualOffencesSuspectChargedPartOfAnotherCase, \
                       SUM(VehicleCrimeUnableProsecuteSuspect)                   AS VehicleCrimeUnableProsecuteSuspect,                SUM(ViolenceSexualOffencesUnableProsecuteSuspect)          AS ViolenceSexualOffencesUnableProsecuteSuspect,          \
                       SUM(VehicleCrimeUnderInvestigation)                       AS VehicleCrimeUnderInvestigation,                    SUM(ViolenceSexualOffencesUnderInvestigation)              AS ViolenceSexualOffencesUnderInvestigation,              \
                       \
                       SUM(ViolentCrimeEMPTYNULLOutcome)                         AS ViolentCrimeEMPTYNULLOutcome,                \
                       SUM(ViolentCrimeActionToBeTakenOtherOrg)                  AS ViolentCrimeActionToBeTakenOtherOrg,         \
                       SUM(ViolentCrimeAwaitingCourtOutcome)                     AS ViolentCrimeAwaitingCourtOutcome,            \
                       SUM(ViolentCrimeCourtCaseUnableToProceed)                 AS ViolentCrimeCourtCaseUnableToProceed,        \
                       SUM(ViolentCrimeCourtResultUnavailable)                   AS ViolentCrimeCourtResultUnavailable,          \
                       SUM(ViolentCrimeDefendantNotGuilty)                       AS ViolentCrimeDefendantNotGuilty,              \
                       SUM(ViolentCrimeDefendantSentCrownCourt)                  AS ViolentCrimeDefendantSentCrownCourt,         \
                       SUM(ViolentCrimeFormalActionNotPublicInterest)            AS ViolentCrimeFormalActionNotPublicInterest,   \
                       SUM(ViolentCrimeInvestigationCompleteNoSuspect)           AS ViolentCrimeInvestigationCompleteNoSuspect,  \
                       SUM(ViolentCrimeLocalResolution)                          AS ViolentCrimeLocalResolution,                 \
                       SUM(ViolentCrimeOffDeprivedProperty)                      AS ViolentCrimeOffDeprivedProperty,             \
                       SUM(ViolentCrimeOffFined)                                 AS ViolentCrimeOffFined,                        \
                       SUM(ViolentCrimeOffGivenCaution)                          AS ViolentCrimeOffGivenCaution,                 \
                       SUM(ViolentCrimeOffGivenDrugsPossessionWarning)           AS ViolentCrimeOffGivenDrugsPossessionWarning,  \
                       SUM(ViolentCrimeOffGivenAbsoluteDischarge)                AS ViolentCrimeOffGivenAbsoluteDischarge,       \
                       SUM(ViolentCrimeOffGivenCommunitySentence)                AS ViolentCrimeOffGivenCommunitySentence,       \
                       SUM(ViolentCrimeOffGivenConditionalDischarge)             AS ViolentCrimeOffGivenConditionalDischarge,    \
                       SUM(ViolentCrimeOffGivenPenaltyNotice)                    AS ViolentCrimeOffGivenPenaltyNotice,           \
                       SUM(ViolentCrimeOffGivenSuspendedPrisonSentence)          AS ViolentCrimeOffGivenSuspendedPrisonSentence, \
                       SUM(ViolentCrimeOffOrderedPayCompensation)                AS ViolentCrimeOffOrderedPayCompensation,       \
                       SUM(ViolentCrimeOffOtherwiseDealtWith)                    AS ViolentCrimeOffOtherwiseDealtWith,           \
                       SUM(ViolentCrimeOffSentPrison)                            AS ViolentCrimeOffSentPrison,                   \
                       SUM(ViolentCrimeSuspectChargedPartOfAnotherCase)          AS ViolentCrimeSuspectChargedPartOfAnotherCase, \
                       SUM(ViolentCrimeUnableProsecuteSuspect)                   AS ViolentCrimeUnableProsecuteSuspect,          \
                       SUM(ViolentCrimeUnderInvestigation)                       AS ViolentCrimeUnderInvestigation               \
                       \
                       from street_MSOA_nomonth\
                       \
                       group by LAD_code, LAD_name')

#Make a table from the dataframe so that it can be called from a SQL context
df_street_agg_LAD_month.registerTempTable("street_LAD_nomonth")

print("Number of records after aggregating to LAD and NoMonth level.")
count = df_street_agg_LAD_nomonth.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_LAD_nomonth = df_street_agg_LAD_nomonth.rdd
#Make one file
rdd_street_agg_LAD_nomonth_1 = rdd_street_agg_LAD_nomonth.coalesce(1)
#Save
#rdd_street_agg_LAD_nomonth_1.saveAsTextFile('s3://ukpolice/street_LAD_nomonth_level')








