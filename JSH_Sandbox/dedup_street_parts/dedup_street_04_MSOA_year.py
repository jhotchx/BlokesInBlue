#==========SETUP==========#

#Packages
from pyspark     import SparkContext
from pyspark.sql import Row, SQLContext

#Declare spark context environments
sc     = SparkContext( appName="Dedup Street" )
sqlCtx = SQLContext(sc)

#Load street data
street = sc.textFile('s3://ukpolice/street_MSOA_month')

#Breakup data into fields
streetMap = street.map(lambda line: line.split(',')) 

#Change to dataframe
df_street = sqlCtx.createDataFrame(streetMap)

#Label columns of dataframe
df_street_analysis = df_street.toDF("Month",
                                    "MSOA_code", 
                                    "MSOA_name", 
                                    "LAD_code",
                                    "LAD_name",
                                    "TotalObs", 
                                    "EMPTYNULLCrime", 
                                    "AntiSocialBehavior", 
                                    "BicycleTheft", 
                                    "Burglary", 
                                    "CriminalDamageArson", 
                                    "Drugs", 
                                    "OtherCrime", 
                                    "OtherTheft", 
                                    "PossessionWeapons", 
                                    "PublicDisorderWeapons", 
                                    "PublicOrder", 
                                    "Robbery", 
                                    "Shoplifting", 
                                    "TheftFromPerson", 
                                    "VehicleCrime", 
                                    "ViolenceSexualOffences", 
                                    "ViolentCrime", 
                                    "EMPTYNULLOutcome", 
                                    "ActionToBeTakenOtherOrg", 
                                    "AwaitingCourtOutcome", 
                                    "CourtCaseUnableToProceed", 
                                    "CourtResultUnavailable", 
                                    "DefendantNotGuilty", 
                                    "DefendantSentCrownCourt", 
                                    "FormalActionNotPublicInterest", 
                                    "InvestigationCompleteNoSuspect", 
                                    "LocalResolution", 
                                    "OffDeprivedProperty", 
                                    "OffFined", 
                                    "OffGivenCaution", 
                                    "OffGivenDrugsPossessionWarning", 
                                    "OffGivenAbsoluteDischarge", 
                                    "OffGivenCommunitySentence", 
                                    "OffGivenConditionalDischarge", 
                                    "OffGivenPenaltyNotice", 
                                    "OffGivenSuspendedPrisonSentence", 
                                    "OffOrderedPayCompensation", 
                                    "OffOtherwiseDealtWith", 
                                    "OffSentPrison", 
                                    "SuspectChargedPartOfAnotherCase", 
                                    "UnableProsecuteSuspect", 
                                    "UnderInvestigation", 
                                    "EMPTYNULLCrimeEMPTYNULLOutcome", 
                                    "EMPTYNULLCrimeActionToBeTakenOtherOrg", 
                                    "EMPTYNULLCrimeAwaitingCourtOutcome", 
                                    "EMPTYNULLCrimeCourtCaseUnableToProceed", 
                                    "EMPTYNULLCrimeCourtResultUnavailable", 
                                    "EMPTYNULLCrimeDefendantNotGuilty", 
                                    "EMPTYNULLCrimeDefendantSentCrownCourt", 
                                    "EMPTYNULLCrimeFormalActionNotPublicInterest", 
                                    "EMPTYNULLCrimeInvestigationCompleteNoSuspect", 
                                    "EMPTYNULLCrimeLocalResolution", 
                                    "EMPTYNULLCrimeOffDeprivedProperty", 
                                    "EMPTYNULLCrimeOffFined", 
                                    "EMPTYNULLCrimeOffGivenCaution", 
                                    "EMPTYNULLCrimeOffGivenDrugsPossessionWarning", 
                                    "EMPTYNULLCrimeOffGivenAbsoluteDischarge", 
                                    "EMPTYNULLCrimeOffGivenCommunitySentence", 
                                    "EMPTYNULLCrimeOffGivenConditionalDischarge", 
                                    "EMPTYNULLCrimeOffGivenPenaltyNotice", 
                                    "EMPTYNULLCrimeOffGivenSuspendedPrisonSentence", 
                                    "EMPTYNULLCrimeOffOrderedPayCompensation", 
                                    "EMPTYNULLCrimeOffOtherwiseDealtWith", 
                                    "EMPTYNULLCrimeOffSentPrison", 
                                    "EMPTYNULLCrimeSuspectChargedPartOfAnotherCase", 
                                    "EMPTYNULLCrimeUnableProsecuteSuspect", 
                                    "EMPTYNULLCrimeUnderInvestigation", 
                                    "AntiSocialBehaviorEMPTYNULLOutcome", 
                                    "AntiSocialBehaviorActionToBeTakenOtherOrg", 
                                    "AntiSocialBehaviorAwaitingCourtOutcome", 
                                    "AntiSocialBehaviorCourtCaseUnableToProceed", 
                                    "AntiSocialBehaviorCourtResultUnavailable", 
                                    "AntiSocialBehaviorDefendantNotGuilty", 
                                    "AntiSocialBehaviorDefendantSentCrownCourt", 
                                    "AntiSocialBehaviorFormalActionNotPublicInterest", 
                                    "AntiSocialBehaviorInvestigationCompleteNoSuspect", 
                                    "AntiSocialBehaviorLocalResolution", 
                                    "AntiSocialBehaviorOffDeprivedProperty", 
                                    "AntiSocialBehaviorOffFined", 
                                    "AntiSocialBehaviorOffGivenCaution", 
                                    "AntiSocialBehaviorOffGivenDrugsPossessionWarning", 
                                    "AntiSocialBehaviorOffGivenAbsoluteDischarge", 
                                    "AntiSocialBehaviorOffGivenCommunitySentence", 
                                    "AntiSocialBehaviorOffGivenConditionalDischarge", 
                                    "AntiSocialBehaviorOffGivenPenaltyNotice", 
                                    "AntiSocialBehaviorOffGivenSuspendedPrisonSentence", 
                                    "AntiSocialBehaviorOffOrderedPayCompensation", 
                                    "AntiSocialBehaviorOffOtherwiseDealtWith", 
                                    "AntiSocialBehaviorOffSentPrison", 
                                    "AntiSocialBehaviorSuspectChargedPartOfAnotherCase", 
                                    "AntiSocialBehaviorUnableProsecuteSuspect", 
                                    "AntiSocialBehaviorUnderInvestigation", 
                                    "BicycleTheftEMPTYNULLOutcome", 
                                    "BicycleTheftActionToBeTakenOtherOrg", 
                                    "BicycleTheftAwaitingCourtOutcome", 
                                    "BicycleTheftCourtCaseUnableToProceed", 
                                    "BicycleTheftCourtResultUnavailable", 
                                    "BicycleTheftDefendantNotGuilty", 
                                    "BicycleTheftDefendantSentCrownCourt", 
                                    "BicycleTheftFormalActionNotPublicInterest", 
                                    "BicycleTheftInvestigationCompleteNoSuspect", 
                                    "BicycleTheftLocalResolution", 
                                    "BicycleTheftOffDeprivedProperty", 
                                    "BicycleTheftOffFined", 
                                    "BicycleTheftOffGivenCaution", 
                                    "BicycleTheftOffGivenDrugsPossessionWarning", 
                                    "BicycleTheftOffGivenAbsoluteDischarge", 
                                    "BicycleTheftOffGivenCommunitySentence", 
                                    "BicycleTheftOffGivenConditionalDischarge", 
                                    "BicycleTheftOffGivenPenaltyNotice", 
                                    "BicycleTheftOffGivenSuspendedPrisonSentence", 
                                    "BicycleTheftOffOrderedPayCompensation", 
                                    "BicycleTheftOffOtherwiseDealtWith", 
                                    "BicycleTheftOffSentPrison", 
                                    "BicycleTheftSuspectChargedPartOfAnotherCase", 
                                    "BicycleTheftUnableProsecuteSuspect", 
                                    "BicycleTheftUnderInvestigation", 
                                    "BurglaryEMPTYNULLOutcome", 
                                    "BurglaryActionToBeTakenOtherOrg", 
                                    "BurglaryAwaitingCourtOutcome", 
                                    "BurglaryCourtCaseUnableToProceed", 
                                    "BurglaryCourtResultUnavailable", 
                                    "BurglaryDefendantNotGuilty", 
                                    "BurglaryDefendantSentCrownCourt", 
                                    "BurglaryFormalActionNotPublicInterest", 
                                    "BurglaryInvestigationCompleteNoSuspect", 
                                    "BurglaryLocalResolution", 
                                    "BurglaryOffDeprivedProperty", 
                                    "BurglaryOffFined", 
                                    "BurglaryOffGivenCaution", 
                                    "BurglaryOffGivenDrugsPossessionWarning", 
                                    "BurglaryOffGivenAbsoluteDischarge", 
                                    "BurglaryOffGivenCommunitySentence", 
                                    "BurglaryOffGivenConditionalDischarge", 
                                    "BurglaryOffGivenPenaltyNotice", 
                                    "BurglaryOffGivenSuspendedPrisonSentence", 
                                    "BurglaryOffOrderedPayCompensation", 
                                    "BurglaryOffOtherwiseDealtWith", 
                                    "BurglaryOffSentPrison", 
                                    "BurglarySuspectChargedPartOfAnotherCase", 
                                    "BurglaryUnableProsecuteSuspect", 
                                    "BurglaryUnderInvestigation", 
                                    "CriminalDamageArsonEMPTYNULLOutcome", 
                                    "CriminalDamageArsonActionToBeTakenOtherOrg", 
                                    "CriminalDamageArsonAwaitingCourtOutcome", 
                                    "CriminalDamageArsonCourtCaseUnableToProceed", 
                                    "CriminalDamageArsonCourtResultUnavailable", 
                                    "CriminalDamageArsonDefendantNotGuilty", 
                                    "CriminalDamageArsonDefendantSentCrownCourt", 
                                    "CriminalDamageArsonFormalActionNotPublicInterest", 
                                    "CriminalDamageArsonInvestigationCompleteNoSuspect", 
                                    "CriminalDamageArsonLocalResolution", 
                                    "CriminalDamageArsonOffDeprivedProperty", 
                                    "CriminalDamageArsonOffFined", 
                                    "CriminalDamageArsonOffGivenCaution", 
                                    "CriminalDamageArsonOffGivenDrugsPossessionWarning", 
                                    "CriminalDamageArsonOffGivenAbsoluteDischarge", 
                                    "CriminalDamageArsonOffGivenCommunitySentence", 
                                    "CriminalDamageArsonOffGivenConditionalDischarge", 
                                    "CriminalDamageArsonOffGivenPenaltyNotice", 
                                    "CriminalDamageArsonOffGivenSuspendedPrisonSentence", 
                                    "CriminalDamageArsonOffOrderedPayCompensation", 
                                    "CriminalDamageArsonOffOtherwiseDealtWith", 
                                    "CriminalDamageArsonOffSentPrison", 
                                    "CriminalDamageArsonSuspectChargedPartOfAnotherCase", 
                                    "CriminalDamageArsonUnableProsecuteSuspect", 
                                    "CriminalDamageArsonUnderInvestigation", 
                                    "DrugsEMPTYNULLOutcome", 
                                    "DrugsActionToBeTakenOtherOrg", 
                                    "DrugsAwaitingCourtOutcome", 
                                    "DrugsCourtCaseUnableToProceed", 
                                    "DrugsCourtResultUnavailable", 
                                    "DrugsDefendantNotGuilty", 
                                    "DrugsDefendantSentCrownCourt", 
                                    "DrugsFormalActionNotPublicInterest", 
                                    "DrugsInvestigationCompleteNoSuspect", 
                                    "DrugsLocalResolution", 
                                    "DrugsOffDeprivedProperty", 
                                    "DrugsOffFined", 
                                    "DrugsOffGivenCaution", 
                                    "DrugsOffGivenDrugsPossessionWarning", 
                                    "DrugsOffGivenAbsoluteDischarge", 
                                    "DrugsOffGivenCommunitySentence", 
                                    "DrugsOffGivenConditionalDischarge", 
                                    "DrugsOffGivenPenaltyNotice", 
                                    "DrugsOffGivenSuspendedPrisonSentence", 
                                    "DrugsOffOrderedPayCompensation", 
                                    "DrugsOffOtherwiseDealtWith", 
                                    "DrugsOffSentPrison", 
                                    "DrugsSuspectChargedPartOfAnotherCase", 
                                    "DrugsUnableProsecuteSuspect", 
                                    "DrugsUnderInvestigation", 
                                    "OtherCrimeEMPTYNULLOutcome", 
                                    "OtherCrimeActionToBeTakenOtherOrg", 
                                    "OtherCrimeAwaitingCourtOutcome", 
                                    "OtherCrimeCourtCaseUnableToProceed", 
                                    "OtherCrimeCourtResultUnavailable", 
                                    "OtherCrimeDefendantNotGuilty", 
                                    "OtherCrimeDefendantSentCrownCourt", 
                                    "OtherCrimeFormalActionNotPublicInterest", 
                                    "OtherCrimeInvestigationCompleteNoSuspect", 
                                    "OtherCrimeLocalResolution", 
                                    "OtherCrimeOffDeprivedProperty", 
                                    "OtherCrimeOffFined", 
                                    "OtherCrimeOffGivenCaution", 
                                    "OtherCrimeOffGivenDrugsPossessionWarning", 
                                    "OtherCrimeOffGivenAbsoluteDischarge", 
                                    "OtherCrimeOffGivenCommunitySentence", 
                                    "OtherCrimeOffGivenConditionalDischarge", 
                                    "OtherCrimeOffGivenPenaltyNotice", 
                                    "OtherCrimeOffGivenSuspendedPrisonSentence", 
                                    "OtherCrimeOffOrderedPayCompensation", 
                                    "OtherCrimeOffOtherwiseDealtWith", 
                                    "OtherCrimeOffSentPrison", 
                                    "OtherCrimeSuspectChargedPartOfAnotherCase", 
                                    "OtherCrimeUnableProsecuteSuspect", 
                                    "OtherCrimeUnderInvestigation", 
                                    "OtherTheftEMPTYNULLOutcome", 
                                    "OtherTheftActionToBeTakenOtherOrg", 
                                    "OtherTheftAwaitingCourtOutcome", 
                                    "OtherTheftCourtCaseUnableToProceed", 
                                    "OtherTheftCourtResultUnavailable", 
                                    "OtherTheftDefendantNotGuilty", 
                                    "OtherTheftDefendantSentCrownCourt", 
                                    "OtherTheftFormalActionNotPublicInterest", 
                                    "OtherTheftInvestigationCompleteNoSuspect", 
                                    "OtherTheftLocalResolution", 
                                    "OtherTheftOffDeprivedProperty", 
                                    "OtherTheftOffFined", 
                                    "OtherTheftOffGivenCaution", 
                                    "OtherTheftOffGivenDrugsPossessionWarning", 
                                    "OtherTheftOffGivenAbsoluteDischarge", 
                                    "OtherTheftOffGivenCommunitySentence", 
                                    "OtherTheftOffGivenConditionalDischarge", 
                                    "OtherTheftOffGivenPenaltyNotice", 
                                    "OtherTheftOffGivenSuspendedPrisonSentence", 
                                    "OtherTheftOffOrderedPayCompensation", 
                                    "OtherTheftOffOtherwiseDealtWith", 
                                    "OtherTheftOffSentPrison", 
                                    "OtherTheftSuspectChargedPartOfAnotherCase", 
                                    "OtherTheftUnableProsecuteSuspect", 
                                    "OtherTheftUnderInvestigation", 
                                    "PossessionWeaponsEMPTYNULLOutcome", 
                                    "PossessionWeaponsActionToBeTakenOtherOrg", 
                                    "PossessionWeaponsAwaitingCourtOutcome", 
                                    "PossessionWeaponsCourtCaseUnableToProceed", 
                                    "PossessionWeaponsCourtResultUnavailable", 
                                    "PossessionWeaponsDefendantNotGuilty", 
                                    "PossessionWeaponsDefendantSentCrownCourt", 
                                    "PossessionWeaponsFormalActionNotPublicInterest", 
                                    "PossessionWeaponsInvestigationCompleteNoSuspect", 
                                    "PossessionWeaponsLocalResolution", 
                                    "PossessionWeaponsOffDeprivedProperty", 
                                    "PossessionWeaponsOffFined", 
                                    "PossessionWeaponsOffGivenCaution", 
                                    "PossessionWeaponsOffGivenDrugsPossessionWarning", 
                                    "PossessionWeaponsOffGivenAbsoluteDischarge", 
                                    "PossessionWeaponsOffGivenCommunitySentence", 
                                    "PossessionWeaponsOffGivenConditionalDischarge", 
                                    "PossessionWeaponsOffGivenPenaltyNotice", 
                                    "PossessionWeaponsOffGivenSuspendedPrisonSentence", 
                                    "PossessionWeaponsOffOrderedPayCompensation", 
                                    "PossessionWeaponsOffOtherwiseDealtWith", 
                                    "PossessionWeaponsOffSentPrison", 
                                    "PossessionWeaponsSuspectChargedPartOfAnotherCase", 
                                    "PossessionWeaponsUnableProsecuteSuspect", 
                                    "PossessionWeaponsUnderInvestigation", 
                                    "PublicDisorderWeaponsEMPTYNULLOutcome", 
                                    "PublicDisorderWeaponsActionToBeTakenOtherOrg", 
                                    "PublicDisorderWeaponsAwaitingCourtOutcome", 
                                    "PublicDisorderWeaponsCourtCaseUnableToProceed", 
                                    "PublicDisorderWeaponsCourtResultUnavailable", 
                                    "PublicDisorderWeaponsDefendantNotGuilty", 
                                    "PublicDisorderWeaponsDefendantSentCrownCourt", 
                                    "PublicDisorderWeaponsFormalActionNotPublicInterest", 
                                    "PublicDisorderWeaponsInvestigationCompleteNoSuspect", 
                                    "PublicDisorderWeaponsLocalResolution", 
                                    "PublicDisorderWeaponsOffDeprivedProperty", 
                                    "PublicDisorderWeaponsOffFined", 
                                    "PublicDisorderWeaponsOffGivenCaution", 
                                    "PublicDisorderWeaponsOffGivenDrugsPossessionWarning", 
                                    "PublicDisorderWeaponsOffGivenAbsoluteDischarge", 
                                    "PublicDisorderWeaponsOffGivenCommunitySentence", 
                                    "PublicDisorderWeaponsOffGivenConditionalDischarge", 
                                    "PublicDisorderWeaponsOffGivenPenaltyNotice", 
                                    "PublicDisorderWeaponsOffGivenSuspendedPrisonSentence", 
                                    "PublicDisorderWeaponsOffOrderedPayCompensation", 
                                    "PublicDisorderWeaponsOffOtherwiseDealtWith", 
                                    "PublicDisorderWeaponsOffSentPrison", 
                                    "PublicDisorderWeaponsSuspectChargedPartOfAnotherCase", 
                                    "PublicDisorderWeaponsUnableProsecuteSuspect", 
                                    "PublicDisorderWeaponsUnderInvestigation", 
                                    "PublicOrderEMPTYNULLOutcome", 
                                    "PublicOrderActionToBeTakenOtherOrg", 
                                    "PublicOrderAwaitingCourtOutcome", 
                                    "PublicOrderCourtCaseUnableToProceed", 
                                    "PublicOrderCourtResultUnavailable", 
                                    "PublicOrderDefendantNotGuilty", 
                                    "PublicOrderDefendantSentCrownCourt", 
                                    "PublicOrderFormalActionNotPublicInterest", 
                                    "PublicOrderInvestigationCompleteNoSuspect", 
                                    "PublicOrderLocalResolution", 
                                    "PublicOrderOffDeprivedProperty", 
                                    "PublicOrderOffFined", 
                                    "PublicOrderOffGivenCaution", 
                                    "PublicOrderOffGivenDrugsPossessionWarning", 
                                    "PublicOrderOffGivenAbsoluteDischarge", 
                                    "PublicOrderOffGivenCommunitySentence", 
                                    "PublicOrderOffGivenConditionalDischarge", 
                                    "PublicOrderOffGivenPenaltyNotice", 
                                    "PublicOrderOffGivenSuspendedPrisonSentence", 
                                    "PublicOrderOffOrderedPayCompensation", 
                                    "PublicOrderOffOtherwiseDealtWith", 
                                    "PublicOrderOffSentPrison", 
                                    "PublicOrderSuspectChargedPartOfAnotherCase", 
                                    "PublicOrderUnableProsecuteSuspect", 
                                    "PublicOrderUnderInvestigation", 
                                    "RobberyEMPTYNULLOutcome", 
                                    "RobberyActionToBeTakenOtherOrg", 
                                    "RobberyAwaitingCourtOutcome", 
                                    "RobberyCourtCaseUnableToProceed", 
                                    "RobberyCourtResultUnavailable", 
                                    "RobberyDefendantNotGuilty", 
                                    "RobberyDefendantSentCrownCourt", 
                                    "RobberyFormalActionNotPublicInterest", 
                                    "RobberyInvestigationCompleteNoSuspect", 
                                    "RobberyLocalResolution", 
                                    "RobberyOffDeprivedProperty", 
                                    "RobberyOffFined", 
                                    "RobberyOffGivenCaution", 
                                    "RobberyOffGivenDrugsPossessionWarning", 
                                    "RobberyOffGivenAbsoluteDischarge", 
                                    "RobberyOffGivenCommunitySentence", 
                                    "RobberyOffGivenConditionalDischarge", 
                                    "RobberyOffGivenPenaltyNotice", 
                                    "RobberyOffGivenSuspendedPrisonSentence", 
                                    "RobberyOffOrderedPayCompensation", 
                                    "RobberyOffOtherwiseDealtWith", 
                                    "RobberyOffSentPrison", 
                                    "RobberySuspectChargedPartOfAnotherCase", 
                                    "RobberyUnableProsecuteSuspect", 
                                    "RobberyUnderInvestigation", 
                                    "ShopliftingEMPTYNULLOutcome", 
                                    "ShopliftingActionToBeTakenOtherOrg", 
                                    "ShopliftingAwaitingCourtOutcome", 
                                    "ShopliftingCourtCaseUnableToProceed", 
                                    "ShopliftingCourtResultUnavailable", 
                                    "ShopliftingDefendantNotGuilty", 
                                    "ShopliftingDefendantSentCrownCourt", 
                                    "ShopliftingFormalActionNotPublicInterest", 
                                    "ShopliftingInvestigationCompleteNoSuspect", 
                                    "ShopliftingLocalResolution", 
                                    "ShopliftingOffDeprivedProperty", 
                                    "ShopliftingOffFined", 
                                    "ShopliftingOffGivenCaution", 
                                    "ShopliftingOffGivenDrugsPossessionWarning", 
                                    "ShopliftingOffGivenAbsoluteDischarge", 
                                    "ShopliftingOffGivenCommunitySentence", 
                                    "ShopliftingOffGivenConditionalDischarge", 
                                    "ShopliftingOffGivenPenaltyNotice", 
                                    "ShopliftingOffGivenSuspendedPrisonSentence", 
                                    "ShopliftingOffOrderedPayCompensation", 
                                    "ShopliftingOffOtherwiseDealtWith", 
                                    "ShopliftingOffSentPrison", 
                                    "ShopliftingSuspectChargedPartOfAnotherCase", 
                                    "ShopliftingUnableProsecuteSuspect", 
                                    "ShopliftingUnderInvestigation", 
                                    "TheftFromPersonEMPTYNULLOutcome", 
                                    "TheftFromPersonActionToBeTakenOtherOrg", 
                                    "TheftFromPersonAwaitingCourtOutcome", 
                                    "TheftFromPersonCourtCaseUnableToProceed", 
                                    "TheftFromPersonCourtResultUnavailable", 
                                    "TheftFromPersonDefendantNotGuilty", 
                                    "TheftFromPersonDefendantSentCrownCourt", 
                                    "TheftFromPersonFormalActionNotPublicInterest", 
                                    "TheftFromPersonInvestigationCompleteNoSuspect", 
                                    "TheftFromPersonLocalResolution", 
                                    "TheftFromPersonOffDeprivedProperty", 
                                    "TheftFromPersonOffFined", 
                                    "TheftFromPersonOffGivenCaution", 
                                    "TheftFromPersonOffGivenDrugsPossessionWarning", 
                                    "TheftFromPersonOffGivenAbsoluteDischarge", 
                                    "TheftFromPersonOffGivenCommunitySentence", 
                                    "TheftFromPersonOffGivenConditionalDischarge", 
                                    "TheftFromPersonOffGivenPenaltyNotice", 
                                    "TheftFromPersonOffGivenSuspendedPrisonSentence", 
                                    "TheftFromPersonOffOrderedPayCompensation", 
                                    "TheftFromPersonOffOtherwiseDealtWith", 
                                    "TheftFromPersonOffSentPrison", 
                                    "TheftFromPersonSuspectChargedPartOfAnotherCase", 
                                    "TheftFromPersonUnableProsecuteSuspect", 
                                    "TheftFromPersonUnderInvestigation", 
                                    "VehicleCrimeEMPTYNULLOutcome", 
                                    "VehicleCrimeActionToBeTakenOtherOrg", 
                                    "VehicleCrimeAwaitingCourtOutcome", 
                                    "VehicleCrimeCourtCaseUnableToProceed", 
                                    "VehicleCrimeCourtResultUnavailable", 
                                    "VehicleCrimeDefendantNotGuilty", 
                                    "VehicleCrimeDefendantSentCrownCourt", 
                                    "VehicleCrimeFormalActionNotPublicInterest", 
                                    "VehicleCrimeInvestigationCompleteNoSuspect", 
                                    "VehicleCrimeLocalResolution", 
                                    "VehicleCrimeOffDeprivedProperty", 
                                    "VehicleCrimeOffFined", 
                                    "VehicleCrimeOffGivenCaution", 
                                    "VehicleCrimeOffGivenDrugsPossessionWarning", 
                                    "VehicleCrimeOffGivenAbsoluteDischarge", 
                                    "VehicleCrimeOffGivenCommunitySentence", 
                                    "VehicleCrimeOffGivenConditionalDischarge", 
                                    "VehicleCrimeOffGivenPenaltyNotice", 
                                    "VehicleCrimeOffGivenSuspendedPrisonSentence", 
                                    "VehicleCrimeOffOrderedPayCompensation", 
                                    "VehicleCrimeOffOtherwiseDealtWith", 
                                    "VehicleCrimeOffSentPrison", 
                                    "VehicleCrimeSuspectChargedPartOfAnotherCase", 
                                    "VehicleCrimeUnableProsecuteSuspect", 
                                    "VehicleCrimeUnderInvestigation", 
                                    "ViolenceSexualOffencesEMPTYNULLOutcome", 
                                    "ViolenceSexualOffencesActionToBeTakenOtherOrg", 
                                    "ViolenceSexualOffencesAwaitingCourtOutcome", 
                                    "ViolenceSexualOffencesCourtCaseUnableToProceed", 
                                    "ViolenceSexualOffencesCourtResultUnavailable", 
                                    "ViolenceSexualOffencesDefendantNotGuilty", 
                                    "ViolenceSexualOffencesDefendantSentCrownCourt", 
                                    "ViolenceSexualOffencesFormalActionNotPublicInterest", 
                                    "ViolenceSexualOffencesInvestigationCompleteNoSuspect", 
                                    "ViolenceSexualOffencesLocalResolution", 
                                    "ViolenceSexualOffencesOffDeprivedProperty", 
                                    "ViolenceSexualOffencesOffFined", 
                                    "ViolenceSexualOffencesOffGivenCaution", 
                                    "ViolenceSexualOffencesOffGivenDrugsPossessionWarning", 
                                    "ViolenceSexualOffencesOffGivenAbsoluteDischarge", 
                                    "ViolenceSexualOffencesOffGivenCommunitySentence", 
                                    "ViolenceSexualOffencesOffGivenConditionalDischarge", 
                                    "ViolenceSexualOffencesOffGivenPenaltyNotice", 
                                    "ViolenceSexualOffencesOffGivenSuspendedPrisonSentence", 
                                    "ViolenceSexualOffencesOffOrderedPayCompensation", 
                                    "ViolenceSexualOffencesOffOtherwiseDealtWith", 
                                    "ViolenceSexualOffencesOffSentPrison", 
                                    "ViolenceSexualOffencesSuspectChargedPartOfAnotherCase", 
                                    "ViolenceSexualOffencesUnableProsecuteSuspect", 
                                    "ViolenceSexualOffencesUnderInvestigation", 
                                    "ViolentCrimeEMPTYNULLOutcome", 
                                    "ViolentCrimeActionToBeTakenOtherOrg", 
                                    "ViolentCrimeAwaitingCourtOutcome", 
                                    "ViolentCrimeCourtCaseUnableToProceed", 
                                    "ViolentCrimeCourtResultUnavailable", 
                                    "ViolentCrimeDefendantNotGuilty", 
                                    "ViolentCrimeDefendantSentCrownCourt", 
                                    "ViolentCrimeFormalActionNotPublicInterest", 
                                    "ViolentCrimeInvestigationCompleteNoSuspect", 
                                    "ViolentCrimeLocalResolution", 
                                    "ViolentCrimeOffDeprivedProperty", 
                                    "ViolentCrimeOffFined", 
                                    "ViolentCrimeOffGivenCaution", 
                                    "ViolentCrimeOffGivenDrugsPossessionWarning", 
                                    "ViolentCrimeOffGivenAbsoluteDischarge", 
                                    "ViolentCrimeOffGivenCommunitySentence", 
                                    "ViolentCrimeOffGivenConditionalDischarge", 
                                    "ViolentCrimeOffGivenPenaltyNotice", 
                                    "ViolentCrimeOffGivenSuspendedPrisonSentence", 
                                    "ViolentCrimeOffOrderedPayCompensation", 
                                    "ViolentCrimeOffOtherwiseDealtWith", 
                                    "ViolentCrimeOffSentPrison", 
                                    "ViolentCrimeSuspectChargedPartOfAnotherCase", 
                                    "ViolentCrimeUnableProsecuteSuspect", 
                                    "ViolentCrimeUnderInvestigation")

#==========AGGREGATE BY LSOA YEAR==========#

df_street_analysis.registerTempTable('street_MSOA_month')

df_street_agg_LSOA_year = sqlCtx.sql('select SUBSTRING(Month,1,4) AS Year, MSOA_code, MSOA_name, LAD_code, LAD_name, SUM(TotalObs) as TotalObs, \
                       SUM(EMPTYNULLCrime)                                    AS EMPTYNULLCrime,                                    SUM(EMPTYNULLOutcome)                                      AS EMPTYNULLOutcome,                \
                       SUM(AntiSocialBehavior)                                AS AntiSocialBehavior,                                SUM(ActionToBeTakenOtherOrg)                               AS ActionToBeTakenOtherOrg,         \
                       SUM(BicycleTheft)                                      AS BicycleTheft,                                      SUM(AwaitingCourtOutcome)                                  AS AwaitingCourtOutcome,            \
                       SUM(Burglary)                                          AS Burglary,                                          SUM(CourtCaseUnableToProceed)                              AS CourtCaseUnableToProceed,        \
                       SUM(CriminalDamageArson)                               AS CriminalDamageArson,                               SUM(CourtResultUnavailable)                                AS CourtResultUnavailable,          \
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
                       SUM(ViolentCrime)                                      AS ViolentCrime,                                      SUM(OffGivenConditionalDischarge)                          AS OffGivenConditionalDischarge,    \
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
                       group by SUBSTRING(Month,1,4), MSOA_code, MSOA_name, LAD_code, LAD_name')

print("Number of records after aggregating to MSOA Year level.")
count = df_street_agg_MSOA_year.count()
print(count)

#Save a copy of the file at this point into s3
#Change to rdd
rdd_street_agg_MSOA_year = df_street_agg_MSOA_year.rdd
#Make one file
rdd_street_agg_MSOA_year_1 = rdd_street_agg_MSOA_year.coalesce(1)
#Save
rdd_street_agg_MSOA_year_1.saveAsTextFile('s3://ukpolice/street_MSOA_year')
