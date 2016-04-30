#Set working directory - Needs to be the location of the datasets
setwd("C:/Users/John/Box Sync/ANLY-502/Group Project")

#Read in the total_crime dataset which contains the LAD values, year, and count
totCrime <- read.csv("total_crime_LAD_year.csv")
totCrime <- totCrime[,-1] #Drop first column, contains an index

#Read in the counts of type
totType <- read.csv("type_LAD_year.csv")
totType <- totType[,-1] #Drop first column, contains an index

#Get LAD/Year for data used in maps
shape <- read.csv("shape.csv")
#Rename column to match other files
names(shape)[names(shape)=="District"] <- "LAD_name"

#Load unemployment data
unemp <- read.csv("UnemploymentLAD.csv")
#Get rid of some of the extra columns
unemp <- unemp[,-grep("(Conf|Numerator|Denominator)",names(unemp))]
#Rename columns we are going to use to start
names(unemp)[names(unemp)=="local.authority..district...unitary..prior.to.April.2015."] <- "LAD_name"
names(unemp)[names(unemp)=="Date"] <- "Year"
names(unemp)[names(unemp)=="Unemployment.rate...aged.16.64"] <- "Unemp16to64"

#Try the first regression
#Limit Unemployment data file to just the variables that we need
reg1.unemp <- unemp[,names(unemp) %in% c("LAD_name","Year","Unemp16to64")]
#Perform merge of unemployment data and crime data
reg1.data <- merge(totCrime, reg1.unemp, by=c("LAD_name","Year"), all=TRUE)
#Perform merge of merged unemp/crime and the shape file for maps
reg1.data <- merge(shape, reg1.data, by=c("LAD_name","Year"), all.x=TRUE)
#Remove observations with weird characters frm Unemp16to64
reg1.data <- reg1.data[!(reg1.data$Unemp16to64 %in% c("!","-")),]
#Change variable formats as needed
reg1.data$Year <- as.factor(reg1.data$Year)
reg1.data$Unemp16to64 <- as.numeric(reg1.data$Unemp16to64)

#First regression done
reg1 <- lm(as.numeric(Unemp16to64) ~ Year + count, data=reg1.data)
summary(reg1)


