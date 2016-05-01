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
#Rename columns we are going to use to start
names(unemp)[names(unemp)=="local.authority..district...unitary..prior.to.April.2015."] <- "LAD_name"
names(unemp)[names(unemp)=="Date"] <- "Year"
names(unemp)[names(unemp)=="Unemployment.rate...aged.16.64"] <- "Unemp16to64"
names(unemp)[names(unemp)=="Denominator"] <- "Pop"

#Get rid of some of the extra columns
unemp <- unemp[,-grep("(Conf|Numerator|Denominator)",names(unemp))]

#Try the first regression
#Limit Unemployment data file to just the variables that we need
reg1.unemp <- unemp[,names(unemp) %in% c("LAD_name","Year","Unemp16to64","Pop")]
#Perform merge of unemployment data and crime data
reg1.data <- merge(totCrime, reg1.unemp, by=c("LAD_name","Year"), all=TRUE)
#Perform merge of merged unemp/crime and the shape file for maps
reg1.data <- merge(shape, reg1.data, by=c("LAD_name","Year"), all.x=TRUE)
#Remove observations with weird characters frm Unemp16to64
reg1.data <- reg1.data[!(reg1.data$Unemp16to64 %in% c("!","-")),]
#Change variable formats as needed
reg1.data$Year <- as.factor(reg1.data$Year)
reg1.data$Unemp16to64 <- as.numeric(levels(reg1.data$Unemp16to64))[reg1.data$Unemp16to64]
reg1.data$Pop <- as.numeric(levels(reg1.data$Pop))[reg1.data$Pop]

#First regression done
reg1 <- lm(count ~ Year + Unemp16to64 + Pop, data=reg1.data)
summary(reg1)

#Model validation
#Use cross-validation
k <- 10 #Number of cv folds

#Create index to identify folds
folds <- sample(1:k, nrow(reg1.data), replace=TRUE) 

#Create matrix to store error values from each regression
cv.errors <- rep(NA,k)
r.squared <- rep(NA,k)
coeffs    <- matrix(NA,k,7)

#Run cross-validation for best subset selection
for (j in 1:k) {
  #Get best subset for the fold
  lm.fit <- lm(count ~ Year + Unemp16to64 + Pop, data=reg1.data[folds!=j,])
  #Get prediction for this fold and i predictors
  pred <- predict(lm.fit, reg1.data[folds==j,])
  #Save mse
  cv.errors[j] <- mean((reg1.data$count[folds==j]-pred)^2)
  r.squared[j] <- summary(lm.fit)$adj.r.squared
  coeffs[j,]   <- coefficients(lm.fit)
}

colnames(coeffs) <- names(coefficients(lm.fit))

#Take average of vector to get test mse
mean.cv.errors <- mean(cv.errors)

#CV Error
plot(cv.errors, type="b",main="Cross-Validation MSE by Fold",
     xlab="Fold",
     ylab="Mean-Squared Error")
abline(h=mean.cv.errors,col="red",lty=2) #Plot average fold

legend("topright",
       legend=c("Value","Cross-fold Mean"), 
       col=c("black","red"), 
       lwd=1, lty=c(1,2), cex=0.7)

#R^2
plot(r.squared, type="b",main=bquote(R^2*" by Fold"),
     xlab="Fold",
     ylab=bquote(R^2))
abline(h=mean(r.squared),col="red",lty=2) #Plot average fold

legend("topright",
       legend=c("Value","Cross-fold Mean"), 
       col=c("black","red"), 
       lwd=1, lty=c(1,2), cex=0.7)

#Unemployment
plot(coeffs[,colnames(coeffs)=="Unemp16to64"], type="b",
     main="Unemployment (16-64) Coefficient by Fold",
     xlab="Fold",
     ylab="Coefficient")
abline(h=mean(coeffs[,colnames(coeffs)=="Unemp16to64"]),
       col="red",lty=2) #Plot average fold

legend("topright",
       legend=c("Value","Cross-fold Mean"), 
       col=c("black","red"), 
       lwd=1, lty=c(1,2), cex=0.7)

#Unemployment
plot(coeffs[,colnames(coeffs)=="Pop"], type="b",
     main="Population Coefficient by Fold",
     xlab="Fold",
     ylab="Coefficient")
abline(h=mean(coeffs[,colnames(coeffs)=="Pop"]),
       col="red",lty=2) #Plot average fold

legend("topright",
       legend=c("Value","Cross-fold Mean"), 
       col=c("black","red"), 
       lwd=1, lty=c(1,2), cex=0.7)

ybounds <- c(min(coeffs[,colnames(coeffs)=="Year2012"],
                 coeffs[,colnames(coeffs)=="Year2013"],
                 coeffs[,colnames(coeffs)=="Year2014"],
                 coeffs[,colnames(coeffs)=="Year2015"]),
             max(coeffs[,colnames(coeffs)=="Year2012"],
                 coeffs[,colnames(coeffs)=="Year2013"],
                 coeffs[,colnames(coeffs)=="Year2014"],
                 coeffs[,colnames(coeffs)=="Year2015"]))

plot(coeffs[,colnames(coeffs)=="Year2012"], type="b",main="Year Coefficients by Fold",
     xlab="Fold",
     ylab="Year Coefficient",
     ylim=ybounds,
     col="red")
abline(h=mean(coeffs[,colnames(coeffs)=="Year2012"]),col="red",lty=2) #Plot average fold
lines(coeffs[,colnames(coeffs)=="Year2013"], type="b",col="blue")
abline(h=mean(coeffs[,colnames(coeffs)=="Year2013"]),col="blue",lty=2) #Plot average fold
lines(coeffs[,colnames(coeffs)=="Year2014"], type="b",col="green")
abline(h=mean(coeffs[,colnames(coeffs)=="Year2014"]),col="green",lty=2) #Plot average fold
lines(coeffs[,colnames(coeffs)=="Year2015"], type="b",col="purple")
abline(h=mean(coeffs[,colnames(coeffs)=="Year2015"]),col="purple",lty=2) #Plot average fold

legend(x=8,y=3100, 
       legend=c("2012 Value","2012 Cross-fold Mean",
                "2013 Value","2013 Cross-fold Mean",
                "2014 Value","2014 Cross-fold Mean",
                "2015 Value","2015 Cross-fold Mean"), 
       col=c("red","red","blue","blue","green","green","purple","purple"), 
       lwd=1, lty=c(1,2,1,2,1,2,1,2), cex=0.7)



