rm(list=ls())

Sys.setlocale(category = "LC_ALL", locale = "English_United States.1252")

##--------------------------------------------------------------------------------------
## Specify the directory of project files
##--------------------------------------------------------------------------------------

RworkingFolder <- "E:/GB_files/Modelling_and_Analytics/Working_files"

## Set a month and project folder
month <- ""
projectFolder <-"Comtrade"

myfiledir <- file.path(RworkingFolder,month,projectFolder)

## remove the # in order to run this install.packages line only once
# install.packages( c("data.table", "XML2R" , "stringr", "plyr") )

library(XML2R)
library(stringr)
library(plyr)
library(data.table)

setwd(myfiledir)

source("../commonCode/10_functions.R")

# if the analysis output directory doesn't exist, create it
output.a.directory <- paste0( getwd() , "/output/" )
try( dir.create( output.a.directory ) , silent = T )

## Countries list loaded from countries_list.csv for which we want to download data
## For some countries codes change over time - will have to do some data cleaning
countries_from_csv <- read.table("input/countries_list.csv" , header = TRUE, sep = ",", quote = "\"",
                                 dec = ".", fill = TRUE, comment.char = "", stringsAsFactors = FALSE)
## Build reporter countries list
country_list <- str_sub(gsub(" ","",as.character(data.frame(countries_from_csv$ctyCode))),3,-2)
## Partner countries list
country_list_p <- country_list

## Commodity codes loaded from ccodes_list.csv file
ccodes_from_csv <- read.table("input/ccodes_list.csv" , header = FALSE, sep = ",", quote = "\"",
                              dec = ".", fill = TRUE, comment.char = "", stringsAsFactors = FALSE)
ccodes_list <- as.list(as.character(as.matrix(ccodes_from_csv)))

Kyears <- 1996:2014

years_list <- as.list(paste0("",Kyears,""))

## Select classification
classif = "h1"

## Comtrade "get data" URL
get_url <- "http://comtrade.un.org/ws/get.aspx?"

## Need an authorization key if downloading from non-work computer
auth_key <- "" # No need for key if using work computer with access
# auth_key <- "&code=yourauthorizationcode"

# ## Use this to test if you are getting access to Comtrade data
# classif = "h1"
# year = "2005"
# ccode = "57"
# country_list = "12,32,36"
# country_list_p = "12,32,36"
# url1 <- paste0(get_url,"px=",classif,"&r=",country_list,"&y=",year,"&cc=",ccode,"&p=",country_list_p,"&comp=false",auth_key)
# xmlToDataFrame(url1,stringsAsFactors=FALSE)

# Create a progress-bar
progress.bar <- create_progress_bar("time")

DT_data_RAW_comb <- list()

cat("\n","Downloading data from UN Comtrade, started at",format(Sys.time()),"\n")
# Initializing progress-bar
progress.bar$init(length(years_list)*length(ccodes_list))
for(year in years_list) {
   DT_data_RAW <- list()

   for(ccode in ccodes_list) {
      ptm<-Sys.time()

      # Add 0 in front for odd codes
      if(nchar(ccode) %% 2 == 1) {
         ccode <- paste0("0",ccode)
      }

      ## Build url for data download
      url1 <- paste0(get_url,"px=",classif,"&r=",country_list,"&y=",year,"&cc=",ccode,"&p=",country_list_p,"&comp=false",auth_key)

      ## Downloading data by year and commodity
      data <- try(xmlToDataFrame(url1,stringsAsFactors=FALSE), silent = T )

      if (class(data) == "try-error") {
         ## Try downloading again
         time<-Sys.time()-ptm
         cat("\n","year =",year,", ccode =",ccode,": downloading FAILED, trying again,",signif(time[[1]], digits = 5),"sec","\n")
         data <- try(xmlToDataFrame(url1,stringsAsFactors=FALSE), silent = T )
         ptm<-Sys.time()
      }

      if (class(data) == "try-error") {
         time<-Sys.time()-ptm
         cat("\n","year =",year,", ccode =",ccode,": downloading FAILED, skipping",signif(time[[1]], digits = 5),"sec","\n")
      }
      else if(nrow(data)==0) {
         time<-Sys.time()-ptm
         cat("\n","year =",year,", ccode =",ccode,": NO DATA,",signif(time[[1]], digits = 5),"sec","\n")
      }
      else {
         ## Successful download
         DT_data_RAW[[ccode]] <- data
         time<-Sys.time()-ptm
         # cat("year =",year,", ccode =",ccode,":",signif(time[[1]], digits = 5),"sec","\n")
      }

      progress.bar$step()
   }
   ## Combine data across categories
   DT_data_RAW_comb[[year]] <- do.call(rbind.data.frame, DT_data_RAW)
   rm(DT_data_RAW,data)
   ## Save intermediate output for each year as compressed csv
   csvfile <- gzfile(paste0("output/Comtrade_data_",year,".csv.gz"))
   write.csv(DT_data_RAW_comb[[year]],file=csvfile,row.names=FALSE, na = "")

   # ----------------------------------------------------------------------
   ## ALTERNATIVE 1: Use this if PC does not have enough RAM memory (requirements depend on country/year/sector selections scope)
   # ----------------------------------------------------------------------
   DT_data_RAW_comb[[year]] <- NULL
   gc()
   # ----------------------------------------------------------------------
}

## Combine data across years
# ----------------------------------------------------------------------
## ALTERNATIVE 1: If PC does not have enough RAM memory: load data from csv file
# ----------------------------------------------------------------------
years_list <- as.list(paste0("",Kyears,""))
for(year in years_list) {
   if (year==years_list[1]) {
      csvfile <- gzfile(paste0("output/Comtrade_data_",year,".csv.gz"))
      DT_data_comb <- read.table(file=csvfile , header = TRUE, sep = ",", quote = "\"",
                         dec = ".", fill = TRUE, comment.char = "", stringsAsFactors = FALSE)
   }
   else {
      csvfile <- gzfile(paste0("output/Comtrade_data_",year,".csv.gz"))
      DT_data_interm <- read.table(file=csvfile , header = TRUE, sep = ",", quote = "\"",
                         dec = ".", fill = TRUE, comment.char = "", stringsAsFactors = FALSE)
      DT_data_comb <- rbind(DT_data_comb,DT_data_interm)
   }
}
rm(DT_data_interm)
# ----------------------------------------------------------------------
## ALTERNATIVE 2: if PC has large RAM memory
# ----------------------------------------------------------------------
# DT_data_comb <- do.call(rbind.data.frame, DT_data_RAW_comb)
# ----------------------------------------------------------------------

rm(DT_data_RAW_comb)
DT_data_comb <- data.table(DT_data_comb)

## Summary stats
str(DT_data_comb)
table(DT_data_comb$cmdCode)
table(DT_data_comb$yr)
table(DT_data_comb$rgCode)

## Country composition changes over time
# Belgium   56   Belgium	1999	2061
# Belgium (prev)   58   Belgium-Luxembourg	1962	1998
# Serbia	688	Serbia	2006	2061
# Serbia (prev)   891	Serbia and Montenegro	1992	2005
# South Africa	710	South Africa	2000	2061
# South Africa (prev)	711	Southern African Customs Union	1962	1999
DT_data_comb[rtCode==58 & yr<=1998,':='(rtCode=56)]
DT_data_comb[ptCode==58 & yr<=1998,':='(ptCode=56)]
DT_data_comb[rtCode==891 & yr<=2005,':='(rtCode=688)]
DT_data_comb[ptCode==891 & yr<=2005,':='(ptCode=688)]
DT_data_comb[rtCode==711 & yr<=1999,':='(rtCode=710)]
DT_data_comb[ptCode==711 & yr<=1999,':='(ptCode=710)]

# Check if you get expected number of reporting/partner countries (also by year)
length(table(DT_data_comb$rtCode))
length(table(DT_data_comb$ptCode))
table(DT_data_comb$rtCode,DT_data_comb$yr)
table(DT_data_comb$ptCode,DT_data_comb$yr)

## Save data file as compressed csv
csvfile <- gzfile(paste0("output/Comtrade_data_",Sys.Date(),".csv.gz"))
write.csv(DT_data_comb,file=csvfile,row.names=FALSE, na = "")

## Check if in all cases there is one record by record type for each country pair
DT_data_comb[,':='(xcount=sum(!is.na(TradeValue))),by=c("yr","ptCode","rtCode","rgCode","cmdCode")]
table(DT_data_comb$xcount)
set(DT_data_comb, j=c("xcount"), value=NULL)

rgCode <- 1:4
Flow_type <- c("Imports","Exports","Re-Exports","Re-Imports")
Flow_type.data <- data.table(data.frame(rgCode,Flow_type))



NAD_commodities <- c(4,5,7,8,9,10,11,12,13,14,17,18,19,20,21,22)
DT_data_NAD <- subset(DT_data_comb,(cmdCode %in% NAD_commodities) & !(ptCode==rtCode),select=c("yr","ptCode","rtCode","rgCode","TradeValue"))
DT_data_NAD <-aggregate(TradeValue~yr+ptCode+rtCode+rgCode,data=DT_data_NAD,sum,na.rm=TRUE)
sumstats(DT_data_NAD)
length(table(DT_data_NAD$rtCode))
length(table(DT_data_NAD$ptCode))
DT_data_NAD <- data.table(merge(DT_data_NAD,countries_from_csv,by.x="ptCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_NAD,c("yr","Country"),c("Year","Partner_Country"))
DT_data_NAD <- data.frame(DT_data_NAD)
DT_data_NAD <- data.table(merge(DT_data_NAD,countries_from_csv,by.x="rtCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_NAD,c("Country"),c("Reporter_Country"))
DT_data_NAD <- merge(DT_data_NAD,Flow_type.data,by="rgCode",all.x=TRUE)
table(DT_data_NAD$Flow_type)
DT_data_NAD <- subset(DT_data_NAD,select=c(Year,Flow_type,Reporter_Country,Partner_Country,TradeValue))
str(DT_data_NAD)
sumstats(DT_data_NAD)
setkey(DT_data_NAD,Reporter_Country,Partner_Country,Flow_type,Year)

## Save data file as compressed csv
csvfile <- gzfile(paste0("output/IntTrade_data_NAD.csv.gz"))
write.csv(DT_data_NAD,file=csvfile,row.names=FALSE, na = "")

# ----------------------------------------------------------------------

CT_commodities <- c(06,12,13,15,18,22,25,27,28,29,30,32,33,34,35,38,39,48,50,52,70,71)
DT_data_CT <- subset(DT_data_comb,(cmdCode %in% CT_commodities) & !(ptCode==rtCode),select=c("yr","ptCode","rtCode","rgCode","TradeValue"))
DT_data_CT <- aggregate(TradeValue~yr+ptCode+rtCode+rgCode,data=DT_data_CT,sum,na.rm=TRUE)
sumstats(DT_data_CT)
length(table(DT_data_CT$rtCode))
length(table(DT_data_CT$ptCode))
DT_data_CT <- data.table(merge(DT_data_CT,countries_from_csv,by.x="ptCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_CT,c("yr","Country"),c("Year","Partner_Country"))
DT_data_CT <- data.frame(DT_data_CT)
DT_data_CT <- data.table(merge(DT_data_CT,countries_from_csv,by.x="rtCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_CT,c("Country"),c("Reporter_Country"))
DT_data_CT <- merge(DT_data_CT,Flow_type.data,by="rgCode",all.x=TRUE)
table(DT_data_CT$Flow_type)
DT_data_CT <- subset(DT_data_CT,select=c(Year,Flow_type,Reporter_Country,Partner_Country,TradeValue))
str(DT_data_CT)
sumstats(DT_data_CT)
setkey(DT_data_CT,Reporter_Country,Partner_Country,Flow_type,Year)

## Save data file as compressed csv
csvfile <- gzfile(paste0("output/IntTrade_data_CT.csv.gz"))
write.csv(DT_data_CT,file=csvfile,row.names=FALSE, na = "")

# ----------------------------------------------------------------------

PK_commodities <- c(02,03,04,05,07,08,10,11,12,13,14,15,16,17,18,19,20,21)
DT_data_PK <- subset(DT_data_comb,(cmdCode %in% PK_commodities) & !(ptCode==rtCode),selePK=c("yr","ptCode","rtCode","rgCode","TradeValue"))
DT_data_PK <- aggregate(TradeValue~yr+ptCode+rtCode+rgCode,data=DT_data_PK,sum,na.rm=TRUE)
sumstats(DT_data_PK)
length(table(DT_data_PK$rtCode))
length(table(DT_data_PK$ptCode))
DT_data_PK <- data.table(merge(DT_data_PK,countries_from_csv,by.x="ptCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_PK,c("yr","Country"),c("Year","Partner_Country"))
DT_data_PK <- data.frame(DT_data_PK)
DT_data_PK <- data.table(merge(DT_data_PK,countries_from_csv,by.x="rtCode",by.y="ctyCode",all.x=TRUE))
setnames(DT_data_PK,c("Country"),c("Reporter_Country"))
DT_data_PK <- merge(DT_data_PK,Flow_type.data,by="rgCode",all.x=TRUE)
table(DT_data_PK$Flow_type)
DT_data_PK <- subset(DT_data_PK,selePK=c(Year,Flow_type,Reporter_Country,Partner_Country,TradeValue))
str(DT_data_PK)
sumstats(DT_data_PK)
setkey(DT_data_PK,Reporter_Country,Partner_Country,Flow_type,Year)

## Save data file as compressed csv
csvfile <- gzfile(paste0("output/IntTrade_data_PK.csv.gz"))
write.csv(DT_data_PK,file=csvfile,row.names=FALSE, na = "")

# ----------------------------------------------------------------------
## Created variables
# pfCode : Classifications
# yr: Year
# rgCode: Trade Flow - 1 Imports, 2 Exports, 3 Re-Exports, 4 Re-Imports
# rtCode: Reporter country
# ptCode: Partner country
# cmdCode: Commodity code
# cmdID: Commodity ID
# qtCode : Quantity code -
#  1   -   No quantity,
#  2   m2	Area in square meters,
#  3	1000 kWh	Electrical energy in thousands of kilowatt-hours,
#  4	m	Length in meters
#  5	u	Number of items,
#  6	2u	Number of pairs,
#  7	l	Volume in liters,
#  8	kg	Weight in kilograms,
#  9	1000u	Thousand of items
#  10	U (jeu/pack)	Number of packages,
#  11	12u	Dozen of items,
#  12	m3	Volume in cubic meters,
#  13	carat	Weight in carats
# TradeQuantity
# NetWeight
# TradeValue
# estCode : 0 = no estimation, 2 = quantity, 4 = netweight, 6 = both quantity and netweight
# htCode : not sure
# ----------------------------------------------------------------------

# ## Interface to Comtrade data web access
#
# Parameters
# ----------
#
# * px - Commodity Classifications - HS, H0-H3, ST, S1-S4, BE
# * r - Reporting Countries - UN Comtrade Country Codes or Country Groups
# * y - Years - 4 digits year
# * cc - Commodity Codes - Commodity Codes, with wild cards or Groups
# * p - Partner Countries - UN Comtrade Country Codes or Country Groups
# * rg - Trade Flow - Number 1 to 4 (1 Imports, 2 Exports, 3 Re-Exports, 4	Re-Imports)
# * so - Sort Order - See Below
# * tv1 - Comparison Sign - See Below
# * tv2 - Comparison Value - See Below
# * qt - Aggregation TradeValalue - y or n
# * lowT - Start Date / Time - Date format YYYY-MM-DD
# * HighT - End Date / Time - Date format YYYY-MM-DD
# * comp - Data Compression - True or False
# * isOri - Data - true or False
# * max - Max returned records - Number
# * app - Application Identifier - Any text (optional)
# * count  To count no of records - True or False
# * async - Asynchronous Web Call - True or False
# * coded - Authorization Code - Use for off-site web services acces.
#
# http://comtrade.un.org/db/dqBasicQueryResults.aspx?cc=TOTAL&px=H6&r=372&y=2006
#
# Returns
# -------
#
# * Pandas dataFrame
# * optionally as a csv?
#
# Based off: http://unstats.un.org/unsd/tradekb/Knowledgebase/
#    Access-Points-and-Parameters-for-Web-Services
# Sorting:
#    so= pre-defined sort order ; possible values
# "1" Year;Flow;Rep;Comm;Ptnr;
# "2" Year;Rep;Flow;Comm;Ptnr;
# "3" Flow;Year;Rep;Comm;Ptnr;
# "4" Flow;Rep;Year;Comm;Ptnr;
# "5" Rep ;Year;Flow;Comm;Ptnr;
# "6" Rep ;Flow;Year;Comm;Ptnr;
# "7" Comm;Year;Flow;Rep ;Ptnr;
# "8" Comm;Year;Rep ;Flow;Ptnr;
# "9" Comm;Flow;Year;Rep ;Ptnr;
# "10" Comm;Flow;Rep ;Year;Ptnr
# "11" Comm;Rep ;Year;Flow;Ptnr;
# "12" Comm;Rep ;Flow;Year;Ptnr;
# "13" Year;Flow;Rep;Comm;TradeVal;
# "14" Year;Rep;Flow;Comm;TradeVal;
# "15" Flow;Year;Rep;Comm;TradeVal;
# "16" Flow;Rep;Year;Comm;TradeVal;
# "17" Rep ;Year;Flow;Comm;TradeVal;
# "18" Rep ;Flow;Year;Comm;TradeVal;
# "19" Comm;Year;Flow;Rep ;TradeVal;
# "20" Comm;Year;Rep ;Flow;TradeVal;
# "21" Comm;Flow;Year;Rep ;TradeVal;
# "22" Comm;Flow;Rep ;Year;TradeVal
# "23" Comm;Rep ;Year;Flow;TradeVal;
# "24" Comm;Rep ;Flow;Year;TradeVal;
# "1001" TradeVal;
# "9999" --None--
#
# For an example, so=13 will order the result by year, flow, reporter,
#    commodity and value
#
# Filter Trade value, use tv1 and tv2
# tv1=comparison sign:
#  0"Greater Than Equal
#  1"Greater Than
#  2"Less Than Equal
#  3"Less Than
# tv2= comparison value in US$
#
# For an example, tv1=0&tv2=1000000000 will filter the result for
#    trade value >= 1 billion US$
#
# Aggregation option
# qt=n or qt=y. If qt set to n, the system will keep the quantity
#    differences during the on-fly commodity aggregation.



