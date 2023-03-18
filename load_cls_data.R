
#-------------------------------------------------------------
# Download CLS volume and flow data from InfoHub to CSV files
#-------------------------------------------------------------

library('RODBC')
library('lubridate')


prjpath <- file.path("G:", "QRS - QAG lead research","CLS_Market_Data")

source(file.path(prjpath, "Script", "cls_mkt_data_lib.r"))

fromDate <- as.Date("2011-01-01")
toDate <- as.Date("2023-02-28")

ccy_pairs <- c('AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD','CADJPY','CHFJPY','EURAUD','EURCAD','EURCHF',
               'EURDKK','EURGBP','EURHUF','EURJPY','EURNOK','EURNZD','EURSEK','EURSGD','EURUSD','GBPAUD',
               'GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDDKK',
               'USDHKD','USDHUF','USDILS','USDJPY','USDKRW','USDMXN','USDNOK','USDSEK','USDSGD','USDZAR')

dsn <- "GBQAGDB4"

# I. Volume
#------------

  # 1. Spot Volume
  
  # 1.1. Daily
  # 1.2. Hourly
  # 1.3. Daily Total At Hour

wd_path <- file.path(prjpath, 'Out', 'CLS', 'Volume', 'Spot', 'daily_total_at_hour')
setwd(wd_path)  
   
print('Eod Total Volumes At Hour')
 
product <- 'Spot'
hour <- 16
src <- "edwvw2.dbo.v_ds_qndl_volume_data_extract"

v <- getEodTotalVolumesAtHour(dsn, fromDate = fromDate, toDate = toDate, ccy_pairs = ccy_pairs, product = product, hour = hour, src = src)
x <- split(x = v, f = v$ccy_pair)
for(e in names(x)) {
  write.csv(x[[e]][,c(2,3)], file = paste0(e,".csv"), row.names = FALSE)
}  
  
  
  # 1. Spot Values
  
  # 1.1. Daily
  # 1.2. Hourly
  # 1.3. Daily Total At Hour

wd_path <- file.path(prjpath, 'Out', 'CLS', 'Value', 'Spot', 'daily_total_at_hour')
setwd(wd_path)  

print('Eod Total Values At Hour')

product <- 'Spot'
hour <- 16
src <- "edwvw2.dbo.v_ds_qndl_volume_data_extract"

v <- getEodTotalValuesAtHour(dsn, fromDate = fromDate, toDate = toDate, ccy_pairs = ccy_pairs, product = product, hour = hour, src = src)
x <- split(x = v, f = v$ccy_pair)
for(e in names(x)) {
  write.csv(x[[e]][,c(2,3)], file = paste0(e,".csv"), row.names = FALSE)
} 

# II. Flow
#------------
  
  # 1. Spot Flow
  
  # 1.1. Daily
 
print('Eod Daily Order Flow Data')
print("... Economic sectors")

tableName <- "[EDWBUS].[qndl].[EOD_DAILY_SPT_FLOW_DATASYNC]"
ccyp <- ccy_pairs  
product <- "Spot"

## Economic sectors

party <- 'Bank'
cparties <- c('Fund', 'Non-Bank Financial', 'Corporate')

for (cparty in cparties) {
  v <- getEodDailyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                                 ccypair = ccyp, product = product, party = party, cparty = cparty) 
  x <- split(x = v, f = v$CCY_PAIR)
  file_path <- file.path(prjpath, 'Out','CLS','Flow','Spot','daily_daily',cparty)
  for(e in names(x)) {
    write.csv(x[[e]], file = file.path(file_path,paste0(e,".csv")), row.names = FALSE)
  }  
}  

print("... BuySide - SellSide")

## BuySide-SellSide

party <- 'SellSide'
cparties <- 'BuySide'

for (cparty in cparties) {
  v <- getEodDailyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                                 ccypair = ccyp, product = product, party = party, cparty = cparty) 
  x <- split(x = v, f = v$CCY_PAIR)
  file_path <- file.path(prjpath, 'Out','CLS','Flow','Spot','daily_daily',cparty)
  for(e in names(x)) {
    write.csv(x[[e]], file = file.path(file_path,paste0(e,".csv")), row.names = FALSE)
  }  
}   

  
  # 1.2. Hourly

tableName <- "[EDWBUS].[qndl].[EOD_ORDER_FLOW_DATASYNC]"
ccyp <- ccy_pairs  
product <- "Spot"

print('Eod Hourly Order Flow Data')
print("... Economic sectors")

## Economic sectors

party <- 'Bank'
cparties <- c('Fund', 'Non-Bank Financial', 'Corporate')

for (cparty in cparties) {
  v <- getEodHourlyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                                 ccypair = ccyp, product = product, party = party, cparty = cparty) 
  x <- split(x = v, f = v$CCY_PAIR)
  file_path <- file.path(prjpath, 'Out','CLS','Flow','Spot','daily_hourly',cparty)
  for(e in names(x)) {
    write.csv(x[[e]][,c(3, 5, 10, 11, 12, 13)], file = file.path(file_path,paste0(e,".csv")), row.names = FALSE)
  }  
}  

print("... BuySide - SellSide")

## BuySide-SellSide

party <- 'SellSide'
cparties <- 'BuySide'

for (cparty in cparties) {
  v <- getEodHourlyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                                 ccypair = ccyp, product = product, party = party, cparty = cparty) 
  x <- split(x = v, f = v$CCY_PAIR)
  file_path <- file.path(prjpath, 'Out','CLS','Flow','Spot','daily_hourly',cparty)
  for(e in names(x)) {
    write.csv(x[[e]][,c(3, 5, 10, 11, 12, 13)], file = file.path(file_path,paste0(e,".csv")), row.names = FALSE)
  }  
}   

  # 1.3. Daily Total At Hour
  
print("End")
 
