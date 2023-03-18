library('RODBC')
library('lubridate')


prjpath <- file.path("H:", "My Documents","Projects","MyR","QuantResearch")

source(file.path(prjpath, "New Data", "Script", "cls_mkt_data_lib.r"))

fromDate <- as.Date("2011-01-01")
toDate <- as.Date("2022-09-30")

ccy_pairs <- c('AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD','CADJPY','CHFJPY','EURAUD','EURCAD','EURCHF',
               'EURDKK','EURGBP','EURHUF','EURJPY','EURNOK','EURNZD','EURSEK','EURSGD','EURUSD','GBPAUD',
               'GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDDKK',
               'USDHKD','USDHUF','USDILS','USDJPY','USDKRW','USDMXN','USDNOK','USDSEK','USDSGD','USDZAR')

product <- 'Spot'
hour <- 16
src <- "edwvw2.dbo.v_ds_qndl_volume_data_extract"
dsn <- "GBQAGDB4"

v <- getEodTotalVolumesAtHour(dsn, fromDate = fromDate, toDate = toDate, ccy_pairs = ccy_pairs, product = product, hour = hour, src = src)
x <- split(x = v, f = v$ccy_pair)
for(e in names(x)) {
  write.csv(x[[e]][,c(2,3)], file = paste0(e,".csv"), row.names = FALSE)
}

# v <- getEodHourlyVolumes(dsn, fromDate = fromDate, toDate = toDate, ccy_pairs = ccy_pairs, product = product, src = src)
# head(v)
# 
# v <- getEodHourlyValues(dsn, fromDate = fromDate, toDate = toDate, ccy_pairs = ccy_pairs, product = product, src = src)
# head(v)
  

  
