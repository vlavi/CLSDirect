library('RODBC')
library('lubridate')


# getOrderFlowData <-
#   function(dsn, tableName, fromDate, toDate, ccypair, product, party, cparty) {
#     # Source data
#     con = odbcConnect(dsn)
#     ccyp <- paste0("'",ccypair,"'",collapse = ",")
#     query = paste0('SELECT T1.[POPULATION_TIMESTAMP],T1.[QNDL_CODE],T1.[LONDON_DATE],T1.[BUSINESS_DATE],T1.[HOUR],',
#                    'T1.[PRICE_TAKER],T1.[MARKET_MAKER],T1.[BASE_CCY],T1.[COUNTER_CCY],T1.[BASE_CCY_BUY_VOLUME] AS base_buy_vol,',
#                    'T2.[BASE_CCY_SELL_VOLUME] AS base_sell_vol,T1.[BUY_TRADE_COUNT],T2.[SELL_TRADE_COUNT],T1.[CCY_PAIR],T1.[PRODUCT] ',
#                    'FROM ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[LONDON_DATE],[BUSINESS_DATE],[HOUR],[PRICE_TAKER],',
#                    '[MARKET_MAKER],[BASE_CCY],[COUNTER_CCY],[BASE_CCY_BUY_VOLUME],[BASE_CCY_SELL_VOLUME],',
#                    "[BUY_TRADE_COUNT] AS [BUY_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
#                    "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
#                    "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T1 ",
#                    'JOIN ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[LONDON_DATE],[BUSINESS_DATE],[HOUR],[PRICE_TAKER],',
#                    '[MARKET_MAKER],[BASE_CCY],[COUNTER_CCY],[BASE_CCY_BUY_VOLUME],[BASE_CCY_SELL_VOLUME],',
#                    "[BUY_TRADE_COUNT] AS [SELL_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
#                    "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
#                    "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T2 ",
#                    'ON (T1.LONDON_DATE = T2.LONDON_DATE AND T1.HOUR = T2.HOUR AND T1.PRICE_TAKER = T2.PRICE_TAKER AND ',
#                    'T1.MARKET_MAKER = T2.MARKET_MAKER AND T1.BASE_CCY = T2.BASE_CCY AND ', 
#                    'T1.COUNTER_CCY = T2.COUNTER_CCY AND LEFT(T1.CCY_PAIR, 3) = T1.BASE_CCY) ',
#                    'ORDER BY [LONDON_DATE], [HOUR], [PRICE_TAKER], [MARKET_MAKER], [CCY_PAIR], [BASE_CCY], [COUNTER_CCY] ')
#     
#     rec = sqlQuery(con, query)
#     odbcClose(con)
#     return(rec)
#   }

prjpath <- file.path("H:", "My Documents","Projects","MyR","QuantResearch")

source(file.path(prjpath, "New Data", "Script", "cls_mkt_data_lib.r"))

fp <-  file.path("H:", "My Documents","Projects","MyR","QuantResearch","GarganoFlow")
setwd(fp)

# ccy_pairs <- c('AUDJPY','AUDNZD','AUDUSD','CADJPY','EURAUD','EURCAD','EURCHF','EURDKK','EURGBP','EURHUF',
#                'EURJPY','EURNOK','EURSEK','EURUSD','GBPAUD','GBPCAD','GBPCHF','GBPJPY','GBPUSD','NZDUSD',
#                'USDCAD','USDCHF','USDDKK','USDHKD','USDHUF','USDILS','USDJPY','USDKRW','USDMXN','USDNOK',
#                'USDSEK','USDSGD','USDZAR')

ccy_pairs <- c('AUDCAD','AUDCHF','AUDJPY','AUDNZD','AUDUSD','CADJPY','CHFJPY','EURAUD','EURCAD','EURCHF',
               'EURDKK','EURGBP','EURHUF','EURJPY','EURNOK','EURNZD','EURSEK','EURSGD','EURUSD','GBPAUD',
               'GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDDKK',
               'USDHKD','USDHUF','USDILS','USDJPY','USDKRW','USDMXN','USDNOK','USDSEK','USDSGD','USDZAR')

dsn <- "GBQAGDB4"
tableName <- "[EDWBUS].[qndl].[EOD_ORDER_FLOW_DATASYNC]"
fromDate <- as.Date("2011-01-01")
toDate <- as.Date("2022-09-30")
ccyp <- ccy_pairs
product <- "Spot"
#party <- 'Bank'
party <- 'SellSide'
#cparties <- c('Fund', 'Non-Bank Financial', 'Corporate')
cparties <- c('BuySide')
cparty <- cparties[1]

for (cparty in cparties) {
  v <- getEodHourlyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                        ccypair = ccyp, product = product, party = party, cparty = cparty) 
  x <- split(x = v, f = v$CCY_PAIR)
  #browser()
  file_path <- file.path(fp, "In", cparty)
  for(e in names(x)) {
    write.csv(x[[e]][,c(3, 5, 10, 11, 12, 13)], file = file.path(file_path,paste0(e,".csv")), row.names = FALSE)
  }  
}


tableName <- "[EDWBUS].[qndl].[EOD_DAILY_SPT_FLOW_DATASYNC]"
f <- getEodDailyOrderFlowData(dsn, tableName, fromDate = fromDate, toDate = toDate, 
                         ccypair = ccyp, product = product, party = party, cparty = cparty) 
