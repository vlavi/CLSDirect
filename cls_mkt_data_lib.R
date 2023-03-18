# Content
# 
#   Getting CLS volume and flow data from InfoHub
#
# - getEodHourlyOrderFlowData
# - getEodDailyOrderFlowData
# - getTotalVolumesAtHour
# - getTotalValuesAtHour
# - getEodHourlyVolumes
# - getEodHourlyValues
#
#   Loading CLS volume and flow data from CSV files
#
# - loadDailyVolumes
# - loadHourlyVolumes
# - loadEodDailyOrderFlowData
# - loadEodHourlyOrderFlowData
# 

getEodHourlyOrderFlowData <-
  function(dsn, tableName, fromDate, toDate, ccypair, product, party, cparty) {
    #' EOD hourly FX Flow data
    #
    #'@param dsn
    #'@param tableName
    #'@param fromDate
    #'@param toDate
    #'@param ccypair
    #'@param product
    #'@param party
    #'@param cparty
    #
    #'@return a data frame with columns 
    #   POPULATION_TIMESTAMP QNDL_CODE BUSINESS_DATE PRICE_TAKER MARKET_MAKER BUY_CCY 
    #   SELL_CCY base_buy_vol base_sell_vol BUY_TRADE_COUNT
    #
    #

    # Source data
    con = odbcConnect(dsn)
    ccyp <- paste0("'",ccypair,"'",collapse = ",")
    query = paste0('SELECT T1.[POPULATION_TIMESTAMP],T1.[QNDL_CODE],T1.[LONDON_DATE],T1.[BUSINESS_DATE],T1.[HOUR],',
                   'T1.[PRICE_TAKER],T1.[MARKET_MAKER],T1.[BASE_CCY],T1.[COUNTER_CCY],T1.[BASE_CCY_BUY_VOLUME] AS base_buy_vol,',
                   'T2.[BASE_CCY_SELL_VOLUME] AS base_sell_vol,T1.[BUY_TRADE_COUNT],T2.[SELL_TRADE_COUNT],T1.[CCY_PAIR],T1.[PRODUCT] ',
                   'FROM ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[LONDON_DATE],[BUSINESS_DATE],[HOUR],[PRICE_TAKER],',
                   '[MARKET_MAKER],[BASE_CCY],[COUNTER_CCY],[BASE_CCY_BUY_VOLUME],[BASE_CCY_SELL_VOLUME],',
                   "[BUY_TRADE_COUNT] AS [BUY_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
                   "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
                   "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T1 ",
                   'JOIN ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[LONDON_DATE],[BUSINESS_DATE],[HOUR],[PRICE_TAKER],',
                   '[MARKET_MAKER],[BASE_CCY],[COUNTER_CCY],[BASE_CCY_BUY_VOLUME],[BASE_CCY_SELL_VOLUME],',
                   "[BUY_TRADE_COUNT] AS [SELL_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
                   "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
                   "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T2 ",
                   'ON (T1.LONDON_DATE = T2.LONDON_DATE AND T1.HOUR = T2.HOUR AND T1.PRICE_TAKER = T2.PRICE_TAKER AND ',
                   'T1.MARKET_MAKER = T2.MARKET_MAKER AND T1.BASE_CCY = T2.BASE_CCY AND ', 
                   'T1.COUNTER_CCY = T2.COUNTER_CCY AND LEFT(T1.CCY_PAIR, 3) = T1.BASE_CCY) ',
                   'ORDER BY [LONDON_DATE], [HOUR], [PRICE_TAKER], [MARKET_MAKER], [CCY_PAIR], [BASE_CCY], [COUNTER_CCY] ')
    
    rec = sqlQuery(con, query)
    odbcClose(con)
    return(rec)
  }


getEodDailyOrderFlowData <-
  function(dsn, tableName, fromDate, toDate, ccypair, product, party, cparty) {
    #' EOD daily FX Flow data
    #
    #'@param dsn
    #'@param tableName
    #'@param fromDate
    #'@param toDate
    #'@param ccypair
    #'@param product
    #'@param party
    #'@param cparty
    #
    #'@return a data frame with columns 
    #   POPULATION_TIMESTAMP QNDL_CODE BUSINESS_DATE PRICE_TAKER MARKET_MAKER BUY_CCY SELL_CCY base_buy_vol base_sell_vol 
    #   BUY_TRADE_COUNT SELL_TRADE_COUNT CCY_PAIR PRODUCT
    #
    #
    
    # Source data
    con = odbcConnect(dsn)
    ccyp <- paste0("'",ccypair,"'",collapse = ",")
    query = paste0('SELECT T1.[POPULATION_TIMESTAMP],T1.[QNDL_CODE],T1.[BUSINESS_DATE],',
                   'T1.[PRICE_TAKER],T1.[MARKET_MAKER],T1.[BUY_CCY],T1.[SELL_CCY],T1.[BUY_VOLUME] AS base_buy_vol,',
                   'T2.[SELL_VOLUME] AS base_sell_vol,T1.[BUY_TRADE_COUNT],T2.[SELL_TRADE_COUNT],T1.[CCY_PAIR],T1.[PRODUCT] ',
                   'FROM ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[BUSINESS_DATE],[PRICE_TAKER],',
                   '[MARKET_MAKER],[BUY_CCY],[SELL_CCY],[BUY_VOLUME],[SELL_VOLUME],',
                   "[BUY_TRADE_COUNT] AS [BUY_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
                   "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
                   "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T1 ",
                   'JOIN ( SELECT [POPULATION_TIMESTAMP],[QNDL_CODE],[BUSINESS_DATE],[PRICE_TAKER],',
                   '[MARKET_MAKER],[BUY_CCY],[SELL_CCY],[BUY_VOLUME],[SELL_VOLUME],',
                   "[SELL_TRADE_COUNT] AS [SELL_TRADE_COUNT],[CCY_PAIR],[PRODUCT] FROM ",tableName," ",
                   "WHERE [BUSINESS_DATE] >= '", fromDate, "' AND [BUSINESS_DATE] <= '", toDate, "' AND ",
                   "( [MARKET_MAKER] = '", party, "' AND [PRICE_TAKER] = '", cparty, "' ) AND [CCY_PAIR] IN (",ccyp," )) T2 ",
                   'ON (T1.BUSINESS_DATE = T2.BUSINESS_DATE AND T1.PRICE_TAKER = T2.PRICE_TAKER AND ',
                   'T1.MARKET_MAKER = T2.MARKET_MAKER AND T1.BUY_CCY = T2.BUY_CCY AND ', 
                   'T1.SELL_CCY = T2.SELL_CCY AND LEFT(T1.CCY_PAIR, 3) = T1.BUY_CCY) ',
                   'ORDER BY [BUSINESS_DATE], [PRICE_TAKER], [MARKET_MAKER], [CCY_PAIR], [BUY_CCY], [SELL_CCY] ')

    rec = sqlQuery(con, query)
    odbcClose(con)
    return(rec)
  }


getEodTotalVolumesAtHour <- function(dsn, fromDate, toDate, ccy_pairs, product = 'Spot', hour = 16, src) {
  #' Total (cumulative) volume (trade count) at some hour calculated over past 24 hours
  #
  #'@param dsn
  #'@param fromDate
  #'@param toDate
  #'@param ccy_pairs
  #'@param product
  #'@param hour
  #'@param src - table name
  #
  #'@return a data frame with columns 
  #   ccy_pair date volume 
  #
  #  
  # Source data
  con = odbcConnect(dsn)
  from <- year(fromDate)*10000 + 100 * month(fromDate) + day(fromDate)
  to   <- year(toDate)*10000 + 100 * month(toDate) + day(toDate)
  ccyp <- paste0("'",ccy_pairs,"'",collapse = ",")
  innerQuery <- paste0("SELECT ccy_pair, CASE WHEN hour >= ", hour + 1,
                       " THEN dateadd(day, 1, [date]) ELSE [date] END AS date, sum(VOLUME) as volume",
                       " FROM ", src, " WHERE product = '", product,"' AND ccy_pair IN (", ccyp, ") ",
                       " AND [date] BETWEEN '", from,"'", " AND '", to,"'",
                       " GROUP BY ccy_pair, CASE WHEN hour >= ", hour + 1, " THEN dateadd(day, 1, [date]) ELSE [date] END")
  query = paste0("SELECT * FROM ( ",innerQuery," ) AS V ORDER BY ccy_pair, date")
  rec = sqlQuery(con, query)
  odbcClose(con)
  return(rec)
} 


getEodTotalValuesAtHour <- function(dsn, fromDate, toDate, ccy_pairs, product = 'Spot', hour = 16, src) {
  #' Total (cumulative) volume (in USD) at some hour calculated over past 24 hours
  #
  #'@param dsn
  #'@param fromDate
  #'@param toDate
  #'@param ccy_pairs
  #'@param product
  #'@param hour
  #'@param src - table name
  #
  #'@return a data frame with columns 
  #   ccy_pair date volume 
  #
  #    
  # Source data
  con = odbcConnect(dsn)
  from <- year(fromDate)*10000 + 100 * month(fromDate) + day(fromDate)
  to   <- year(toDate)*10000 + 100 * month(toDate) + day(toDate)
  ccyp <- paste0("'",ccy_pairs,"'",collapse = ",")
  innerQuery <- paste0("SELECT ccy_pair, CASE WHEN hour >= ", hour + 1,
                       " THEN dateadd(day, 1, [date]) ELSE [date] END AS date, sum(VALUE) as volume",
                       " FROM ", src, " WHERE product = '", product,"' AND ccy_pair IN (", ccyp, ") ",
                       " AND [date] BETWEEN '", from,"'", " AND '", to,"'",
                       " GROUP BY ccy_pair, CASE WHEN hour >= ", hour + 1, " THEN dateadd(day, 1, [date]) ELSE [date] END")
  query = paste0("SELECT * FROM ( ",innerQuery," ) AS V ORDER BY ccy_pair, date")
  rec = sqlQuery(con, query)
  odbcClose(con)
  return(rec)
} 


getEodHourlyVolumes <- function(dsn, fromDate, toDate, ccy_pairs, product = 'Spot', src) {
  #' EOD hourly volume (trade count) data
  #
  #'@param dsn
  #'@param fromDate
  #'@param toDate
  #'@param ccy_pairs
  #'@param product
  #'@param src - table name
  #
  #'@return a data frame with columns 
  #   DATE BUSINESSDATE HOUR ccy_pair volume 
  #
  #  

  # Source data
  con = odbcConnect(dsn)
  from <- year(fromDate)*10000 + 100 * month(fromDate) + day(fromDate)
  to   <- year(toDate)*10000 + 100 * month(toDate) + day(toDate)
  ccyp <- paste0("'",ccy_pairs,"'",collapse = ",")
  innerQuery <- paste0("SELECT [DATE], BUSINESSDATE, [HOUR],  ccy_pair, [VOLUME] as volume",
                       " FROM ", src, " WHERE product = '", product,"' AND ccy_pair IN (", ccyp, ") ",
                       " AND [date] BETWEEN '", from,"'", " AND '", to,"'")
  query = paste0("SELECT * FROM ( ",innerQuery," ) AS V ORDER BY ccy_pair, date, hour")
  rec = sqlQuery(con, query)
  odbcClose(con)
  return(rec)
} 

getEodHourlyValues <- function(dsn, fromDate, toDate, ccy_pairs, product = 'Spot', src) {
  #' EOD hourly value (in USD) data
  #
  #'@param dsn
  #'@param fromDate
  #'@param toDate
  #'@param ccy_pairs
  #'@param product
  #'@param src - table name
  #
  #'@return a data frame with columns 
  #   DATE BUSINESSDATE HOUR ccy_pair volume 
  #
  #
  
  # Source data
  con = odbcConnect(dsn)
  from <- year(fromDate)*10000 + 100 * month(fromDate) + day(fromDate)
  to   <- year(toDate)*10000 + 100 * month(toDate) + day(toDate)
  ccyp <- paste0("'",ccy_pairs,"'",collapse = ",")
  innerQuery <- paste0("SELECT [DATE], BUSINESSDATE, [HOUR],  ccy_pair, [VALUE] as volume",
                       " FROM ", src, " WHERE product = '", product,"' AND ccy_pair IN (", ccyp, ") ",
                       " AND [date] BETWEEN '", from,"'", " AND '", to,"'")
  query = paste0("SELECT * FROM ( ",innerQuery," ) AS V ORDER BY ccy_pair, date, hour")
  rec = sqlQuery(con, query)
  odbcClose(con)
  return(rec)
} 

#########################################

loadDailyVolumes <- function(symbol_list, path) {
  #' Create a volume matrix
  #
  #'@param symbol_list, a list of currency pairs
  #'@param path, a path to the directory with files
  #
  #'@return volumes_matrix, an xts object with symbol_list as column names
  #   
  # CSV files have two columns: "date","volume"
  #

  volumes_matrix <- NULL
  
  for( sym in symbol_list){
    
    fileName <- paste0(path,'/',sym,'.csv')
    y <- read.csv(fileName, stringsAsFactors = FALSE)
    
    if (substr(y[1,1],3,3) == "/") {
      y <- xts(y[, -1], as.Date(y[, 1], format = "%d/%m/%Y"), src = "csv")    
    } else {
      y <- xts(y[, -1], as.Date(y[, 1], format = "%Y-%m-%d"), src = "csv")
    }
    colnames(y) <- paste(toupper(gsub("\\^", "", sym  )), 
                         c("Volume"), 
                         sep = ".")   
    volumes_matrix <- merge.xts(volumes_matrix,y)
  }
  
  # Substitute zeros and NAs with max values to push down in ordering
  for (i in 1:ncol(volumes_matrix)) {
    for (j in 2:nrow(volumes_matrix)) {
      if (volumes_matrix[j,i] == 0 || is.na(volumes_matrix[j,i]))
        volumes_matrix[j,i] <- max(volumes_matrix[1:j,i], na.rm = TRUE) #volumes_matrix[j-1,i]
    }
  }  
  
  return(volumes_matrix)  
}


loadEodDailyOrderFlowData <- function(symbol_list, path, print_info = FALSE) {
  #' Create a list of daily flows
  #
  #'@param symbol_list, a list of currency pairs
  #'@param path, a path to the directory with files
  #'@param print_info, boolean
  #
  #'@return flow_df, a data.frame with the following columns:
  #   
  #  BUSINESS_DATE, base_buy_vol, base_sell_vol, BUY_TRADE_COUNT, SELL_TRADE_COUNT
  #  
  
  flow_df <- NULL
  
  for (sym in symbol_list) {
    if (print_info)
      print(sym)
    
    fileName <- file.path(path, paste0(sym,'.csv'))
    if (!file.exists(fileName)) return(NULL)
    
    flow <- read.csv(fileName, stringsAsFactors = FALSE) 
    flow$BUSINESS_DATE <- as.Date(flow$BUSINESS_DATE, format = "%Y-%m-%d")
    
    if (is.null(flow_df)) {
      flow_df <- flow
    } else {
      flow_df <- rbind(flow_df, flow)
    }
    
  }
  return(flow_df)
}


loadEodHourlyOrderFlowData <- function(symbol_list, path, print_info = FALSE) {
  #' Create a list of daily flows
  #
  #'@param symbol_list, a list of currency pairs
  #'@param path, a path to the directory with files
  #'@param print_info, boolean
  #
  #'@return flow_df, a data.frame with the following columns:
  #   
  #  LONDON_DATE, HOUR, base_buy_vol, base_sell_vol, BUY_TRADE_COUNT, SELL_TRADE_COUNT
  #  
  
  flow_df <- NULL
  
  for (sym in symbol_list) {
    if (print_info)
      print(sym)
    
    fileName <- file.path(path, paste0(sym,'.csv'))
    if (!file.exists(fileName)) return(NULL)

    flow <- read.csv(fileName, stringsAsFactors = FALSE) 
    flow$LONDON_DATE <- as.Date(flow$LONDON_DATE, format = "%Y-%m-%d")
    
    if (is.null(flow_df)) {
      flow_df <- flow
    } else {
      flow_df <- rbind(flow_df, flow)
    }
    
  }
  return(flow_df)
}


loadHourlyVolumes <- function(symbol_list, path) {
  #' Create a list of hourly flows
  #
  #'@param symbol_list, a list of currency pairs
  #'@param path, a path to the directory with files
  #
  #'@return a list of xts objects with this column names:
  #   
  # CSV files have these columns: 
  #   "LONDON_DATE","HOUR","VOLUME","TRADE_COUNT"
  
  volume_df <- NULL
  
  for (sym in symbol_list) {
    if (print_info)
      print(sym)
    
    fileName <- file.path(path, paste0(sym,'.csv'))
    if (!file.exists(fileName)) return(NULL)

    volume <- read.csv(fileName, stringsAsFactors = FALSE) 
    volume$LONDON_DATE <- as.Date(volume$LONDON_DATE, format = "%Y-%m-%d")
    
    if (is.null(volume_df)) {
      volume_df <- volume
    } else {
      volume_df <- rbind(volume_df, volume)
    }
    
  }
  return(volume_df)
  
}
