library(Quandl)
library(RPostgreSQL)

setwd('C:/Users/Luke/Documents/GitHub/Trading/')
Quandl.auth('rvmvEN6PUKj4V9nos2Ek')

# Connect to db
drv=dbDriver("PostgreSQL")
con=dbConnect(drv, dbname="Trading", user="postgres", password="")

# Get max date available from Quandl
args = commandArgs(trailingOnly = TRUE)
print(length(args))
if (length(args) == 0) {
  rundate = max(Quandl("WIKI/MSFT", start_date=Sys.Date()-30, end_date=Sys.Date())$Date)
} else {
  rundate = as.Date(args[1])
}

# Get max date from db and fill in all dates
if (length(args) == 0) {
  maxdate = dbGetQuery(con, "SELECT max(trade_date) as md FROM Prices")$md
  if (maxdate < rundate) { rundates = as.character(seq(maxdate+1, rundate, 1)) } else { rundates = as.character(rundate) }
} else {
  rundates = as.character(rundate)
}

stocklist = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/secwiki_tickers.csv'
destfile = 'stocklist.csv'

stocks = download.file(stocklist, 'stocks.csv')
stocks = read.csv('stocks.csv')
stocks = stocks[!is.na(stocks$Price),]
file.remove('stocks.csv')

for (rundate in rundates) {
  if (weekdays(as.Date(rundate)) %in% c('Saturday', 'Sunday')) { next }
  for (i in seq(1, nrow(stocks), 1)) {
    id = dbGetQuery(con, paste0("SELECT id FROM Security WHERE ticker = '", stocks$Ticker[i], "'"))$id
    if (is.null(id)) {
      # Add to security
      id = dbGetQuery(con, "SELECT COALESCE(max(id),0)+1 AS id FROM Security")
      dbSendQuery(con, paste0("INSERT INTO Security (id, ticker, description, active) VALUES (", id, ",'", stocks$Ticker[i], "','", gsub("'", "", stocks$Name[i]), "',TRUE)"))
    }
    
    # If price table already populated, skip this one
    cnt = dbGetQuery(con, paste0("SELECT count(*) AS cnt FROM Prices WHERE id = ", id, " AND trade_date = '", rundate, "'"))$cnt
    if (cnt > 0) { next }
    
    # Get price from Quandl
    success = FALSE
    while (!success) {
      print(paste0(Sys.time(), ": ", nrow(stocks)-i, ": ", stocks$Ticker[i]))
      tryCatch({price = Quandl(as.character(stocks$Price[i]), start_date=rundate, end_date=rundate); success=TRUE},
               error=function(e){
                 if (!grepl('does not exist', e$message)) {
                   print('Connection error, trying again in 30 seconds...')
                   print(e$message)
                   Sys.sleep(30)
                 } else {
                   print(e$message)
                   success <<- TRUE
                 }
               }
               )
    }
    if (exists('price')) {
      idx = which(!is.na(price$Open))
      if (length(idx) == 0) { idx = 1 }
      price = price[idx[1],]
      price[,is.na(price)] = 'NULL'
      
      if (nrow(price) != 0) {
        # Add to prices
        dbSendQuery(con, paste0("DELETE FROM Prices WHERE id = ", id, " AND trade_date = '", rundate, "'"))
        dbSendQuery(con, paste0("INSERT INTO Prices (id, trade_date, open_price, close_price, volume, split_ratio)
                          VALUES (", id, ",'", rundate, "',", price$Open, ",", price$Close, ",", price$Volume, ",", price['Split Ratio'],")"))
      }
      rm(price)
    }
  }
}
source('UpdateDBGoogleData.R')