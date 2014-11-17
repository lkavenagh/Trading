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
  rundate = max(Quandl("WIKI/MSFT", start_date=rundate-30, end_date=rundate)$Date)
} else {
  rundate = as.Date(args[1])
}

# Get max date from db and fill in all dates
if (length(args) == 0) {
  maxdate = dbGetQuery(con, "SELECT max(trade_date) as md FROM Prices")$md
  if (maxdate < rundate) { rundates = as.character(seq(maxdate, rundate, 1)) } else { rundates = as.character(rundate) }
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
  for (i in 1:nrow(stocks)) {
    print(paste0(nrow(stocks)-i, ": ", stocks$Ticker[i]))
    price = NA
    id = dbGetQuery(con, paste0("SELECT id FROM Security WHERE ticker = '", stocks$Ticker[i], "'"))$id
    if (is.null(id)) {
      # Add to security
      id = dbGetQuery(con, "SELECT COALESCE(max(id),0)+1 AS id FROM Security")
      dbSendQuery(con, paste0("INSERT INTO Security (id, ticker, description, active) VALUES (", id, ",'", stocks$Ticker[i], "','", gsub("'", "", stocks$Name[i]), "',TRUE)"))
    }
    tryCatch({price = Quandl(as.character(stocks$Price[i]), start_date=rundate, end_date=rundate)},
             error=function(e){
               if (!grepl('does not exist', e$message)) {
                 Sys.sleep(5)
                 tryCatch({price = Quandl(as.character(stocks$Price[i]), start_date=rundate, end_date=rundate)},error=function(e)print(e$message))
               } else {
                 print(e$message)
               }
             })
    
    if (!is.na(price)) {
      idx = which(!is.na(price$Open))
      price = price[idx[1],]
      # Add to prices
      dbSendQuery(con, paste0("DELETE FROM Prices WHERE id = ", id, " AND trade_date = '", rundate, "'"))
      dbSendQuery(con, paste0("INSERT INTO Prices (id, trade_date, open_price, close_price, volume, split_ratio)
                            VALUES (", id, ",'", rundate, "',", price$Open, ",", price$Close, ",", price$Volume, ",", price['Split Ratio'],")"))
    }
  }
}