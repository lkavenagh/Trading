library(RPostgreSQL)
library(RCurl)

setwd('C:/Users/Luke/Documents/GitHub/Trading/')

base_url = "http://www.google.com/finance/historical?output=csv&q="

start_date = as.Date('2013-11-20')
# start_date = Sys.Date()-7

# Connect to db
drv=dbDriver("PostgreSQL")
con=dbConnect(drv, dbname="Trading", user="postgres", password="")

# First populate any new stocks
stocklist = 'https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/secwiki_tickers.csv'
destfile = 'stocklist.csv'

stocks = download.file(stocklist, 'stocks.csv')
stocks = read.csv('stocks.csv')
stocks = stocks[!is.na(stocks$Price),]
file.remove('stocks.csv')

for (i in 1:nrow(stocks)) {
  cnt = dbGetQuery(con, paste0("SELECT count(id) as cnt FROM Security WHERE ticker = '", stocks$Ticker[i], "'"))$cnt
  if (cnt == 0) {
    # Insert into security
    print(paste0("Inserting new ticker: ", stocks$Ticker[i]))
    id = dbGetQuery(con, "SELECT COALESCE(max(id),0)+1 AS id FROM Security")
    dbSendQuery(con, paste0("INSERT INTO Security (id, ticker, description, active) VALUES (", id, ",'", stocks$Ticker[i], "','", gsub("'", "", stocks$Name[i]), "',TRUE)"))
  }
}

# Get tickers
q = "SELECT DISTINCT Ticker FROM Security WHERE active"
tickers = dbGetQuery(con, q)$ticker

for (i in 1:length(tickers)) {
  print(paste0(length(tickers)-i+1, ": ", tickers[i]))
  if (!url.exists(paste0(base_url, tickers[i]))) { next }
  success = FALSE
  while (!success) {
    tryCatch({download.file(paste0(base_url, tickers[i]), destfile = 'tmp.csv', quiet = TRUE); success=TRUE}, error=function(e)e)
  }
  prices = read.csv('tmp.csv')
  file.remove('tmp.csv')
  names(prices)[1] = 'trade_date'
  prices$trade_date = as.Date(as.character(prices$trade_date), format='%d-%b-%y')
  prices = prices[prices$trade_date >= start_date,]
  if (nrow(prices) == 0) {
    # No data, for some reason
    next
  }
  prices$Volume = as.numeric(as.character(prices$Volume))
  prices$Volume[is.na(prices$Volume)] = 'NULL'
  for (j in 1:nrow(prices)) {
    dt = prices$trade_date[j]
    tk = tickers[i]
    id = dbGetQuery(con, paste0("SELECT id FROM Security WHERE ticker = '", tk, "'"))$id
    if (is.null(id)) {
      # Should never happen...
      next
    }
    q = paste0("SELECT count(id) as cnt FROM Prices WHERE id = ", id, " and trade_date = '", dt, "'")
    cnt = dbGetQuery(con, q)$cnt
    if (cnt == 0) {
      # Insert into Prices
      dbSendQuery(con, paste0("INSERT INTO Prices (id, trade_date, open_price, close_price, volume, split_ratio)
                          VALUES (", id, ",'", dt, "',", prices$Open[j], ",", prices$Close[j], ",", prices$Volume[j], ",NULL)"))
    }
  }
}