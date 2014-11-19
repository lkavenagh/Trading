library(RPostgreSQL)
library(ggplot2)

setwd('C:/Users/Luke/Documents/GitHub/Trading/')

drv=dbDriver("PostgreSQL")
con=dbConnect(drv, dbname="Trading", user="postgres", password="")

q = paste0("SELECT trade_date, ticker, open_price, close_price, volume
            FROM Security s JOIN Prices p
            ON s.id = p.id
            ORDER BY ticker, trade_date")
data = dbGetQuery(con, q)

# Convert into matrices, one for each metric

ticker = unique(data$ticker)
trade_date = sort(unique(data$trade_date))

open = matrix(nrow=length(ticker), ncol = length(trade_date))
for (i in 1:length(ticker)) {
  idx = trade_date %in% data$trade_date[data$ticker == ticker[i]]
  open[i,idx] = data$open_price[data$ticker == ticker[i]]
}

close = matrix(nrow=length(ticker), ncol = length(trade_date))
for (i in 1:length(ticker)) {
  idx = trade_date %in% data$trade_date[data$ticker == ticker[i]]
  close[i,idx] = data$close_price[data$ticker == ticker[i]]
}

ret = cbind(rep(0,nrow(close)), (close[,2:ncol(close)] - close[,1:(ncol(close)-1)]) / close[,1:(ncol(close)-1)])


x = data.frame(dt = trade_date, open = open[91,], close = close[91,], ret = ret[91,])
ggplot(x, aes(x=dt)) + geom_line(aes(y=close, color = 'close')) + geom_line(aes(y=(ret*50+38), color = 'ret'))

# Are there patterns just in SPY?
# Find bounces, price has fallen, but now rising, how are returns distributed after that? (more positive than negative?)



