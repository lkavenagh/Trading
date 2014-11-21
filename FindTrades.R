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

qty = matrix(0, nrow = nrow(ret), ncol = ncol(ret))

cash = 10000
N = 25

buy = sample(1:nrow(qty), N)
qty[buy,1] = floor((cash/N)/close[buy,1])
cash = cash - sum(qty[buy,1] * close[buy,1], na.rm=TRUE)

for (i in 2:ncol(qty)) {
  qty[,i] = qty[,i-1]
  exp = qty * close
  retUSD = ret * exp
  # which stocks have we lost more than 1% in?
  idx = which((rowSums(retUSD[,1:i]) / exp[,1]) < 0)
  
  # Get out of those
  cash = cash + sum(qty[idx,i] * close[idx,i])
  qty[idx,i] = 0
  
  # Redistribute?
  if (cash > 1000) {
    buy = qty[,i] > 0
    qty[buy,1] = qty[buy,i] + floor((cash/N)/close[buy,1])
    
  }
}

print(paste0("Ended up with ", sum(exp[,ncol(exp)], na.rm=TRUE) + cash, " total USD."))

SPYexp = rep(25*500,length(trade_date))
SPYret = ret[ticker == 'SPY']
SPYretUSD = SPYexp * SPYret
ggplot(NULL, aes(x=trade_date)) + geom_line(aes(y=cumsum(colSums(retUSD, na.rm=TRUE)), color = 'MyPortfolio')) + geom_line(aes(y=cumsum(SPYretUSD), color = 'SPY'))