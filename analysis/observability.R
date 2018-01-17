
library(plm)
library(lmtest)

df = read.csv('tmp_observability.csv')

#mdf = df[df['monitor_num'] > 0,]
mdf = df[df['monitor_num'] == 1,]

mdf['buy'] = as.integer(mdf['condition'] == 'buy')
mdf['sell'] = as.integer(mdf['condition'] == 'sell')
mdf['treatment'] = 1 - as.integer(mdf['condition'] == 'control')

mdf['initial_total_rel'] = log(mdf['initial_total_60'] / exp(mdf['coin_total_60']))

mdf = mdf[mdf['treatment'] == 1,]


fit = lm(recent_last_trade_nonnull ~ buy * (buy_state +
             sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel),
             data = mdf)

summary(fit)

fit = lm(recent_last_trade_nonnull ~ buy * (buy_state +
             sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel +
	     coin_recent_last_trade_buy + 
	     coin_last.percent.buy.BTC.volume + 
	     coin_trivial + 
	     coin_total_60 + 
	     coin_last_trade_price + 
	     coin_highest_buy_total + 
	     coin_lowest_sell_total + 
	     coin_spread),
             data = mdf)

summary(fit)

fit = lm(recent_last_trade_nonnull ~ buy * (coin_name + buy_state +
             sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel),
             data = mdf)

summary(fit)