
df = read.csv('tmp_bot_check.csv')
df[,'workhours'] = (df[,'humantimes']=='True') * (df[,'weekday']=='True')

summary(lm(recent_last_trade_buy ~ condition*workhours, data = df))
summary(lm(last.percent.buy.BTC.volume ~ condition*workhours, data = df))
summary(lm(recent_last_trade_nonnull ~ condition*workhours, data = df))