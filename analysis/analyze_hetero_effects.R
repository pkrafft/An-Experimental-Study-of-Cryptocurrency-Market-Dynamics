
df = read.csv('tmp_hetero_effects.csv')

summary(lm(recent_last_trade_buy ~  
            total_60 + buy_total_60 +
            sell_total_60            +   last_trade_price +
            last_trade_total    +        highest_buy_price +
            highest_buy_total  +         lowest_sell_price + 
            lowest_sell_total +           spread,df))

summary(lm(last.percent.buy.BTC.volume ~          total_60 + buy_total_60 +
            sell_total_60            +   last_trade_price +
            last_trade_total    +        highest_buy_price +
            highest_buy_total  +         lowest_sell_price + 
            lowest_sell_total +           spread,df))


summary(lm(recent_last_trade_nonnull ~          total_60 + buy_total_60 +
            sell_total_60            +   last_trade_price +
            last_trade_total    +        highest_buy_price +
            highest_buy_total  +         lowest_sell_price + 
            lowest_sell_total +           spread,df))
