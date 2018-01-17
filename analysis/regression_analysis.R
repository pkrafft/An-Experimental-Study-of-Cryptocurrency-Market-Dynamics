library(plm)
library(lmtest)

df = read.csv('tmp_regression_features.csv')

mdf = df[df['monitor_num'] > 0,]

mdf['buy'] = as.integer(mdf['condition'] == 'buy')
mdf['sell'] = as.integer(mdf['condition'] == 'sell')
mdf['treatment'] = 1 - as.integer(mdf['condition'] == 'control')

mdf['initial_total_rel'] = log(mdf['initial_total_60'] / exp(mdf['coin_total_60']))

results = list()

fit = plm(recent_last_trade_buy ~ coin_name + factor(monitor_num)*buy + factor(monitor_num)*
             sell + buy_state + sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel,
         data = mdf, index=c("coin_name"), model="within")

results[['buy']] = coeftest(fit, vcov.=function(x) vcovHC(x, method = "white1", type = "HC0", cluster = "group"))

fit = plm(last.percent.buy.BTC.volume ~ coin_name + factor(monitor_num)*buy + factor(monitor_num)*
             sell + buy_state + sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel,
         data = mdf, index=c("coin_name"), model="within")

results[['perc']] = coeftest(fit, vcov.=function(x) vcovHC(x, method = "white1", type = "HC0", cluster = "group"))

fit = plm(recent_last_trade_nonnull ~ coin_name + factor(monitor_num)*buy + factor(monitor_num)*
             sell + buy_state + sell_state + initial_last_trade_buy +
             initial_last.percent.buy.BTC.volume + initial_total_rel,
         data = mdf, index=c("coin_name"), model="within")

results[['trade']] = coeftest(fit, vcov.=function(x) vcovHC(x, method = "white1", type = "HC0", cluster = "group"))


vars = c('buy', 'factor(monitor_num)2:buy', 'factor(monitor_num)3:buy', 'sell', 'factor(monitor_num)2:sell', 'factor(monitor_num)3:sell')
var_names = c('Buy Treat.', 'Buy Treat.*Time 2', 'Buy Treat.*Time 3', 'Sell Treat.', 'Sell Treat.*Time 2', 'Sell Treat.*Time 3')
depvars = c('buy','perc','trade')
depvar_names = c('Buy Prob.','\\% Buy Vol.','Trade Prob.')

ntests = length(vars)*length(depvars)

cat(paste("Number of Tests:", ntests,"\n"))

cat("\\begin{tabular}{|r|r|r|r|r|}\n")
cat("\\hline\n")
cat("\\textbf{Dep. Var.} & \\textbf{Independent Var.} & \\textbf{Coef.} & \\textbf{$t$-stat} & \\textbf{$p$-value}\\\\\n")
cat("\\hline\n")

for(i in 1:length(depvars)) {
    for(j in 1:length(vars)) {

        d = depvars[i]
        v = vars[j]

        r = results[[d]]
        
        b = round(r[,'Estimate'][v],3)
        t = round(r[,'t value'][v],2)
        p = format(min(ntests*r[,'Pr(>|t|)'][v],1), digits = 3, scientific  = T)
        cat(paste(depvar_names[i], "&", var_names[j], "&", b, "&", t, "&", p, "\\\\\\hline\n"))
    }
}

cat('\\end{tabular}')
