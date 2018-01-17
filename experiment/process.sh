# sh process.sh ../out/ epsilon-5en7-complete-separate-volume ../processed/

out1=$3/$2-checks.csv
out2=$3/$2-interventions.csv

echo coin_name,coin_id,intervention_num,monitor_num,buy_state,sell_state,buy_eats_block,sell_eats_block,condition,success,time,volume_15,volume_30,volume_60,total_15,total_30,total_60,volume,total,buy_volume_15,buy_volume_30,buy_volume_60,buy_total_15,buy_total_30,buy_total_60,buy_volume,buy_total,sell_volume_15,sell_volume_30,sell_volume_60,sell_total_15,sell_total_30,sell_total_60,sell_volume,sell_total,last_trade_type,last_trade_id,last_trade_time,last_trade_quantity,last_trade_price,last_trade_total,highest_buy_price,highest_buy_total,highest_buy_quantity,lowest_sell_price,lowest_sell_total,lowest_sell_quantity > $out1

cat $1/$2/*/checks.csv >> $out1

echo coin_name,coin_id,intervention_num,buy_state,sell_state,buy_eats_block,sell_eats_block,condition,success,start_time,end_time,wait,last_trade_type,last_trade_id,last_trade_time,last_trade_quantity,last_trade_price,last_trade_total,this_order_id,this_trade_id,this_trade_time,this_trade_quantity,this_trade_price,this_trade_total > $out2

cat $1/$2/*/interventions.csv >> $out2
