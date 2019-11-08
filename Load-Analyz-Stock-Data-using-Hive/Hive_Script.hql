
Load And Analyze Stock data into hive 

1- Create Table for the sotck data 

create table nyse (exc String,stk_smbl String,st_date String,st_open_price double, st_high_price double, 
st_low_price double,
 st_close_price double, stock_volume double, stock_price_adj_close double) row format delimited fields terminated by ",";
 
 2- Load The Data from local Os into Hive 
 
 load data local inpath '/home/edureka/NYSE_daily_prices_Q.csv' into table nyse;
 
 3- Calculate the Covariance 
 
 select a.stk_smbl, b.stk_smbl, month(a.st_date), 
 (AVG(a.st_high_price*b.st_high_price) - (AVG(a.st_high_price)*AVG(b.st_high_price)))
 from nyse a join nyse b on a.st_date=b.st_date group by  a.stk_smbl,b.stk_smbl, month(a.st_date);
