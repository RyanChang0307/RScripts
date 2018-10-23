Sys.setenv(LD_LIBRARY_PATH='$LD_LIBRARY_PATH:/usr/lib64/unixODBC:/opt/cloudera/hiveodbc/lib/64/')
Sys.setenv(ODBCINI='/etc/unixODBC/odbc.ini')
Sys.setenv(ODBCSYSINI='/etc/unixODBC/odbcinst.ini')
library(odbc)
library(implyr)
library(dplyr)
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = "/opt/cloudera/impalaodbc/lib/64/libclouderaimpalaodbc64.so",
  host = "172.16.244.149",
  port = 21050,
  database = "default"
)
src_tbls(impala)
orders_tbl<-dbGetQuery(
  impala,
  "SELECT * FROM orders limit 10"
)
