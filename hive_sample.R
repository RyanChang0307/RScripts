library(odbc)
library(implyr)
library(dplyr)
drv <- odbc::odbc()
hive_con<- dbConnect(drv,
                     driver = "/opt/cloudera/hiveodbc/lib/64/libclouderahiveodbc64.so", server = "172.16.244.161", database = "default", uid = "cloudera",
                     pwd = cloudera)


con <- DBI::dbConnect(odbc::odbc(),
                      driver = "/opt/cloudera/hiveodbc/lib/64/libclouderahiveodbc64.so",
                      database = "default",
                      UID    = "cloudera",
                      PWD    = "cloudera",
                      host = "172.16.244.161",
                      port = 10000)