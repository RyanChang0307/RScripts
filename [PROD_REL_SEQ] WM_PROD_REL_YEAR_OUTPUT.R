library(sparklyr)
library(dplyr)
library(DBI)
library(arulesSequences)


#connection
Sys.setenv(SPARK_HOME="/opt/cloudera/parcels/SPARK2/lib/spark2")
Sys.setenv(HADOOP_CONF_DIR='/etc/hadoop/conf.cloudera.hdfs')
Sys.setenv(YARN_CONF_DIR='/etc/hadoop/conf.cloudera.yarn')
Sys.setenv(JAVA_HOME = '/usr/java/jdk1.7.0_67-cloudera')
Sys.setenv(SPARK_CONF_DIR = '/etc/spark2/conf')
config <- spark_config()
config$sparklyr.shell.files <- "/etc/spark2/conf/hive-site.xml"
config$spark.executor.memory<-"4G"
config$spark.driver.memory<-"4G"
config$spark.yarn.executor.memoryOverhead<-"1G"
config$spark.kryoserializer.buffer.max<-'512m'
config[["sparklyr.shell.conf"]]<-"spark.driver.extraJavaOptions=-XX:MaxHeapSize=1G"
sc <- spark_connect(master = "yarn-cluster", config = config, version = '2.1.0')


#更改科學符號設定
options(scipen=999)
prod_rel_input <- dbGetQuery(sc, "SELECT * FROM PTEMP.WM_PROD_REL_YEAR_INPUT ")


#重新排序
ToSeqData <- function(prod_data)
{
  prod_real_arrange <- prod_data %>%
    arrange(party_id) %>%
    unique()
  write.table(prod_real_arrange, "tempResult.txt", sep=" ", row.names = FALSE, col.names = FALSE, quote = FALSE)
  return(read_baskets("tempResult.txt", info = c("sequenceID","eventID","SIZE")))
}


#arulesSequences
AnalysisSeqData <- function(Analysis_data,sup,conf)
{
  arulSeq_analysis <-cspade(Analysis_data,parameter = list(support= sup,maxlen=2), control = list(verbose = TRUE))
  arulSeq_ruleIn <-ruleInduction(arulSeq_analysis,confidence=conf,control=list(verbose=TRUE))
  Analysis_Result_data <- subset(as(arulSeq_ruleIn,"data.frame"))
  #符號取代
  Analysis_Result_data$rule <- gsub("=>","*",Analysis_Result_data$rule)
  Analysis_Result_data$rule <- gsub("[=|<|>\\^_`{|}~]","",Analysis_Result_data$rule)
  #只取MAPPING ONE ON ONE
  Analysis_Result_data <- Analysis_Result_data[count.fields(textConnection(Analysis_Result_data$rule),sep = ",")<=1,]
  #split_result
  #Result_Split <- data.frame(do.call('rbind',strsplit(as.character(Analysis_Result_data$rule),'*',fixed = TRUE)),Analysis_Result_data$support,(Analysis_Result_data$support*length(unique(prod_rel_input$party_id))),Analysis_Result_data$confidence,Analysis_Result_data$lift,as.factor(Sys.time()))
  #colnames(Result_Split) <- c("Product1","Product2","Support","frequecy","Confidence","Lift","Data_Date") 
  Result_Split <- data.frame(do.call('rbind',strsplit(as.character(Analysis_Result_data$rule),'*',fixed = TRUE)),Analysis_Result_data$support,(Analysis_Result_data$support*length(unique(prod_rel_input$party_id))),Analysis_Result_data$confidence,Analysis_Result_data$lift,'12')
  colnames(Result_Split) <- c("Product1","Product2","Support","frequecy","Confidence","Lift","DATA_PERIOD") 
  return(Result_Split)
}


round_df <- function(x, digits) {
  numeric_columns <- sapply(x, mode) == 'numeric'
  x[numeric_columns] <-  round(x[numeric_columns], digits)
  x
}




#資料寫回hadoop
ResultToHadoop <- function(output)
{
  # dbGetQuery(sc, "drop table DTEMP.prod_rel_wm_seq_cim_prod_output")
  # dbGetQuery(sc, "CREATE TABLE DTEMP.prod_rel_wm_seq_cim_prod_output
  # (
  #   Product1 VARCHAR,
  #   Product2 VARCHAR,
  #   Support FLOAT,
  #   Frequency INT,
  #   Confidence FLOAT,
  #   Lift FLOAT
  # )")
  dbWriteTable(sc, name='PMART_R.WM_PROD_REL_YEAR_OUTPUT',output,append = TRUE)
}

seq_anai_data <- ToSeqData(prod_rel_input)
seq_result_data <- AnalysisSeqData(seq_anai_data,0.001,0)
ResultToHadoop(seq_result_data)
spark_disconnect(sc)


