from pyspark import SparkContext, SparkConf
from pyspark import SQLContext

import logParser
import sys

conf = SparkConf().setAppName("Log Analyzer")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
log_file = "D:\\Data\\access_log"

access_logs = (sc.textFile(log_file)
               .map(logParser.parse_log)
               .cache())
logs = sqlContext.createDataFrame(access_logs)
logs.show(truncate=False)
