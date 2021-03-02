base on es 6.8.0

this work's target is parse es query_string and use direct on pure lucene index

then can use other bigdata technology and compatibility with es,reduce es's load

## Main features

* format es query_string to orginal lucene phrase
* parse es query_string to lucene Query object
* load es range/term/terms json to Query object
* load es query json to Query object
* use on pure lucene index
* query_string/range/term/terms do well

## Important

don't supply all es Query features


## Other

need do some other job like develop plugins to work on target technology like 

hbase/hive/spark,spark streaming/flink/storm/nifi,clickhouse/druid/kylin

https://www.cnblogs.com/zihunqingxin/p/14461623.html

https://www.cnblogs.com/zihunqingxin/p/14461627.html

https://www.cnblogs.com/zihunqingxin/p/14471678.html

https://www.cnblogs.com/zihunqingxin/p/14471732.html

https://www.cnblogs.com/zihunqingxin/p/14471735.html
