Apache Phoenix's [salted tables](http://phoenix.apache.org/salted.html) feature saves dev cycles: they pre-split your tables and distribute your data without forcing you to think about hotspotting.

However, using too many buckets to achieve maximal parallelism has drawbacks at read-time, particularly for aggregation or range-scan queries for which locality is more valuable than parallelism. Additionally, tooling for salt bucket management is limited to a one time setting at table creation time, and only the Phoenix client libraries are salt aware.

This presents a challenge for speedily populating large salted tables: we want maximal write parallelism without incurring read penalties.

Since HBase's bulk load tools can run only one reducer per region in the target table, the usual approach is to temporarily increase increase the region count by scripting splits through the HBase shell. After load, regions are compacted down to a more optimal number. The added complexity of binary salt bits makes getting the splitpoints right difficult.

This repository is intended to supply a tool that automates analysis of source (Hive) data to pre-split a salted Phoenix table in prep for a [bulk load](http://phoenix.apache.org/bulk_dataload.html).

The tool does the following at runtime:
1. Runs a Hive query to compute even "buckets" of values for a column which should match the "leftmost" field in the target Phoenix table's primary key
2. Iterate through query results to generate an array of splitpoints
3. Issue the HBase admin commands to split the table at those splitpoints

Building:
```
mvn clean package
```

Usage:
```
java -jar target/SaltSpliter-0.0.1-SNAPSHOT.jar conf/example.props
```

Setup:
```
# Create source 'example' Hive table
hive -f setup/hive-ddl.sql

# Put data in the source table
hdfs dfs -mkdir /user/root/example
hdfs dfs -put /var/log/ambari-agent/ambari-agent.log /user/root/example/

# Create the 'example' Phoenix table
/usr/hdp/current/phoenix-client/bin/psql.py setup/phoenix-ddl.sql
```

Configuration is exposed in the conf directory. `example.props` 

Make sure to set the correct number of salt buckets in `conf/example.props`.

The argument to the ntile UDF in `conf/query.sql` specifies how many additional region splits to execute.

Run the Bulk Load:
```
HADOOP_CLASSPATH=/etc/hbase/conf hadoop jar /usr/hdp/current/phoenix-client/phoenix-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table EXAMPLE --input /user/root/example/
```
