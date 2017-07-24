create external table if not exists example(
  keya string,
  keyb string,
  val int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
location '/user/root/example/';
