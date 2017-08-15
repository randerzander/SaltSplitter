-- Raw log data
create external table if not exists logs(
  line string
)
location '/user/root/logs/';


-- Enable gzip compression
set hive.exec.compress.output=true;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;

create table if not exists words(
  word string,
  length int,
  count int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
location '/user/root/words/';

-- Split logs into records: word, length, count
INSERT OVERWRITE TABLE words
select word, length(word) as length, count(*) as count from (
  select explode(split(line, ' ')) as word
  from logs
) a
where length(word) > 0
group by word;
