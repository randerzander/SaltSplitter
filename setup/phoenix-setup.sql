create table if not exists words(
  word varchar not null,
  length integer not null,
  count integer,
  constraint my_pk primary key (word, length)
) salt_buckets = 5;
