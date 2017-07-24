create table if not exists example(
  keya varchar not null,
  keyb varchar not null,
  blank integer,
  constraint my_pk primary key (keya, keyb)
) salt_buckets = 5;
