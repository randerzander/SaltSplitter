select 
  min(key),
  max(key),
  count(*) as count,
  bucket
from (
  select concat_ws("\0", keya, keyb) as key, ntile(5) over (cluster by keya, keyb) as bucket
  from example
) a
group by bucket
