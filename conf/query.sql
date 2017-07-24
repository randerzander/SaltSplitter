select 
  min(keya),
  max(keya),
  count(*) as count,
  bucket
from (
  select keya, ntile(5) over (order by keya, keyb) as bucket
  from example
) a
group by bucket
