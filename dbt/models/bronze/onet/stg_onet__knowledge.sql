with source as (
  select * from {{ parquet_path('onet', 'knowledge.parquet') }}
)

select *
from source
