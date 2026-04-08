with source as (
  select * from {{ parquet_path('onet', 'abilities.parquet') }}
)

select * 
from source
