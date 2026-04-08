with source as (
  select * from {{ parquet_path('onet', 'occupation_data.parquet') }}
)

select *
from source
