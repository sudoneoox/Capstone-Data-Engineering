with source as (
  select * from {{ parquet_path('onet', 'alternate_titles.parquet') }}
)

select *
from source
