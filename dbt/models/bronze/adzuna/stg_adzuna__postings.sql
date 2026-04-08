with source as (
  select * from {{ parquet_path('adzuna', 'adzuna_*.parquet') }}
)

select *
from source
