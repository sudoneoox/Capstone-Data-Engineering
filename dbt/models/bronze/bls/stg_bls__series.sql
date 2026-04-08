with source as (
  select * from {{ parquet_path('bls', 'bls_2023_2026.parquet') }}
)

select *
from source
