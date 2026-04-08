with source as (
  select * from {{ parquet_path('acs', 'acs_metro_profiles.parquet') }}
)

select *
from source
