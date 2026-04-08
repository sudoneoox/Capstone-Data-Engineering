with source as (
  select * from {{ parquet_path('fred', 'fred_series.parquet') }}
)

select *
from source
