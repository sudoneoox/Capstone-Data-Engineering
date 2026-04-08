with source as (
 select * from {{ parquet_path('onet', 'skills.parquet') }}
)

select *
from source
