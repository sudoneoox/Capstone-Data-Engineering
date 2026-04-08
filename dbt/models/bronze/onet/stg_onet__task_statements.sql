with source as (
  select * from {{ parquet_path('onet', 'task_statements.parquet') }}
)

select *
from source
