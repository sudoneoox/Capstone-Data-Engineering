with source as (
  select * from {{ parquet_path('onet', 'work_activities.parquet') }}
)

select * from source
