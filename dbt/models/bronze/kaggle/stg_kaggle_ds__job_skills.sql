with source as (
  select * from {{ parquet_path('kaggle/data_science_jobs', 'ds_job_skills.parquet') }}
)

select *
from source
