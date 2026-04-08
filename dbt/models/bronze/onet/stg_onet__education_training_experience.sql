with source as (
  select * from {{ parquet_path('onet', 'education_training_experience.parquet') }}
)

select * 
from source
