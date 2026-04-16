-- fct_posting_skills: one row per posting × skill (long format)
-- Unions skills from all posting sources:
--   - arshkon LinkedIn: resolves skill_abr → full name via mapping table
--   - asaniczka large + DS: explodes comma-separated job_skills string
--   - Adzuna: no structured skills (not included here)

{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH linkedin_skills AS (
    -- arshkon dataset: skill_abr resolved to full names via mapping
    SELECT
        CAST(js.job_id AS VARCHAR)                  AS posting_id,
        LOWER(TRIM(ms.skill_name))                  AS skill_name_raw,
        'kaggle_linkedin'                           AS data_source
    FROM {{ ref("stg_kaggle_linkedin__job_skills") }} AS js
    INNER JOIN {{ ref("stg_kaggle_linkedin__mapping_skills") }} AS ms
        USING(skill_abr)
    WHERE ms.skill_name IS NOT NULL
      AND TRIM(ms.skill_name) != ''
),

large_skills AS (
    -- asaniczka 1.3M dataset: comma-separated → exploded rows
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(skill))                          AS skill_name_raw,
        'kaggle_linkedin_large'                     AS data_source
    FROM {{ ref("stg_kaggle_linkedin_large__job_skills") }},
    {{ explode_csv('job_skills') }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
),

ds_skills AS (
    -- asaniczka data science dataset: comma-separated → exploded rows
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(skill))                          AS skill_name_raw,
        'kaggle_ds'                                 AS data_source
    FROM {{ ref("stg_kaggle_ds__job_skills") }},
    {{ explode_csv('job_skills') }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
),

combined_raw AS (
    SELECT * FROM linkedin_skills
    UNION ALL
    SELECT * FROM large_skills
    UNION ALL
    SELECT * FROM ds_skills
),



-- =======================================================
-- Step 1: Apply Skill Name Normalization
-- Maps variants like problemsolving -> problem solving
-- =======================================================

normalized AS (
  SELECT
    c.posting_id,
    COALESCE(norm.canonical_skill, c.skill_name_raw) AS skill_name,
    c.data_source
  FROM combined_raw AS c
  LEFT JOIN {{ ref("skill_name_normalization") }} AS norm
    ON c.skill_name_raw = norm.raw_skill

),

-- ============================================================
-- STEP 2: Filter out non-skill entries
-- These are job requirements, legal boilerplate, benefits,
-- and sentence fragments that leak from asaniczka datasets
-- ============================================================
filtered AS (
    SELECT DISTINCT
        posting_id,
        skill_name,
        data_source
    FROM normalized
    WHERE skill_name IS NOT NULL
      AND LENGTH(skill_name) >= 2
 
      -- Max length filter: real skills are short names, not sentences
      AND LENGTH(skill_name) <= 50
 
      -- ---- LICENSE / CERTIFICATION requirements ----
      AND skill_name NOT ILIKE '%license%'
      AND skill_name NOT ILIKE '%licensure%'
      AND skill_name NOT ILIKE '%licens%'
      AND skill_name NOT ILIKE '%certification%'
      AND skill_name NOT ILIKE '%certified %'  -- trailing space: catches "certified nurse" but not "certification"
      AND skill_name NOT ILIKE '%accredit%'
      AND skill_name NOT ILIKE '%credential%'
 
      -- ---- EDUCATION / DEGREE requirements ----
      AND skill_name NOT ILIKE '%degree%'
      AND skill_name NOT ILIKE '%diploma%'
      AND skill_name NOT ILIKE '%bachelor%'
      AND skill_name NOT ILIKE '%master''s%'
      AND skill_name NOT ILIKE '%doctorate%'
      AND skill_name NOT ILIKE '%ged%'
      AND skill_name NOT ILIKE '%accredited school%'
      AND skill_name NOT ILIKE '%graduate of%'
      AND skill_name NOT ILIKE '%graduate from%'
 
      -- ---- AGE requirements ----
      AND skill_name NOT ILIKE '%years of age%'
      AND skill_name NOT ILIKE '%years old%'
      AND skill_name NOT ILIKE '%or older%'
      AND skill_name NOT ILIKE '%18+%'
      AND skill_name NOT ILIKE '%21+%'
      AND skill_name NOT ILIKE '%must be 18%'
      AND skill_name NOT ILIKE '%must be 21%'
      AND skill_name NOT ILIKE '%at least 18%'
      AND skill_name NOT ILIKE '%at least 21%'
      AND skill_name NOT ILIKE '%minimum 18%'
 
      -- ---- EMPLOYER BOILERPLATE ----
      AND skill_name NOT ILIKE '%equal opportunity%'
      AND skill_name NOT ILIKE '%affirmative action%'
      AND skill_name NOT ILIKE '%eeo/%'
      AND skill_name NOT ILIKE '%eeo %'
      AND skill_name NOT ILIKE '%reasonable accommod%'
      AND skill_name NOT ILIKE '%authorized to work%'
      AND skill_name NOT ILIKE '%eligib%to work%'
      AND skill_name NOT ILIKE '%authorization to work%'
      AND skill_name NOT ILIKE '%legal%to work%'
      AND skill_name NOT ILIKE '%disability%'
      AND skill_name NOT ILIKE '%protected veteran%'
      AND skill_name NOT ILIKE '%background check%'
      AND skill_name NOT ILIKE '%drug test%'
      AND skill_name NOT ILIKE '%drug screen%'
      AND skill_name NOT ILIKE '%security clearance%'
      AND skill_name NOT ILIKE '%u.s. citizen%'
      AND skill_name NOT ILIKE '%us citizen%'
      AND skill_name NOT ILIKE '%citizenship%'
      AND skill_name NOT ILIKE '%covid%'
      AND skill_name NOT ILIKE '%vaccination%'
 
      -- ---- BENEFITS / PERKS (not skills) ----
      AND skill_name NOT ILIKE '%insurance%'
      AND skill_name NOT ILIKE '%reimbursement%'
      AND skill_name NOT ILIKE '%paid time off%'
      AND skill_name NOT ILIKE '%401k%'
      AND skill_name NOT ILIKE '%401(k)%'
      AND skill_name NOT ILIKE '%pto%'
      AND skill_name NOT ILIKE '%vacation%'
      AND skill_name NOT ILIKE '%tuition%'
      AND skill_name NOT ILIKE '%stipend%'
      AND skill_name NOT ILIKE '%discount%'
      AND skill_name NOT ILIKE '%company paid%'
      AND skill_name NOT ILIKE '%company-paid%'
      AND skill_name NOT ILIKE '%career advance%'
      AND skill_name NOT ILIKE '%career opportun%'
      AND skill_name NOT ILIKE '%adoption%'
      AND skill_name NOT ILIKE '%fertility%'
      AND skill_name NOT ILIKE '%childcare%'
      AND skill_name NOT ILIKE '%child care%'
      AND skill_name NOT ILIKE '%parenting%'
      AND skill_name NOT ILIKE '%weight loss%'
      AND skill_name NOT ILIKE '%tobacco%'
      AND skill_name NOT ILIKE '%wellness%televis%'
 
      -- ---- WORK LOGISTICS (not skills) ----
      AND skill_name NOT ILIKE '%driver%license%'
      AND skill_name NOT ILIKE '%driving license%'
      AND skill_name NOT ILIKE '%chauffeur%'
      AND skill_name NOT ILIKE '%valid license%'
      AND skill_name NOT ILIKE '%clean driv%'
      AND skill_name NOT ILIKE '%motor vehicle%'
      AND skill_name NOT ILIKE '%able to lift%'
      AND skill_name NOT ILIKE '%pounds%'
      AND skill_name NOT ILIKE '%standing and walking%'
      AND skill_name NOT ILIKE '%weather conditions%'
      AND skill_name NOT ILIKE '%climbing up to%'
      AND skill_name NOT ILIKE '%toxic%caustic%'
      AND skill_name NOT ILIKE '%warehouse environment%'
 
      -- ---- GENERIC ATTRIBUTES (not skills) ----
      AND skill_name NOT ILIKE '%"can do"%'
      AND skill_name NOT ILIKE '%can do%attributes%'
      AND skill_name NOT ILIKE '%can-do attitude%'
      AND skill_name NOT ILIKE '%hard worker%'
      AND skill_name NOT ILIKE '%fast learner%'
      AND skill_name NOT ILIKE '%go-getter%'
      AND skill_name NOT ILIKE '%self-starter%'
      AND skill_name NOT ILIKE '%self starter%'
      AND skill_name NOT ILIKE '%detail-oriented%'
      AND skill_name NOT ILIKE '%detail oriented%'
 
      -- ---- POLICY / PROCEDURE boilerplate ----
      AND skill_name NOT ILIKE '%company policies%'
      AND skill_name NOT ILIKE '%policies and procedures%'
      AND skill_name NOT ILIKE '%labor laws%'
      AND skill_name NOT ILIKE '%legal requirements%'
 
      -- ---- COMPOUND PHRASES that are requirements, not skills ----
      AND skill_name NOT ILIKE '%excellent %and%'  -- "excellent written and verbal..."
      AND skill_name NOT ILIKE '%strong %and%'     -- "strong written and verbal..."
      AND skill_name NOT ILIKE '%effective %and%'  -- "effective oral and written..."
      AND skill_name NOT ILIKE '%good %and%'       -- "good verbal and written..."
      AND skill_name NOT ILIKE '%proficiency in %and%'
      AND skill_name NOT ILIKE '%proof of%'
      AND skill_name NOT ILIKE '%exposure to%'
 
      -- ---- RETAIL/SERVICE operational tasks ----
      AND skill_name NOT ILIKE '%planogram%'
      AND skill_name NOT ILIKE '%cash register%'
      AND skill_name NOT ILIKE '%flatbed scanner%'
      AND skill_name NOT ILIKE '%merchandise presentation%'
      AND skill_name NOT ILIKE '%countertop%'
      AND skill_name NOT ILIKE '%restocking%'
      AND skill_name NOT ILIKE '%restock%'
      AND skill_name NOT ILIKE '%refunds and overrides%'
      AND skill_name NOT ILIKE '%drawer pulls%'
      AND skill_name NOT ILIKE '%cartons and totes%'
      AND skill_name NOT ILIKE '%collect payment%'
      AND skill_name NOT ILIKE '%collecting payment%'
      AND skill_name NOT ILIKE '%itemizing and totaling%'
      AND skill_name NOT ILIKE '%monitoring cameras%'
      AND skill_name NOT ILIKE '%secure shopping%'
      AND skill_name NOT ILIKE '%secure working%'
      AND skill_name NOT ILIKE '%customer service leadership%'
 
      -- ---- EMPLOYER-SPECIFIC boilerplate ----
      AND skill_name NOT ILIKE '%archways to opportunity%'
      AND skill_name NOT ILIKE '%weatherby healthcare%'
      AND skill_name NOT ILIKE '%mayo clinic%'
      AND skill_name NOT ILIKE '%women owned%'
      AND skill_name NOT ILIKE '%irs reassignment%'
 
      -- ---- MILITARY-SPECIFIC ----
      AND skill_name NOT ILIKE '%canadian forces%'
      AND skill_name NOT ILIKE '%military officer%'
 
      -- ---- EARLY YEARS (UK childcare sector noise) ----
      AND skill_name NOT ILIKE '%early years%'
 
      -- ---- STAKEHOLDER (borderline — remove the noisy variants) ----
      AND skill_name NOT ILIKE 'stakeholder %'  -- "stakeholder alignment", "stakeholder identification", etc
      AND skill_name NOT ILIKE '%stakeholder relationship%'
      AND skill_name NOT ILIKE '%stakeholder communication%'
      AND skill_name NOT ILIKE '%stakeholder collaboration%'
      AND skill_name NOT ILIKE '%stakeholder coordination%'
      AND skill_name NOT ILIKE '%stakeholder liaison%'
      AND skill_name NOT ILIKE '%stakeholder influence%'
      AND skill_name NOT ILIKE '%stakeholder analysis%'
      AND skill_name NOT ILIKE '%stakeholder interviews%'
      AND skill_name NOT ILIKE '%stakeholder consultation%'
      AND skill_name NOT ILIKE '%stakeholder alignment%'
      AND skill_name NOT ILIKE '%stakeholder identification%'
      AND skill_name NOT IN ('stakeholder', 'stakeholders',
                              'internal stakeholders', 'external stakeholders',
                              'key stakeholders', 'business stakeholders',
                              'project stakeholders', 'shareholder',
                              'shareholder opportunity', 'shareholder eligibility',
                              'shareholder track', 'shareholder opportunities')
 
      -- Keep legitimate stakeholder skills:
      -- "stakeholder management" and "stakeholder engagement" survive
)
 
SELECT *
FROM filtered
 
