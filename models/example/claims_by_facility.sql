SELECT
    ca.dim_patient_id AS patient_id,
    ca.dim_patient_name AS patient_name,
    ca.dim_date_of_birth AS date_of_birth,
    ca.dim_provider_id AS provider_id,
    ca.dim_provider_name AS provider_name,
    ca.dim_specialty AS specialty,
    ca.fct_claim_id AS claim_id,
    ca.fct_claim_date AS claim_date,
    ca.fct_claim_amount AS claim_amount,
    ca.fct_total_claim_amount AS total_claim_amount,
    ca.fct_avg_claim_amount AS avg_claim_amount,
    ca.fct_claim_count AS claim_count,
    hf.Hospital_Name,
    hf.Hospital_Location,
    hf.Facility_Type,
    hf.Bed_Count
FROM
    {{ ref('abc_company', 'claims_analytics') }} ca
LEFT JOIN
    {{ ref('facilities') }}
ON
    ca.dim_provider_id = hf.Provider_ID;
