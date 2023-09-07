SELECT
    ca.patient_id AS patient_id,
    ca.patient_name AS patient_name,
    ca.date_of_birth AS date_of_birth,
    ca.provider_id AS provider_id,
    ca.provider_name AS provider_name,
    ca.specialty AS specialty,
    ca.claim_id AS claim_id,
    ca.claim_date AS claim_date,
    ca.claim_amount AS claim_amount,
    ca.total_claim_amount AS total_claim_amount,
    ca.avg_claim_amount AS avg_claim_amount,
    ca.claim_count AS claim_count,
    hf.Hospital_Name,
    hf.Hospital_Location,
    hf.Facility_Type,
    hf.Bed_Count
FROM
    {{ ref('abc_company', 'claims_analytics') }} ca
LEFT JOIN
    {{ ref('facilities') }} hf
ON
    ca.provider_id = hf.Provider_ID
