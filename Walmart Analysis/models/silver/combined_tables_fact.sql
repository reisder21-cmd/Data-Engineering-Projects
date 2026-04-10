-- joining stores and dept to fact to get all 3 keys to use for final snapshot SCD2

SELECT
    d.STORE_ID,
    d.DEPT_ID,
    d.DATE_ID,

    -- Measures
    d.STORE_WEEKLY_SALES,

    s.TEMPERATURE,
    s.FUEL_PRICE,
    s.CPI,
    s.UNEMPLOYMENT,
    s.MARKDOWN1,
    s.MARKDOWN2,
    s.MARKDOWN3,
    s.MARKDOWN4,
    s.MARKDOWN5,

    st.STORE_SIZE

FROM {{ ref('stage_dept') }} d

LEFT JOIN {{ ref('stg_fact') }} s
    ON d.STORE_ID = s.STORE_ID
    AND d.DATE_ID = s.DATE_ID
LEFT JOIN {{ ref('stage_store') }} st
    ON d.STORE_ID = st.STORE_ID