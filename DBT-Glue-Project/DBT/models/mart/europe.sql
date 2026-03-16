{{

config

({

"materialized":'table',

"schema": 'MART'

})

}}

 

WITH country_details_europe AS

(

SELECT

*

FROM

{{ ref('country_details_transform') }}

WHERE UPPER(COUNTRY_CONTINENT_NAME) = 'EUROPE'

)

 

SELECT

*

FROM country_details_europe
