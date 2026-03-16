{{

config

({

"materialized":'table',

"schema": 'MART'

})

}}

 

WITH country_details_antarctica AS

(

SELECT

*

FROM

{{ ref('country_details_transform') }}

WHERE UPPER(COUNTRY_CONTINENT_NAME) = 'ANTARCTICA'

)

SELECT

*

FROM country_details_antarctica
