



select count(*)
from (
    select
      points    

    from "postgres"."public"."points_leaders"

    where points <= 0

) validation_errors

