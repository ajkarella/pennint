



select count(*) as validation_errors
from (

    select
        id

    from "postgres"."public"."nhl_players"
    where id is not null
    group by id
    having count(*) > 1

) validation_errors

