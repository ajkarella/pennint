



select count(*) as validation_errors
from "postgres"."public"."nhl_players"
where full_name is null

