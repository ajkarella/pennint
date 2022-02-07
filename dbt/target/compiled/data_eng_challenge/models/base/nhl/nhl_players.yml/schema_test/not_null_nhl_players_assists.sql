



select count(*) as validation_errors
from "postgres"."public"."nhl_players"
where assists is null

