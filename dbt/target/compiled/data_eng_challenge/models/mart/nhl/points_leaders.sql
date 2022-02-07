select 
team_name,
full_name,
points -- TODO replace with correct projection columns
from "postgres"."public"."nhl_players"  -- or other tables
order by points desc
limit 10