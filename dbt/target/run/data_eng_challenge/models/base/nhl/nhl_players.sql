
  create view "postgres"."public"."nhl_players__dbt_tmp" as (
    select
  nhl_player_id as id,
  full_name,
  game_team_name as team_name,
  sum(stats_assists) as assists,
  sum(stats_goals) as goals,
  (sum(stats_goals) + sum(stats_assists)) as points -- TODO replace this with correct columns
from "postgres"."public"."player_game_stats" --or whatever other table reference
group by id, full_name, game_team_name
  );
