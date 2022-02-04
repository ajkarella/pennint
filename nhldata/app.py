'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
import sys #added to allow graceful exit
import json #added to write json and see necc xforms
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    # DONE what propertie are needed to partition?
    #just the gameid, since all data lives in the single csv
    gameid: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # DONE use the properties to return the s3 key
        return f'{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
            # NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
	        # so here we are looking for your ability to gently massage a data set.
        # DONE error handling
        # err handling added for missing skaterStats for non goalies (i.e nones)
        # DONE get games for dates
        sked = self.api.schedule(startDate, endDate)

        # saving to a list to simplify & verify functionality
        # in a prod setting, it would be faster to run directly from loop
        # or find better way to parse larger json
        gameIDs = []
        for i in sked["dates"]:
            for i2 in i["games"]:
                gameIDs.append(i2["gamePk"])

        #DONE for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: player_object (see boxscore above)
        for game in gameIDs:
            gameid = game
            game = self.api.boxscore(gameid)

            # get away team
            stats = []
            awayTeam = game["teams"]["away"]["team"]["name"]
            awayplayers = dict(game["teams"]["away"]["players"])
            for i in awayplayers.items():
                if str(i[1]["stats"].keys()) == "dict_keys(['skaterStats'])":
                    name = i[1]["person"]["fullName"]
                    id = i[1]["person"]["id"]
                    goals = i[1]["stats"]["skaterStats"]["goals"]
                    assists = i[1]["stats"]["skaterStats"]["assists"]
                    team = awayTeam
                    side = "away"
                    stats.append([id,team,name,assists,goals,side])

            # get home team
            homeTeam = game["teams"]["home"]["team"]["name"]
            homeplayers = dict(game["teams"]["home"]["players"])
            for i in homeplayers.items():
                if str(i[1]["stats"].keys()) == "dict_keys(['skaterStats'])":
                    name = i[1]["person"]["fullName"]
                    id = i[1]["person"]["id"]
                    goals = i[1]["stats"]["skaterStats"]["goals"]
                    assists = i[1]["stats"]["skaterStats"]["assists"]
                    team = homeTeam
                    side = "home"
                    stats.append([id, team, name, assists, goals, side])

            # creates df out of list of lists
            df = pd.DataFrame(stats, columns=['player_person_id', 'player_person_currentTeam_name',
                                              'player_person_fullName','player_stats_skaterStats_assists',
                                              'player_stats_skaterStats_goals','side'])

            csv = df.to_csv(index=False)
            #df.to_csv(str(gameid)+'.csv',index=False) # lets me grab CSVs as files since minio not functional
            key = StorageKey(str(gameid))
            self.storage.store_game(key,csv)
            #DONE ignore goalies (players with "goalieStats")
            #DONE output to S3 should be a csv that matches the schema of utils/create_games_stats
                 
def main():
    import os
    import argparse
    try:
        parser = argparse.ArgumentParser(description='NHL Stats crawler')
        # DONE what arguments are needed to make this thing run,  if any?
        # get arg startDate, endDate
        parser.add_argument('--start', type=str, default="jan 5 2022")
        parser.add_argument('--end', type=str, default="jan 7 2022")
        args = parser.parse_args()
        start = args.start
        end = args.end
    except:
        start = "jan 5 2022"
        end = "jan 7 2022"
        #sys.exit('encapsulate dates in double quotes, ex: "jan 12 2022"')

    dest_bucket = 'data-bucket'
    startDate = dateparse(start) # DONE get this however but should be like datetime(2020,8,4)
    endDate = dateparse(end)  # DONE get this however but should be like datetime(2020,8,5)
    api = NHLApi()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url='http://s3:9000')
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)

    crawler.crawl(startDate, endDate)

if __name__ == '__main__':
    main()
