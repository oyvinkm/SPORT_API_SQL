import http.client
import json
import os
from dotenv import load_dotenv
from process import (proccess_req, insert_tour, 
                    insert_games, insert_teams)



def http_req(headers = None):
    load_dotenv()
    if headers is None:
        headers = {
        'X-RapidAPI-Key': os.getenv('RAPIDAPI_KEY'),
        'X-RapidAPI-Host': "sportapi7.p.rapidapi.com"
        }
    print(headers)
    conn = http.client.HTTPSConnection("sportapi7.p.rapidapi.com")


    conn.request("GET", "/api/v1/sport/football/events/live", headers=headers)
    res = conn.getresponse()
    data = res.read()
    json_data =  json.loads(data)
    return json_data

def proccess_post_api_data(cursor, local = False):
    if local:
        with open('process/req.json', 'r') as f:
            json_data = json.loads(f)
    else:
        json_data = http_req()
    games_df, tour_df, teams_df = proccess_req(json_data)
    insert_tour(cursor=cursor, tour_df=tour_df)
    insert_games(cursor=cursor, games_df=games_df)
    insert_teams(cursor, teams_df=teams_df)

