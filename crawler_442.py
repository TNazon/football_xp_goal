# coding: utf-8
import requests
import unittest
from bs4 import BeautifulSoup
import time
from multiprocessing import Pool
from dateutil import parser
import itertools
import pandas as pd
import numpy as np

website_prefix = "https://www.fourfourtwo.com"
result_prefix = "https://www.fourfourtwo.com/statszone/results/"
shots_suffix = "0_SHOT_01#tabs-wrapper-anchor"

SHOOTS_CAT = {
  'head': '0_SHOT_11#tabs-wrapper-anchor',
  'right_foot': '0_SHOT_12#tabs-wrapper-anchor',
  'left_foot': '0_SHOT_13#tabs-wrapper-anchor',
  'other_parts': '0_SHOT_14#tabs-wrapper-anchor'
}


TEAM_ID = {'Borussia Dortmund': '157',
 'Borussia Mönchengladbach': '683',
 'VfB Stuttgart': '169',
 'Hannover 96': '808',
 '1. FC Nürnberg': '684',
 'SC Freiburg': '160',
 'FC Augsburg': '1772',
 'Hamburger SV': '161',
 'Eintracht Frankfurt': '159',
 'FC Bayern München': '156',
 'FC Schalke 04': '167',
 'Bayer 04 Leverkusen': '164',
 'VfL Wolfsburg': '172',
 'SV Werder Bremen': '171',
 '1899 Hoffenheim': '1902',
 '1. FSV Mainz 05': '810',
 'SpVgg Greuther Fürth': '812',
 'Fortuna Düsseldorf': '1738'}


###########################################################################################
######################################## FUNCTIONS ########################################
###########################################################################################


def _handle_request_result_and_build_soup(request_result):
  if request_result.status_code == 200:
    html_doc =  request_result.text
    soup = BeautifulSoup(html_doc,"html.parser")
    return soup


def get_info_for_matches(page_query):
  url = result_prefix + page_query
  res = requests.get(url)
  soup = _handle_request_result_and_build_soup(res)

  all_links = list(map(lambda x : website_prefix + x.attrs['href'] + "/team-stats" , soup.find_all("a", class_= "blue")))
  all_home_teams = list(map(lambda x : x.text , soup.find_all("td", class_= "home-team")))
  all_away_teams = list(map(lambda x : x.text , soup.find_all("td", class_= "away-team")))
  all_scores = list(map(lambda x : x.text , soup.find_all("td", class_= "score")))
  all_fixture_id = list(map(lambda x : x.split("/")[-2], all_links))
  df = pd.DataFrame({"link": all_links,
    "fixture_id": all_fixture_id,
    "home_team": all_home_teams,
    "away_team": all_away_teams,
    "score": all_scores})
  return df


def get_home_team_id(row):
  time.sleep(1)
  if row["home_team"] in TEAM_ID.keys():
    return TEAM_ID[row["home_team"]]
  else:
    result = 0
    while result == 0:
      res = requests.get(row["link"])
      soup = _handle_request_result_and_build_soup(res)
      if soup is not None:
        result += 1 
      else:
        print("Fail request")
    home_team_number = soup.find_all("li", {"class": "tabs-primary__tab"})[1].find("a", {"class": "active tabs-primary__tab-link"}).attrs["href"].split("/")[-2]
    TEAM_ID[row["home_team"]] = home_team_number 
    return home_team_number


def get_away_team_id(row):
  return TEAM_ID[row["away_team"]]


def get_fixture_date(row):
  time.sleep(1.5)
  result = 0
  while result == 0:
    res = requests.get(row["link"])
    soup = _handle_request_result_and_build_soup(res)
    if soup is not None:
      result += 1
  fixture_details_soup = soup.find("div", class_="teams").text
  fixture_details = fixture_details_soup.split("\n")[1].strip().split(",", 1)
  try: 
    parser.parse(fixture_details[1])
  except:
    return None 


def build_soup_for_shots(row):
  dict_shots = {}
  time.sleep(1.5)
  result = 0
  while result == 0:
    res = requests.get(row)
    soup = _handle_request_result_and_build_soup(res)
    if soup is not None:
      result += 1

  shots = soup.find_all("line", {"class" : lambda c: c and c.startswith('pitch-object')})
  return shots


def dict_builder_shot(soup_elem, team):
  shot_dict = {}

  # For a given shot - get minute of the shot
  shot_dict["minute"] = soup_elem.attrs["class"][1].split("-")[-1]

# For a given shot - get the result of the shot (color => result)
  shot_dict["result"] = soup_elem.attrs["style"].split(";")[0].split(":")[1]

# For a given shot - get coordinates
  shot_dict["x1"] = soup_elem.attrs["x1"]
  shot_dict["x2"] = soup_elem.attrs["x2"]
  shot_dict["y1"] = soup_elem.attrs["y1"]
  shot_dict["y2"] = soup_elem.attrs["y2"]
  shot_dict["shot_by"] = team

  return shot_dict


def soup_to_dict(row, team):
  return [dict_builder_shot(elem, team) for elem in row]


def complete_fixtures_df(df):
  # df["match_id"] = df["link"].apply(lambda x: x.split("/")[-2])
  df["home_team_id"] = df.apply(get_home_team_id, axis=1)
  df["away_team_id"] = df.apply(get_away_team_id, axis=1)
  df["url_shot_home"] = df["link"] + "/" + df["home_team_id"] + "/" + shots_suffix
  df["url_shot_away"] = df["link"] + "/" + df["away_team_id"] + "/" + shots_suffix
  time.sleep(1)
  print("Processing match date")
  print("...")
  df["match_date"] = df.apply(get_fixture_date, axis=1)
  print("End of processing match date")

  print("Processing shots data")
  print("...")
  df["shots_home"] = df["url_shot_home"].apply(build_soup_for_shots)
  print("End of processing shots data for home team (1/2)")
  df["shots_away"] = df["url_shot_away"].apply(build_soup_for_shots)
  print("End of processing shots data for away team (2/2)")
  
  df["shots_home_processed"] = df["shots_home"].apply(soup_to_dict, team="home")
  df["shots_away_processed"] = df["shots_away"].apply(soup_to_dict, team="away")
  df["shots_data"] = df["shots_home_processed"] + df["shots_away_processed"]
  return df


def explode_df(row, dict_col, dfs):
    json_df = pd.DataFrame(row[dict_col])
    dfs.append(json_df.assign(**row[['link', 'home_team', 'away_team', 'score','home_team_id', 'away_team_id', 'fixture_id', 'match_date']]))

###########################################################################################
######################################## FUNCTIONS ########################################
###########################################################################################


def launch_scrawling(league_id, year):
  search_string = str(league_id) + "-" + str(year)
  print(search_string)
  df_fixtures = get_info_for_matches(search_string)
  if df_fixtures.shape[0] == 0:
    return None
  else:
    print("Scrapping league {} - season {}".format(TOP5_LEAGUES[league_id], str(year)))
    df_fixtures_completed = complete_fixtures_df(df_fixtures)
    dfs = []
    df_fixtures_completed.apply(explode_df, axis=1, dict_col="shots_data", dfs = dfs)

    file_name = TOP5_LEAGUES[league_id] + "_" + str(year) + ".csv"
    pd.concat(dfs).to_csv(file_name)

######################################################################################
######################################## MAIN ########################################
######################################################################################



TOP5_LEAGUES = {22: "german_bundesliga",
                23: "spain_la_liga",
                24: "france_league_1",
                8: "england_premier_league",
                21: "italy_serie_A",
                5: "UEFA_champions_league"}

YEAR = range(2009, 2017)


for pair in itertools.product(list(TOP5_LEAGUES.keys()), YEAR):
  launch_scrawling(pair[0], pair[1])
