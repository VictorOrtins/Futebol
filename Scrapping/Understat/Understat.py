import json
import requests

import pandas as pd

from bs4 import BeautifulSoup


class UnderstatScrapper:
    def __init__(self):
        self.base_url_match = "https://understat.com/match/"
        self.base_url_player = "https://understat.com/player/"

    def scrape_player(self, player: int | str) -> tuple[tuple[pd.DataFrame, list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame]], pd.DataFrame, pd.DataFrame]:
        scrape_url = self._get_scrape_url(player, self.base_url_player)

        res = requests.get(scrape_url)
        soup = BeautifulSoup(res.content)
        scripts = soup.find_all('script')

        groups_string = scripts[1].string
        player_stats_string = scripts[2].string
        shots_data_string = scripts[3].string

        player_groups_tuple = self._get_player_groups_df(groups_string)
        player_positions_df = self._get_player_positions_df(player_stats_string)

        shots_data_json = self._get_json_data(shots_data_string)
        shots_df = pd.DataFrame(shots_data_json)

        return player_groups_tuple, player_positions_df, shots_df


    def scrape_team(self, url: str) -> list[pd.DataFrame]:
        res = requests.get(url)
        soup = BeautifulSoup(res.content)
        scripts = soup.find_all('script')

        games_string = scripts[1].string
        statistics_string = scripts[2].string
        players_string = scripts[3].string

        games_json = self._get_json_data(games_string)
        statistics_json = self._get_json_data(statistics_string)
        players_json = self._get_json_data(players_string)

        games_df = pd.DataFrame(games_json)
        games_df = self._transform_games_data_df(games_df)

        players_list_df = self._transform_statistics_json(statistics_json)

        players_df = pd.DataFrame(players_json)

        final_list = [games_df, players_df]
        final_list.extend(players_list_df)

        return final_list


    def scrape_match(self, match: int | str) -> tuple[list[pd.DataFrame], str, str]:
        scrape_url = self._get_scrape_url(match, self.base_url_match)

        res = requests.get(scrape_url)
        soup = BeautifulSoup(res.content)
        scripts = soup.find_all("script")

        shots_data = scripts[1].string
        roster_data = scripts[2].string
        teams_data = scripts[1].string

        shots_df = self._get_shots_df(shots_data)
        roster_df = self._get_roster_stats_df(roster_data)
        teams_df = self._get_teams_stats_df(teams_data)

        home_team = shots_df['h_team'].iloc[0]
        away_team = shots_df['a_team'].iloc[0]

        return ([shots_df, roster_df, teams_df], home_team, away_team)
    



    def _get_shots_df(self, shots_data: str) -> pd.DataFrame:
        shots_json = self._get_json_data(shots_data)

        shots_df = pd.concat(
            [
                pd.DataFrame(shots_json["h"]),
                pd.DataFrame(shots_json["a"]),
            ]
        )

        shots_df['minute'] = shots_df['minute'].astype(int)
        shots_df.sort_values(by=['minute'], inplace=True)

        return shots_df

    def _get_roster_stats_df(self, roster_data: str) -> pd.DataFrame:
        json_data = self._get_json_data(roster_data)

        json_data_home = []
        for _, item in json_data['h'].items():
            json_data_home.append(item)

        json_data_away = []
        for _, item in json_data['a'].items():
            json_data_away.append(item)

        home_data = pd.DataFrame(json_data_home)
        away_data = pd.DataFrame(json_data_away)

        roster_df = pd.concat([home_data, away_data])

        return roster_df
    
    def _get_teams_stats_df(self, team_data: str) -> pd.DataFrame:
        match_info_index = team_data.index("match_info")
        str_start = team_data.index("('", match_info_index) + 2
        str_end = team_data.index("')", match_info_index)
        json_data = team_data[str_start:str_end]
        json_data = json_data.encode('utf-8').decode('unicode_escape')

        json_data = json.loads(json_data)

        team_stats_df = pd.DataFrame([json_data])

        return team_stats_df
    
    def _get_player_groups_df(self, groups_string: str) -> tuple[pd.DataFrame, list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame]]:
        json_data = self._get_json_data(groups_string)

        season_df = pd.DataFrame(json_data['season'])

        positions_list = []
        for key, item in json_data['position'].items():
            positions_list.append((key, pd.DataFrame(item).transpose()))

        situations_list = []
        for key, item in json_data['situation'].items():
            situations_list.append((key, pd.DataFrame(item).transpose()))

        shotZoness_list = []
        for key, item in json_data['shotZones'].items():
            shotZoness_list.append((key, pd.DataFrame(item).transpose()))

        shotTypess_list = []
        for key, item in json_data['shotTypes'].items():
            shotTypess_list.append((key, pd.DataFrame(item).transpose()))

        return (season_df, positions_list, situations_list, shotZoness_list, shotTypess_list)
    
    def _get_player_positions_df(self, player_stats_string: str) -> pd.DataFrame:
        json_data = self._get_json_data(player_stats_string)

        new_data = {}
        for position, position_dict in json_data.items():
            if position not in new_data:
                new_data[position] = {}
            for stat, stats in position_dict.items():
                for math, stat_value in stats.items():
                    new_data[f'{position}'][f'{stat}_{math}'] = stat_value

        return pd.DataFrame(new_data).transpose()
    
    def _transform_games_data_df(self, games_data_df: pd.DataFrame) -> pd.DataFrame:
        games_data_df['home_team_id'] = games_data_df['h'].apply(lambda x: x['id'])
        games_data_df['home_team_name'] = games_data_df['h'].apply(lambda x: x['title'])
        games_data_df['home_team_short_name'] = games_data_df['h'].apply(lambda x: x['short_title'])

        games_data_df['away_team_id'] = games_data_df['a'].apply(lambda x: x['id'])
        games_data_df['away_team_name'] = games_data_df['a'].apply(lambda x: x['title'])
        games_data_df['away_team_short_name'] = games_data_df['a'].apply(lambda x: x['short_title'])

        games_data_df['home_goals'] = games_data_df['goals'].apply(lambda x: x['h'])
        games_data_df['away_goals'] = games_data_df['goals'].apply(lambda x: x['a'])

        games_data_df['home_xG'] = games_data_df['xG'].apply(lambda x: x['h'])
        games_data_df['away_xG'] = games_data_df['xG'].apply(lambda x: x['a'])

        games_data_df['forecast_win'] = games_data_df['forecast'].apply(lambda x: x['w'])
        games_data_df['forecast_draw'] = games_data_df['forecast'].apply(lambda x: x['d'])
        games_data_df['forecast_loss'] = games_data_df['forecast'].apply(lambda x: x['l'])

        games_data_df.drop(columns=['h', 'a', 'goals', 'xG','forecast'], inplace=True)

        return games_data_df
    
    def _transform_statistics_json(self, statistics_json: dict) -> list[pd.DataFrame]:
        df_list = []
        for _, item_external in statistics_json.items():
            for _, item in item_external.items():
                item['shots_against'] = item['against']['shots']
                item['goals_against'] = item['against']['goals']
                item['xG_against'] = item['against']['xG']
                item.pop('against')

            df_list.append(pd.DataFrame(item_external).transpose())

        return df_list
    
    def _get_json_data(self, data: str) -> dict:
        str_start = data.index("('") + 2
        str_end = data.index("')")
        json_data = data[str_start:str_end]
        json_data = json_data.encode('utf-8').decode('unicode_escape')

        json_data = json.loads(json_data)

        return json_data
    
    def _get_scrape_url(self, match, base_url) -> str:
        scrape_url = match

        if isinstance(match, int):
            match_id = f"{match}"
            scrape_url = f"{base_url}{match_id}"
        elif not isinstance(match, str):
            raise ValueError(
                "O argumento match precisa ser um int (match_id) ou uma string (url da partida)"
            )
        
        return scrape_url
