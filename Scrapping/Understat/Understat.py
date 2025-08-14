import json
import requests

import pandas as pd

from bs4 import BeautifulSoup
from tqdm import tqdm


class UnderstatScrapper:
    """A class for scraping football statistics from Understat.com.

    This scraper fetches and parses data for leagues, players, teams, and
    matches by extracting JSON data embedded within the site's <script> tags.

    Attributes:
        base_url_match (str): The base URL for match-specific pages.
        base_url_player (str): The base URL for player-specific pages.
        base_url_league (str): The base URL for league-specific pages.
    """
    def __init__(self):
        """Initializes the UnderstatScrapper and its base URLs."""
        self.base_url_match = "https://understat.com/match/"
        self.base_url_player = "https://understat.com/player/"
        self.base_url_league = "https://understat.com/league/"

        self.leagues = {
            "Serie A": "https://understat.com/league/Serie_A/",
            "Premier League": "https://understat.com/league/EPL/",
            "La Liga": "https://understat.com/league/La_Liga/",
            "Bundesliga": "https://understat.com/league/Bundesliga/",
            "Ligue 1": "https://understat.com/league/Ligue_1/",
            "Russian Premier League": "https://understat.com/league/RFPL/",
        }

    def scrape_league(self, league: str) -> tuple[pd.DataFrame, list[pd.DataFrame], pd.DataFrame]:
        """Scrapes comprehensive data for an entire league season.

        Args:
            league (str): The name of the league and season (e.g., 'EPL/2023')
                          or a full URL to a league page.

        Returns:
            tuple[pd.DataFrame, list[pd.DataFrame], pd.DataFrame]: A tuple containing:
                1. A DataFrame of all matches played in the season.
                2. A list of DataFrames, each with a single team's match history.
                3. A DataFrame of aggregated season stats for all players.
        """
        if 'https' in league:
            scrape_url = league
        else:
            scrape_url = f'{self.base_url_league}{league}'

        scripts = self._find_scripts(scrape_url)

        games_string = scripts[1].string
        teams_string = scripts[2].string
        players_string = scripts[3].string
        
        games_json = self._get_json_data(games_string)
        games_df = pd.DataFrame(games_json)
        games_df = self._transform_games_data_df(games_df)

        teams_json = self._get_json_data(teams_string)
        teams_league_stats_df = self._get_teams_stats_league_df(teams_json)

        player_json = self._get_json_data(players_string)
        player_df = pd.DataFrame(player_json)

        return games_df, teams_league_stats_df, player_df

    def scrape_player(self, player: int | str) -> tuple[tuple, pd.DataFrame, pd.DataFrame]:
        """Scrapes detailed statistics for a single player.

        Args:
            player (int | str): The player's unique Understat ID (e.g., 8260)
                                or the full URL to their player page.

        Returns:
            tuple[tuple, pd.DataFrame, pd.DataFrame]: A tuple containing:
                1. A nested tuple with grouped stats:
                   (season_df, positions_list, situations_list, shot_zones_list, shot_types_list).
                2. A DataFrame of the player's performance stats per position.
                3. A DataFrame of every shot the player has taken.
        """
        scrape_url = self._get_scrape_url(player, self.base_url_player)

        scripts = self._find_scripts(scrape_url)

        groups_string = scripts[1].string
        player_stats_string = scripts[2].string
        shots_data_string = scripts[3].string

        player_groups_tuple = self._get_player_groups_df(groups_string)
        player_positions_df = self._get_player_positions_df(player_stats_string)

        shots_data_json = self._get_json_data(shots_data_string)
        shots_df = pd.DataFrame(shots_data_json)

        return player_groups_tuple, player_positions_df, shots_df
    
    def scrape_players_from_league(self, year: str, league: str, init: int, end: int) -> dict:
        """Scrapes all players from a specific league season.

        Args:
            year (str): The season year (e.g., '2023').
            league (str): The name of the league (e.g., 'EPL').

        Returns:
            dict: A dictionary where keys are player URLs and values are the scraped data.
        """
        league_url = self.leagues[league]
        season_url = f'{league_url}{year}'

        _, _, players_data = self.scrape_league(season_url)
        players_data['id'] = players_data['id'].astype(int)

        players = dict()

        if end == -1:
            players_data = players_data[init:]
        else:
            players_data = players_data[init:end]
        
        for player_id in tqdm(players_data['id'], desc="Scraping players from league"):
            player_stats = self.scrape_player(player_id)
            player_name = players_data[players_data['id'] == player_id]['player_name'].values[0]

            players[player_name] = player_stats

        return players



    def scrape_team(self, url: str) -> list[pd.DataFrame]:
        """Scrapes data for a specific team's season.

        Args:
            url (str): The full URL to a team's page for a specific season
                       (e.g., 'https://understat.com/team/Manchester_City/2023').

        Returns:
            list[pd.DataFrame]: A list containing multiple DataFrames:
                - [0]: All games for the team in that season.
                - [1]: All players who played for the team in that season.
                - [2:]: DataFrames for various team statistics.
        """
        scripts = self._find_scripts(url)

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
    
    def scrape_teams_from_league(self, year: str, league: str, init: int = 0, end: int = -1) -> dict:      
        _, teams_data, _ = self.scrape_season_data(year, league)
        teams_links = [
            f'https://understat.com/team/{x["title"].replace(" ", "_")}/{year.split("/")[0]}'
            for x in teams_data.values()
        ]

        return self.scrape_teams_from_links(teams_links, init, end)
    
    def scrape_teams_from_links(self, team_links: list[str], init: int, end: int) -> dict:
        """Scrapes data for multiple teams given their URLs.

        Args:
            team_links (list[str]): A list of team URLs to scrape.

        Returns:
            dict: A dictionary where keys are team URLs and values are the scraped data.
        """
        return self._scrape_teams(team_links, '', '', init, end)

    def scrape_match(self, match: int | str) -> tuple[list[pd.DataFrame], str, str]:
        """Scrapes detailed data for a single match.

        Args:
            match (int | str): The match's unique Understat ID (e.g., 22169)
                               or the full URL to the match page.

        Returns:
            tuple[list[pd.DataFrame], str, str]: A tuple containing:
                1. A list of DataFrames: [shots_df, roster_df, teams_stats_df].
                2. The home team's name.
                3. The away team's name.
        """
        scrape_url = self._get_scrape_url(match, self.base_url_match)

        scripts = self._find_scripts(scrape_url)

        shots_data = scripts[1].string
        roster_data = scripts[2].string
        teams_data = scripts[1].string # This is correct, team stats are in the same script as shots

        shots_df = self._get_shots_df(shots_data)
        roster_df = self._get_roster_stats_df(roster_data)
        teams_df = self._get_teams_stats_df(teams_data)

        home_team = shots_df['h_team'].iloc[0]
        away_team = shots_df['a_team'].iloc[0]

        return ([shots_df, roster_df, teams_df], home_team, away_team)
    
    def scrape_matches_from_league(self, year: str, league: str, init: int = 0, end: int = -1) -> tuple[dict, dict, dict]:
        """Scrapes all matches for a specific league season.

        Args:
            year (str): The season year (e.g., '2023').
            league (str): The name of the league (e.g., 'EPL').

        Returns:
            tuple[dict, dict, dict]: A tuple containing:
                1. A dictionary of all matches played in the season.
                2. A dictionary of teams and their stats.
                3. A dictionary of players and their stats.
        """
        matches_data, _, _ = self.scrape_season_data(year, league)
        match_links = [f'https://understat.com/match/{x["id"]}' for x in matches_data if x['isResult']]

        return self._scrape_matches(match_links, init, end), \
    
    def scrape_from_match_links(self, match_links: list[str], init: int = 0, end: int = -1) -> dict:
        """Scrapes detailed data for multiple matches given their URLs.

        Args:
            match_links (list[str]): A list of match URLs to scrape.

        Returns:
            dict: A dictionary where keys are match URLs and values are the scraped data.
        """
        return self._scrape_matches(match_links, init, end)
        
    def _scrape_matches(self, match_links: list[str], init: int, end: int) -> dict:
        """Scrapes detailed data for multiple matches.

        Args:
            match_links (list[str]): A list of match URLs to scrape.

        Returns:
            dict: A dictionary where keys are match URLs and values are the scraped data.
        """
        matches = dict()

        if end == -1:
            match_links = match_links[init:]
        else:
            match_links = match_links[init:end]


        for match_link in tqdm(match_links, desc="Scraping matches"):
            try:
                matches[match_link] = self.scrape_match(match_link)
            except Exception as e:
                print(f"Error scraping match {match_link}: {e}")

        return matches
    
    def _scrape_teams(self, teams_links: list[str], league: str, year: str, init: int, end: int) -> dict:
        """
        Scrapes data for multiple teams given their links, league, and year.

        Args:
            teams_links (list[str]): List of URLs or identifiers for the teams to scrape.
            league (str): Name of the league for which teams are being scraped.
            year (str): Year or season for which data is being scraped.

        Returns:
            dict: A dictionary where keys are team links and values are the scraped data for each team.

        Notes:
            If an error occurs while scraping a team, the error is printed and the team is skipped.
        """
        teams = dict()

        if end == -1:
            teams_links = teams_links[init:]
        else:
            teams_links = teams_links[init:end]
        
        for team_link in tqdm(teams_links, desc=f"Scraping teams for {league} {year}"):
            try:
                teams[team_link] = self.scrape_team(team_link)
            except Exception as e:
                print(f"Error scraping team {team_link}: {e}")

        return teams
    

    def _get_shots_df(self, shots_data: str) -> pd.DataFrame:
        """Parses shot data from a JSON string into a DataFrame.

        Args:
            shots_data (str): The raw string content from the shots data script.

        Returns:
            pd.DataFrame: A DataFrame of all shots from the match, sorted by minute.
        """
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
        """Parses roster data from a JSON string into a DataFrame.

        Args:
            roster_data (str): The raw string content from the roster data script.

        Returns:
            pd.DataFrame: A DataFrame of all players in the match roster and their stats.
        """
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
        """Parses general team statistics from a JSON string.
        
        This data is located in a different part of the shots data script.

        Args:
            team_data (str): The raw string content from the shots data script.

        Returns:
            pd.DataFrame: A single-row DataFrame with overall match statistics.
        """
        match_info_index = team_data.index("match_info")
        str_start = team_data.index("('", match_info_index) + 2
        str_end = team_data.index("')", match_info_index)
        json_data = team_data[str_start:str_end]
        json_data = json_data.encode('utf-8').decode('unicode_escape')

        json_data = json.loads(json_data)

        team_stats_df = pd.DataFrame([json_data])

        return team_stats_df
    
    def _get_player_groups_df(self, groups_string: str) -> tuple:
        """Parses a player's grouped statistics from a JSON string.

        Args:
            groups_string (str): The raw string content from the player groups script.

        Returns:
            tuple: A tuple containing various DataFrames and lists of DataFrames:
                   (season_df, positions_list, situations_list, shot_zones_list, shot_types_list).
        """
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
        """Parses a player's positional statistics from a JSON string.

        Args:
            player_stats_string (str): The raw string from the player positions script.

        Returns:
            pd.DataFrame: A DataFrame of player stats, indexed by position.
        """
        json_data = self._get_json_data(player_stats_string)

        new_data = {}
        for position, position_dict in json_data.items():
            if position not in new_data:
                new_data[position] = {}
            for stat, stats in position_dict.items():
                for math, stat_value in stats.items():
                    new_data[f'{position}'][f'{stat}_{math}'] = stat_value

        return pd.DataFrame(new_data).transpose()
    
    def _get_teams_stats_league_df(self, teams_json: dict) -> list[pd.DataFrame]:
        """Parses and structures team statistics for an entire league.

        This function restructures the JSON to create a list of DataFrames,
        where each DataFrame contains the match history for a single team.

        Args:
            teams_json (dict): The parsed JSON object for teams data.

        Returns:
            list[pd.DataFrame]: A list of DataFrames, one for each team's history.
        """
        for _, item in teams_json.items():
            for _, item2 in item.items():
                if isinstance(item2,list):
                    for dictionary in item2:
                        for key2_2, item2_2 in item.items():
                            if isinstance(item2_2, str):
                                dictionary[key2_2] = item2_2

        teams_dfs_list = []
        for _, item in teams_json.items():
            teams_dfs_list.append(pd.DataFrame(item['history']))

        return teams_dfs_list
            
    def _transform_games_data_df(self, games_data_df: pd.DataFrame) -> pd.DataFrame:
        """Transforms a raw games DataFrame into a more usable format.

        It unnests dictionary columns like 'h', 'a', 'goals', 'xG', and
        'forecast' into their own distinct columns.

        Args:
            games_data_df (pd.DataFrame): The raw DataFrame of games data.

        Returns:
            pd.DataFrame: A cleaned and transformed DataFrame of games data.
        """
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
        """Transforms raw team statistics JSON into a list of DataFrames.

        It unnests the 'against' dictionary into separate columns.

        Args:
            statistics_json (dict): The raw JSON object for team statistics.

        Returns:
            list[pd.DataFrame]: A list of transformed DataFrames.
        """
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
        """Extracts and parses an embedded JSON string from a script tag.

        The data on Understat is stored inside script tags as a string literal,
        e.g., var shotsData = ('JSON_STRING'). This function extracts JSON_STRING.

        Args:
            data (str): The raw string content of a <script> tag.

        Returns:
            dict: A dictionary parsed from the JSON data.
        """
        str_start = data.index("('") + 2
        str_end = data.index("')")
        json_data = data[str_start:str_end]
        json_data = json_data.encode('utf-8').decode('unicode_escape')

        json_data = json.loads(json_data)

        return json_data
    
    def _get_scrape_url(self, match: int | str, base_url: str) -> str:
        """Constructs the full URL for scraping.

        Args:
            match (int | str): The ID or full URL provided by the user.
            base_url (str): The base URL for the data type (match, player, etc.).

        Returns:
            str: The complete and valid URL to scrape.
        
        Raises:
            ValueError: If 'match' is not an int or a str.
        """
        scrape_url = match

        if isinstance(match, int):
            match_id = f"{match}"
            scrape_url = f"{base_url}{match_id}"
        elif not isinstance(match, str):
            raise ValueError(
                "O argumento match precisa ser um int (match_id) ou uma string (url da partida)"
            )
        
        return scrape_url
    
    def scrape_season_data(self, year: str, league: str):
        """ Scrapes data for chosen Understat league season.

        Parameters
        ----------
        year : str
            See the :ref:`understat_year` `year` parameter docs for details.
        league : str
            League. Look in ScraperFC.Understat comps variable for available leagues.

        Returns
        -------
        : tuple of dicts
            matches_data, teams_data, players_data
        """
        try:
            league_url = self.leagues[league]
            season_url = f'{league_url}{year}'
        except KeyError as e:
            raise KeyError(f"Liga inválida: {e}")
        
        soup = BeautifulSoup(requests.get(season_url).content, 'html.parser')

        scripts = soup.find_all('script')
        dates_data_tag = [x for x in scripts if 'datesData' in x.text][0]
        teams_data_tag = [x for x in scripts if 'teamsData' in x.text][0]
        players_data_tag = [x for x in scripts if 'playersData' in x.text][0]

        matches_data = self._get_json_data(dates_data_tag.text)
        teams_data = self._get_json_data(teams_data_tag.text)
        players_data = self._get_json_data(players_data_tag.text)

        return matches_data, teams_data, players_data
    
    def _find_scripts(self, url: str) -> list:
        """Fetches a URL and finds all script tags within the HTML.

        Args:
            url (str): The URL of the page to scrape.

        Returns:
            list: A list of BeautifulSoup script tag objects.
        """
        res = requests.get(url)
        res.raise_for_status() # Raises an exception for bad status codes
        soup = BeautifulSoup(res.content, "html.parser")
        scripts = soup.find_all('script')

        return scripts