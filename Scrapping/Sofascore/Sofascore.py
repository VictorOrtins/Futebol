import requests

import pandas as pd

from ScraperFC.sofascore import Sofascore
from ScraperFC import sofascore as sfc_sofascore


def requisicao_personalizada(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
    }

    response = requests.get(url, headers=headers)

    return response


sfc_sofascore.botasaurus_get = requisicao_personalizada

sfc_sofascore.comps['Brasileirão'] = '325'
sfc_sofascore.comps['Brasileirão Série B'] = '390'
sfc_sofascore.comps['Liga Portugal'] = '238'
sfc_sofascore.comps['Liga Profesional'] = '155'

class SofascoreScrapper:
    def __init__(self):
        self.sofascore_scrapper = Sofascore()

    def scrape_team_stats_from_match(self, match) -> pd.DataFrame:
        return self.sofascore_scrapper.scrape_team_match_stats(match)

    def scrape_team_stats_from_matches(self, year, league) -> list[pd.DataFrame, str, str]:
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_team_stats_from_match)    
    def scrape_heatmaps_from_match(self, match) -> pd.DataFrame:
        return self.sofascore_scrapper.scrape_heatmaps(match)

    def scrape_heatmaps_from_matches(self, year, league) -> list[pd.DataFrame, str, str]:
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_heatmaps_from_match)
    
    def scrape_match_momentum(self, match) -> pd.DataFrame:
        return self.sofascore_scrapper.scrape_team_match_stats(match)

    def scrape_matches_momentums(self, year, league) -> list[pd.DataFrame, str, str]:
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_match_momentum)
    
    def scrape_match_shots(self, match) -> pd.DataFrame:
        return self.sofascore_scrapper.scrape_match_shots(match)

    def scrape_matches_shots(self, year, league):
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_match_shots)
    
    def scrape_player_avg_position_from_match(self, match):
        return self.sofascore_scrapper.scrape_player_average_positions(match)

    def scrape_player_avg_position_from_matches(self, year, league):
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_player_avg_position_from_match)
    
    def scrape_player_league_stats(self, year, league, accumulation, selected_position):
        return self.sofascore_scrapper.scrape_player_league_stats(year, league, accumulation, selected_position)
    
    def scrape_player_stats_from_match(self, match):
        return self.sofascore_scrapper.scrape_player_match_stats(match)
    
    def scrape_player_stats_from_matches(self, year, league):
        matches_ids = self._get_matches_ids(year, league)

        return self._append_scraped_artifact(matches_ids, self.scrape_player_stats_from_match)

    def _append_scraped_artifact(self, matches_ids, scrape_function):
        scrape_list = []
        for match_id in matches_ids:
            heatmap_df = scrape_function(match_id[0])
            scrape_list.append((heatmap_df, match_id[1], match_id[2]))

        return scrape_list

    def _get_matches_ids(self, year, league) -> list[int, str, str]:
        matches_dicts = self.sofascore_scrapper.get_match_dicts(year, league)

        matches_ids = [
            (match["id"], match["homeTeam"]["name"], match["awayTeam"]["name"])
            for match in matches_dicts
        ]

        return matches_ids
    


