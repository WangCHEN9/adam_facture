
from pathlib import Path
from typing import Union

import pandas as pd
from difflib import get_close_matches
from loguru import logger


class Article_Info:
    def __init__(self, source_excel:Path) -> None:
        self.df = pd.read_excel(source_excel, sheet_name="ARTICLE+CODE+POIDS")
        self._df_habilite = pd.read_excel(source_excel, sheet_name="STE+NO HABILITE")

    # @property
    # def habilite_code(self) -> str:
    #     return self._df_habilite.loc[self._df_habilite['NOM STE'] == self.site_name, 'NO HBILITE'].values[0]

    def get_article_info(self, article_name:str, target_col:str) -> Union[str, None]:
        df = self.df
        related_code = df.loc[df['ARTICLE'] == article_name, target_col].values

        if related_code.size > 0:
            logger.debug(f"The {target_col} for {article_name} is {related_code[0]}")
            return related_code[0]
        else:
            # Find the closest match
            closest_match = get_close_matches(article_name, df['ARTICLE'], n=1, cutoff=0.6)
            if closest_match:
                closest_article = closest_match[0]
                closest_code = df.loc[df['ARTICLE'] == closest_article, target_col].values[0]
                logger.warning(f"No exact match found for '{article_name}'. Closest match: '{closest_article}' with {target_col}='{closest_code}'")
                return closest_code
            else:
                for possible_match in df['ARTICLE']:
                    if article_name.startswith(possible_match) or possible_match.startswith(article_name):
                        closest_code = df.loc[df['ARTICLE'] == possible_match, target_col].values[0]
                        logger.warning(f"No exact match found for '{article_name}'. Closest match: '{possible_match}' with {target_col}='{closest_code}'")
                        return closest_code
                logger.error(f"No close matches found for '{article_name}'")

if __name__ == "__main__":
    source_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    a = Article_Info(source_excel, 'IVIVI')
    print(a.habilite_code)

    article_code = a.get_article_info("BLOUSON")
    article_weight = a.get_article_weight(article_code)
    print(article_code)
    print(article_weight)