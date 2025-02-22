
from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader
from jessy_facture_reader import JessyFactureReader
from dolvika_facture_reader import DolvikaFactureReader
from mod_facture_reader import ModFactureReader
from loguru import logger
import pandas as pd


if __name__ == "__main__":

    output_folder_path = Path(r'output')
    article_info_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    article_info = Article_Info(source_excel=article_info_excel)

    pdf_path = Path(r"input/MOD CMD.pdf")
    log_file_path = output_folder_path / "log" / f"{pdf_path.stem}.log"
    if log_file_path.exists():
        # one log file per pdf, and clean existed log file
        log_file_path.unlink()

    logger.add(log_file_path, level="INFO")

    x = ModFactureReader(pdf_path=pdf_path, article_info=article_info, output_folder_path=output_folder_path)
    df = x.run()
    if isinstance(df, pd.DataFrame):
        df.to_excel(output_folder_path / f"{pdf_path.stem}.xlsx", index=False)