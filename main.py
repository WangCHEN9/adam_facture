
from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader

from loguru import logger


if __name__ == "__main__":

    output_folder_path = Path(r'output')
    article_info_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    article_info = Article_Info(source_excel=article_info_excel)

    #* add pdf path here
    pdf_path_list = [
        Path(r"data/Facture 01-11 au 15-11.pdf"),
        Path(r"data/Facture 16-11 au 30-11.pdf"),
    ]

    for pdf_path in pdf_path_list:
        log_file_path = output_folder_path / "log" / f"{pdf_path.stem}.log"
        if log_file_path.exists():
            # one log file per pdf, and clean existed log file
            log_file_path.unlink()

        logger.add(log_file_path, level="INFO")

        x = IviviFactureReader(pdf_path=pdf_path, article_info=article_info, output_folder_path=output_folder_path)
        x.run()