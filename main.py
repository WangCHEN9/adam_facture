
from pathlib import Path
from article_info import Article_Info
from ivivi_facture_reader import IviviFactureReader


if __name__ == "__main__":
    # pdf_path = Path(r"data/Facture 01-11 au 15-11.pdf")
    pdf_path = Path(r"data/Facture 16-11 au 30-11.pdf")

    article_info_excel = Path(r"data/DONNEES DOUANE PYTHON.xlsx")
    output_folder_path = Path(r'output')
    article_info = Article_Info(source_excel=article_info_excel)

    x = IviviFactureReader(pdf_path=pdf_path, article_info=article_info, output_folder_path=output_folder_path)
    x.run()