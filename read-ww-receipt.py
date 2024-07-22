import os
import requests
import sys
import pdfplumber
import polars as pl

from utils import get_woolworths_data, create_new_table



if __name__ == "__main__":

    pdf_path = sys.argv[1]
    pdf_file_name = pdf_path.split('/')[-1].split('\\')[-1].split('.')[0]
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    else:
        output_path = f"{pdf_file_name}.csv"
    
    using_temp_file = False
    # Check if pdf_path is a web link or a local file
    if pdf_path.startswith('http'):
        print(f"Downloading {pdf_path}")
        r = requests.get(pdf_path)
        print(r.status_code, r.reason)
        # Save the pdf file to a temporary file
        using_temp_file = True
        with open(f'{pdf_file_name}.pdf', 'wb') as f:
            f.write(r.content)
        pdf_path = f'{pdf_file_name}.pdf'


    with pdfplumber.open(pdf_path) as pdf:
        pages = {}
        page_count = len(pdf.pages)
        for page in pdf.pages:
            # page_obj = gen_properties_dict(page)
            page_obj = {}
            page_obj['rects'] = page.rects
            page_obj['lines'] = page.lines
            page_obj['page_number'] = page.page_number
            page_obj['width'] = page.width
            page_obj['height'] = page.height
            page_obj['tables'] = page.extract_tables()
            page_obj['words'] = page.extract_words()
            page_obj['texts'] = page.extract_text()
            pages[page.page_number] = page_obj
            rects = page.rects
            words = page.extract_words()

    if using_temp_file:        
        os.remove(pdf_path)

    woolworths_summary, woolworths_tables = get_woolworths_data(pages)
    woolworths_tables_new = create_new_table(woolworths_tables)
    woolworths_tables_new_df = pl.DataFrame(woolworths_tables_new[1:], schema=woolworths_tables_new[0])
    woolworths_tables_new_df.write_csv(output_path)
    print(woolworths_summary)