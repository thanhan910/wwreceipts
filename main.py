from fastapi import FastAPI, HTTPException, File, Form, UploadFile, Response
from fastapi.responses import HTMLResponse, FileResponse

import time
import pdfplumber
import polars as pl

from utils import get_woolworths_data, create_new_table

app = FastAPI()





@app.post("/upload-pdf/")
async def create_upload_file(file: UploadFile = File(...)):
    if file.content_type != 'application/pdf':
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a PDF file.")
    # file_location = f"uploads/{file.filename}"
    # with open(file_location, "wb+") as file_object:
    #     file_object.write(await file.read())

    with pdfplumber.open(file.file) as pdf:
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


    woolworths_summary, woolworths_tables = get_woolworths_data(pages)
    woolworths_tables_new = create_new_table(woolworths_tables)
    woolworths_tables_new_df = pl.DataFrame(woolworths_tables_new[1:], schema=woolworths_tables_new[0])
    csv_file_path = f"{time.time()}.csv"
    woolworths_tables_new_df.write_csv(csv_file_path)

    content = f"""
<body>
<h1>Woolworths Summary</h1>
{
    "<br>".join(f"{text} {value}" for text, value in woolworths_summary)
}
<h1>Woolworths Tables</h1>
<form action="/upload-pdf/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
<form action="/download-csv/" method="post">
<input type="hidden" name="filepath" value="{csv_file_path}">
<input type="hidden" name="filename" value="{file.filename.removesuffix('.pdf')}.csv">
<input type="submit" value="Download CSV">
</form>
<table>
<thead>
{"".join(f"<th>{col}</th>" for col in woolworths_tables_new[0])}
</thead>
<tbody>
{" ".join("<tr>" + "".join(f"<td>{cell}</td>" if cell is not None else f"<td></td>" for cell in row) + "</tr>" for row in woolworths_tables_new[1:])}
</tbody>
</table>
</body>
    """
    return HTMLResponse(content=content)


@app.post("/download-csv/")
async def download_csv(filepath: str = Form(...), filename: str = Form(...)):
    return FileResponse(filepath, media_type='text/csv', filename=filename)


@app.get("/")
async def main():
    content = """
<body>
<h1>Woolworths Receipts</h1>
<p>Upload a Woolworths receipt PDF file to extract the summary and tables and convert it to CSV.</p>
<form action="/upload-pdf/" enctype="multipart/form-data" method="post">
<input name="file" type="file">
<input type="submit">
</form>
</body>
    """
    return HTMLResponse(content=content)