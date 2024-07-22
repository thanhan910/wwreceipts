import os
import requests
import sys
import pdfplumber
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.patches as patches



def gen_properties_dict(obj):
    dict_obj = {}
    for prop in dir(obj):
        if not prop.startswith("_"):
            # Skip properties that are methods
            attr = getattr(obj, prop)
            if not callable(attr):
                dict_obj[prop] = attr
    return dict_obj


def get_woolworths_table(words, rects):
    top_edges = rects[1:13:2]

    bottom_edges = rects[-16:-10]

    side_edges = rects[0:13:2] + rects[13:-16]

    assert len(side_edges) % 7 == 0

    side_rows = [side_edges[i:i+7] for i in range(0, len(side_edges), 7)]

    rows_coords = [{
        'top': r[1]['top'],
        'bottom': r[1]['bottom'],
    } for r in side_rows]

    cols_coords = [{
        'x0': e['x0'],
        'x1': e['x1'],
    } for e in top_edges]

    table_words = [w for w in words if (w['top'] >= top_edges[1]['bottom']) and (w['bottom'] <= bottom_edges[1]['top']) and (w['upright'])]

    for word in table_words:
        for i, row in enumerate(rows_coords):
            if (word['top'] >= row['top']) and (word['bottom'] <= row['bottom']):
                word['row'] = i
                break

        for i, col in enumerate(cols_coords):
            if (word['x0'] >= col['x0']) and (word['x1'] <= col['x1']):
                word['col'] = i
                break

    array2d = [[None for _ in range(len(cols_coords))] for _ in range(len(rows_coords))]


    for word in table_words:
        y = word['row']
        x = word['col']
        text = word['text']
        array2d[y][x] = [text] if array2d[y][x] is None else array2d[y][x] + [text]
            
    for i, row in enumerate(array2d):
        if (i == 0):
            assert all(text[::2] == text[1::2] for cell in row for text in cell), row
            array2d[i] = [[text[::2] for text in cell] for cell in row]
        if (row[4] is None):
            assert all(cell is None for j, cell in enumerate(row) if j != 1), row
            texts = row[1]
            # Assert that the first row are bold (in each word, each character is repeated)
            assert all(text[::2] == text[1::2] for text in texts), texts
            array2d[i][1] = [text[::2] for text in texts]

    array2d = [[' '.join(cell) if isinstance(cell, list) else cell for cell in row] for row in array2d]

    return array2d


def get_woolworths_table(top_edges, side_rows, words):

    rows_coords = [{
        'top': r[1]['top'],
        'bottom': r[1]['bottom'],
    } for r in side_rows]

    cols_coords = [{
        'x0': e['x0'],
        'x1': e['x1'],
    } for e in top_edges]

    smallest_top = top_edges[1]['bottom'] # Highest point
    biggest_top = side_rows[-1][1]['bottom'] # Lowest point

    table_words = [w for w in words if (w['top'] >= smallest_top) and (w['bottom'] <= biggest_top) and (w['upright'])]

    for word in table_words:
        for i, row in enumerate(rows_coords):
            if (word['top'] >= row['top']) and (word['bottom'] <= row['bottom']):
                word['row'] = i
                break

        for i, col in enumerate(cols_coords):
            if (word['x0'] >= col['x0']) and (word['x1'] <= col['x1']):
                word['col'] = i
                break

    array2d = [[None for _ in range(len(cols_coords))] for _ in range(len(rows_coords))]


    for word in table_words:
        y = word['row']
        x = word['col']
        text = word['text']
        array2d[y][x] = [text] if array2d[y][x] is None else array2d[y][x] + [text]
            
    for i, row in enumerate(array2d):
        if (i == 0):
            assert all(text[::2] == text[1::2] for cell in row for text in cell), row
            array2d[i] = [[text[::2] for text in cell] for cell in row]
        if (row[4] is None):
            assert all(cell is None for j, cell in enumerate(row) if j != 1), row
            texts = row[1]
            # Assert that the first row are bold (in each word, each character is repeated)
            assert all(text[::2] == text[1::2] for text in texts), texts
            array2d[i][1] = [text[::2] for text in texts]

    array2d = [[' '.join(cell) if isinstance(cell, list) else cell for cell in row] for row in array2d]

    return array2d


def get_pages_edges_data(pages):
    page_count = len(pages)
    page_edges_all = {}
    bottom_edge_in_2_to_last_page = False
    summary_in_last_page = True
    for page_number in range(page_count, 0, -1):
        page = pages[page_number]
        main_rects = page["rects"][:-10]
        page_edges = {}
        if page_number == page_count:
            if len(main_rects) < (6 + 7 + 6 + 3 + 1):
                assert len(main_rects) in {3 + 1, 1}, len(main_rects)
                bottom_edge_in_2_to_last_page = True
                summary_in_last_page = (len(main_rects) == 3 + 1)
                if summary_in_last_page:
                    page_edges["summary"] = main_rects[-4:-1]
                    page_edges["rewards"] = main_rects[-1]
                else:
                    page_edges["rewards"] = main_rects[-1]
            else:
                bottom_edge_in_2_to_last_page = False
                summary_in_last_page = True
                page_edges["summary"] = main_rects[-4:-1]
                page_edges["rewards"] = main_rects[-1]
                assert (len(main_rects) - (6 + 7 + 6 + 3 + 1)) % 7 == 0, len(main_rects)
                page_edges["top"] = main_rects[1:13:2]
                page_edges["side"] = main_rects[0:13:2] + main_rects[13:-10]
                assert len(page_edges["side"]) % 7 == 0, len(page_edges["side"])
                page_edges["side"] = [page_edges["side"][i:i + 7] for i in range(0, len(page_edges["side"]), 7)] # Split into chunks of 7
                page_edges["bottom"] = main_rects[-10:-4]
        else:
            assert len(main_rects) >= 6 + 7, len(main_rects)
            if page_number == page_count - 1 and bottom_edge_in_2_to_last_page:
                if summary_in_last_page:
                    assert (len(main_rects) - (6 + 7 + 6)) % 7 == 0, len(main_rects)  # Number of edges is 6 + 7 + 7 * n + 6
                    page_edges["top"] = main_rects[1:13:2]
                    page_edges["side"] = main_rects[0:13:2] + main_rects[13:-6]
                    assert len(page_edges["side"]) % 7 == 0, len(page_edges["side"])
                    page_edges["side"] = [page_edges["side"][i:i + 7] for i in range(0, len(page_edges["side"]), 7)]  # Split into chunks of 7
                    page_edges["bottom"] = main_rects[-6:]
                else:
                    assert (len(main_rects) - (6 + 7 + 6 + 3)) % 7 == 0, len(main_rects)  # Number of edges is 6 + 7 + 7 * n + 6
                    page_edges["top"] = main_rects[1:13:2]
                    page_edges["side"] = main_rects[0:13:2] + main_rects[13:-9]
                    assert len(page_edges["side"]) % 7 == 0, len(page_edges["side"])
                    page_edges["side"] = [page_edges["side"][i:i + 7] for i in range(0, len(page_edges["side"]), 7)]  # Split into chunks of 7
                    page_edges["bottom"] = main_rects[-9:-3]
                    page_edges["summary"] = main_rects[-3:]
            else:
                assert (len(main_rects) - (6 + 7)) % 7 in [0, 6], len(main_rects)  # Number of edges is 6 + 7 + 7 * n (+ 6)
                page_edges["top"] = main_rects[1:13:2]
                page_edges["side"] = main_rects[0:13:2] + (main_rects[13:-6] if (len(main_rects) - (6 + 7)) % 7 == 6 else main_rects[13:])
                assert len(page_edges["side"]) % 7 == 0, len(page_edges["side"])
                page_edges["side"] = [page_edges["side"][i:i + 7] for i in range(0, len(page_edges["side"]), 7)] # Split into chunks of 7

        page_edges_all[page_number] = page_edges

    # Sort page_edges_all
    page_edges_all = dict(sorted(page_edges_all.items()))

    return page_edges_all

def get_summary_data(summary_edges, words):
    summary_side = summary_edges[1]
    summary_side_top = summary_side['top']
    summary_side_bottom = summary_side['bottom']
    summary_words = [w for w in words if (w['top'] >= summary_side_top) and (w['bottom'] <= summary_side_bottom) and (w['upright'])]
    summary_texts = []
    summary_text = ''
    summary_value = ''
    for word in summary_words:
        text : str = word['text']
        if not text.startswith("$"):
            assert text[::2] == text[1::2], text
            summary_text += text[::2] + " "
        else:
            summary_value = text
            summary_text = summary_text.strip()
            summary_texts.append((summary_text, summary_value))
            summary_text = ''
            summary_value = ''

    return summary_texts

def get_woolworths_data(pages):
    page_edges_all = get_pages_edges_data(pages)
    woolworths_summary = None
    woolworths_tables = []
    page_count = len(pages)
    for page_number in range(1, page_count + 1):
        page = pages[page_number]
        page_edges = page_edges_all[page_number]
        words = page['words']
        if "side" in page_edges:
            table = get_woolworths_table(page_edges["top"], page_edges["side"], words)
            if page_number == 1:
                woolworths_tables += table
            else:
                woolworths_tables += table[1:]
        if "summary" in page_edges:
            woolworths_summary = get_summary_data(page_edges["summary"], words)

    return woolworths_summary, woolworths_tables


def create_new_table(table):
    column_names = table[0] + ['Category', 'Price per unit', 'Unit', 'Supplied quantity', 'Supplied amount', 'Supplied unit']
    new_table = []
    current_type = None
    for row in table[1:]:
        supplied : str = row[3]
        price : str = row[4]
        amount : str = row[5]
        if price is not None:
            assert price.startswith('$'), price
        if amount is not None:
            assert amount.startswith('$'), amount

        if price is None:
            assert all(cell is None for j, cell in enumerate(row) if j != 1), row
            current_type = row[1]
            continue

        price_list = [w.strip() for w in price.split('/')]
        assert len(price_list) <= 2 and len(price_list) >= 1, price_list
        if len(price_list) == 1:
            price_list.append(None)

        if supplied is not None:
            supplied_list = [w.strip() for w in supplied.split(' @ ')]
            assert len(supplied_list) <= 2 and len(supplied_list) >= 1, supplied_list
            supplied_quantity = supplied_list[0]
            if len(supplied_list) == 2 or (len(supplied_list) == 1 and (not supplied_list[0].isnumeric())):
                # Get number from supplied_list[1]
                supplied_amount = ""
                dot_encountered = False
                for c in supplied_list[-1]:
                    if c.isdigit() or (c == '.' and not dot_encountered):
                        supplied_amount += c
                        if c == '.':
                            dot_encountered = True
                    else:
                        break
                supplied_unit = supplied_list[-1][len(supplied_amount):].strip()
                if len(supplied_list) == 1:
                    supplied_quantity = supplied_amount
                supplied_list = [supplied_quantity, supplied_amount, supplied_unit]
            else:
                supplied_list = [supplied_quantity, supplied_quantity, None]
        else:
            supplied_list = [None, None, None]

        new_row = row + [current_type] + price_list + supplied_list
        new_table.append(new_row)

    return [column_names] + new_table


def inspect_page_rectanges(pages):
    # Draw rectangles in each page

    fig, ax = plt.subplots(nrows=len(pages), figsize=(10, 10))

    for page_number, page in pages.items():
        i = page_number - 1
        ax[i].set_xlim(0, page['width'])
        ax[i].set_ylim(0, page['height'])
        ax[i].set_aspect('equal')

        rects = page['rects']
        assert len(rects) >= 14, len(rects)
        if len(rects) > 14:
            assert len(rects) >= 14 + 13, len(rects)
        print(len(rects))
        # # 6:12
        # # 6:12
        # # 13:20
        # # 20:27        
        # for rect in rects[-14:-10]:
        main_rects = rects[:-10]
        for rect in main_rects[-4:-1]:
            rect_obj = patches.Rectangle((rect['x0'], rect['y0']), rect['width'], rect['height'], linewidth=1, edgecolor='r', facecolor='none')
            ax[i].add_patch(rect_obj)

        # rect_edges = page['rect_edges']
        # for edge in rect_edges[0:20]:
        #     rect_obj = patches.Rectangle((edge['x0'], edge['y0']), edge['width'], edge['height'], linewidth=1, edgecolor='r', facecolor='none')
        #     ax[i].add_patch(rect_obj)

    return fig, ax


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