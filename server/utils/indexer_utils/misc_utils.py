import json
import magic
from typing import Optional
from collections import namedtuple
from nlm_ingestor.ingestor_utils.utils import NpEncoder
from nlm_ingestor.ingestor.visual_ingestor import block_renderer
from bs4 import BeautifulSoup


def ingest_data_row_file(data_row_file_info):
    blocks = data_row_file_info.blocks
    sents, _ = blocks_to_sents(blocks)
    block_texts, _ = get_block_texts(blocks)
    result = [
        {"title": data_row_file_info.title, "text": data_row_file_info.html_str, "title_page_fonts": []},
        {"title": data_row_file_info.title, "document": data_row_file_info.json_dict, "title_page_fonts": []},  # JSON not enabled now.
    ]

    file_data = json.dumps(result)
    return blocks, block_texts, sents, file_data, result

def render_file(
    fn, title_fn, title_is_fn: bool = True, mime_type: Optional[str] = None,
):
    result = {}
    blocks, title = ingest_file(fn, mime_type)
    if not title:
        if title_is_fn:
            title = title_fn
        else:
            title = blocks[0]["block_text"]
            if len(title) > 50:
                title = title[0:50] + "..."

    result["title"] = title
    sents, block_texts, html_str = render_blocks(blocks)
    #this code needs a more complete rework
    doc_dict = {"blocks": blocks, "line_style_classes": {}, "class_levels": {}}
    doc = namedtuple("ObjectName", doc_dict.keys())(*doc_dict.values())
    br = block_renderer.BlockRenderer(doc)
    html_str = br.render_html()
    json_dict = br.render_json()

    result["text"] = html_str

    result = [
        {"title": title, "text": html_str, "title_page_fonts": {"first_level": [title]}},
        {"title": title, "document": json_dict, "title_page_fonts": {"first_level": [title]}},  # JSON not enabled now.
    ]

    file_data = [json.dumps(res, cls=NpEncoder) for res in result]

    return blocks, block_texts, sents, file_data, result, [1, 1], 0


def blocks_to_sents(blocks, flatten_merged_table=False, debug=False):
    block_texts = []
    block_info = []
    header_block_idx = -1
    header_match_idx = -1
    header_match_idx_offset = -1
    header_block_text = ""
    is_rendering_table = False
    is_rendering_merged_cells = False
    table_idx = 0
    levels = []
    prev_header = None
    block_idx = 0
    for block_idx, block in enumerate(blocks):
        block_type = block["block_type"]
        if block_type == "header":
            if debug:
                print("---", block["level"], block["block_text"])
            header_block_text = block["block_text"]
            header_block_idx = block["block_idx"]
            header_match_idx = header_match_idx_offset + 1
            if prev_header and block["level"] <= prev_header['level'] and len(levels) > 0:
                while len(levels) > 0 and levels[-1]["level"] >= block["level"]:
                    if debug:
                        print("<<", levels[-1]["level"], levels[-1]["block_text"])
                    levels.pop(-1)
            if debug:
                print(">>", block["block_text"])
            levels.append(block)
            prev_header = block
            if debug:
                print("-", [str(level['level']) + "-" + level['block_text'] for level in levels])
        block["header_text"] = header_block_text
        block["header_block_idx"] = header_block_idx
        block["header_match_idx"] = header_match_idx
        block["block_idx"] = block_idx

        level_chain = []
        for level in levels:
            level_chain.append({"block_idx": level["block_idx"], "block_text": level["block_text"]})
        # remove a level for header
        if block_type == "header":
            level_chain = level_chain[:-1]
        level_chain.reverse()
        block["level_chain"] = level_chain

        # if block_type == "header" or block_type == "table_row":
        if (
                block_type == "header"
                and not is_rendering_table and 'is_table_start' not in block
        ):
            block_texts.append(block["block_text"])
            # append text from next block to header block
            # TODO: something happened here, it messed up the match_text
            # if block_type == "header" and block_idx + 1 < len(blocks):
            #     block[
            #         "block_text"
            #     ] += blocks[block_idx+1]['block_text']

            block_info.append(block)
            header_match_idx_offset += 1
        elif (
                block_type == "list_item" or block_type == "para" or block_type == "numbered_list_item"
        ) and not is_rendering_table:
            block_sents = block["block_sents"]
            header_match_idx_offset += len(block_sents)
            for sent in block_sents:
                block_texts.append(sent)
                block_info.append(block)
        elif 'is_table_start' in block:
            is_rendering_table = True
            if 'has_merged_cells' in block:
                is_rendering_merged_cells = True
        elif 'is_table_start' not in block and not is_rendering_table and block_type == "table_row":
            block_info.append(block)
            block_texts.append(block["block_text"])
            header_match_idx_offset += 1

        if is_rendering_table:
            if is_rendering_merged_cells and "effective_para" in block and flatten_merged_table:
                eff_header_block = block["effective_header"]
                eff_para_block = block["effective_para"]

                eff_header_block["header_text"] = block["header_text"]
                eff_header_block["header_block_idx"] = block["block_idx"]
                eff_header_block["header_match_idx"] = header_match_idx_offset + 1
                eff_header_block["level"] = block["level"] + 1
                eff_header_block["level_chain"] = block["level_chain"]

                eff_para_block["header_block_idx"] = block["block_idx"]
                eff_para_block["header_match_idx"] = header_match_idx_offset + 1
                eff_para_block["level"] = block["level"] + 2
                eff_para_block["level_chain"] = [
                                {
                                    "block_idx": eff_header_block["block_idx"],
                                    "block_text": eff_header_block["block_text"],
                                },
                ] + eff_header_block["level_chain"]
                header_match_idx_offset += 1
                block_info.append(block["effective_header"])
                block_texts.append(block["effective_header"]["block_text"])
                for sent in block["effective_para"]["block_sents"]:
                    block_texts.append(sent)
                    block_info.append(block["effective_para"])
                header_match_idx_offset += len(block["effective_para"]["block_sents"])
            else:
                block["table_idx"] = table_idx
                block_info.append(block)
                block_texts.append(block["block_text"])
                header_match_idx_offset += 1

        if 'is_table_end' in block:
            is_rendering_table = False
            table_idx += 1

    return block_texts, block_info

def get_block_texts(blocks):
    block_texts = []
    block_info = []
    for block in blocks:
        block_type = block["block_type"]
        if (
            block_type == "list_item"
            or block_type == "para"
            or block_type == "numbered_list_item"
            or block_type == "header"
        ):
            block_texts.append(block["block_text"])
            block_info.append(block)
    return block_texts, block_info

def render_nested_block(block, block_idx, tag, sent_idx, html_str):
    block_sents = block["block_sents"]
    if len(block_sents) == 1:
        html_str = (
            html_str
            + "<"
            + tag
            + " class='nlm_sent_"
            + str(sent_idx)
            + "'>"
            + block_sents[0]
            + "</"
            + tag
            + ">"
        )
        sent_idx = sent_idx + 1
    else:
        html_str = html_str + "<" + tag + " class='nlm_block_" + str(block_idx) + "'>"
        for sent in block_sents:
            html_str = (
                html_str
                + "<span class='nlm_sent_"
                + str(sent_idx)
                + "'> "
                + sent
                + "</span>"
            )
            sent_idx = sent_idx + 1
        html_str = html_str + "</" + tag + ">"
    return sent_idx, html_str


def render_blocks(blocks):
    html_str = ""
    sent_idx = 0
    nested_block_idx = 0
    skip_cnt = 0
    for idx, block in enumerate(blocks):
        block_type = block["block_type"]
        block_text = block["block_text"]
        if skip_cnt:
            skip_cnt -= 1
            continue

        if block_type == "header":
            html_str = (
                html_str
                + "<h3 class='nlm_sent_"
                + str(sent_idx)
                + "'>"
                + block_text
                + "</h3>"
            )
            sent_idx = sent_idx + 1

        elif block_type == "list_item":
            sent_idx, html_str = render_nested_block(
                block, nested_block_idx, "li", sent_idx, html_str,
            )
            nested_block_idx = nested_block_idx + 1
        elif block_type == "para" or block_type == "numbered_list_item":
            sent_idx, html_str = render_nested_block(
                block, nested_block_idx, "p", sent_idx, html_str,
            )
            nested_block_idx = nested_block_idx + 1
        elif block_type == "table_row":
            if block["text_group_start_idx"] != -1 and False:  # and False
                table_start_idx = block["text_group_start_idx"]
                table = [block["block_list"]]
                skip_cnt = 0
                for table_block_idx in range(idx + 1, len(blocks)):
                    if (
                        table_start_idx
                        == blocks[table_block_idx]["text_group_start_idx"]
                    ):
                        table.append(blocks[table_block_idx]["block_list"])
                        skip_cnt += 1
                        sent_idx = sent_idx + 1
                    else:
                        break
                html_str += "<br>" + table_builder.construct_table(table) + "<br>"

            else:
                html_str = (
                    html_str
                    + "<p memo='tr' class='nlm_sent_"
                    + str(sent_idx)
                    + "'>"
                    + block_text
                    + "</p>"
                )
                sent_idx = sent_idx + 1
        elif block_type == "hr":
            html_str += "<hr>"
            # sent_idx = sent_idx + 1
    sents, _ = blocks_to_sents(blocks)
    block_texts, _ = get_block_texts(blocks)
    return sents, block_texts, html_str
