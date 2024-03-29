import ast

import numpy as np
from bs4 import BeautifulSoup
from server.storage import nosql_db

from nlm_ingestor.ingestor.line_parser import list_chars
from nlm_ingestor.ingestor.line_parser import punctuations
from nlm_ingestor.ingestor.line_parser import stop_words
from nlm_ingestor.ingestor.visual_ingestor import table_parser


PUNCTUATION = set(punctuations)
LIST_CHAR = set(list_chars)
STOP_WORDS = stop_words


class BBOXDetector:
    def __init__(self, file_idx, tika_html=None, blocks=None):
        self.bboxes = {}
        self.file_idx = file_idx

        self.tika_html = tika_html

        if blocks:
            self.convert_blocks_to_bboxes(blocks)

    def convert_blocks_to_bboxes(self, blocks):
        working_block_idx = -1
        for block_idx, block in enumerate(blocks):
            if block["block_type"] != "table_row" and not block.get(
                table_parser.row_group_key,
                False,
            ):
                working_block_idx = block_idx
                self.bboxes[block_idx] = {
                    "file_idx": self.file_idx,
                    "block_idx": block_idx,
                    "page_idx": block["page_idx"],
                    "block_type": block["block_type"],
                    "bbox": [
                        block["box_style"][1],
                        block["box_style"][0],
                        block["box_style"][1] + block["box_style"][3],
                        block["box_style"][0] + block["box_style"][4],
                    ],
                    "audited": block.get("audited", False),
                }
                continue

            if (
                # check if start of table, add new bbox for table
                block.get("is_table_start", False)
                or
                # not start of table but table breaks to next page, add new bbox for table
                (
                    len(self.bboxes) > 0
                    and working_block_idx > 0
                    and self.bboxes[working_block_idx]["page_idx"] != block["page_idx"]
                )
            ):
                working_block_idx = block_idx
                self.bboxes[block_idx] = {
                    "file_idx": self.file_idx,
                    "page_idx": block["page_idx"],
                    "block_idx": block_idx,
                    "block_type": "table",
                    "bbox": [
                        block["box_style"][1],
                        block["box_style"][0],
                        block["box_style"][1] + block["box_style"][3],
                        block["box_style"][0] + block["box_style"][4],
                    ],
                    "audited": block.get("audited", False),
                }
            # expand bbox x2y2 based on table right and bottom
            else:
                self.bboxes[working_block_idx]["bbox"] = [
                    min(
                        self.bboxes[working_block_idx]["bbox"][0],
                        block["box_style"][1],
                    ),
                    min(
                        self.bboxes[working_block_idx]["bbox"][1],
                        block["box_style"][0],
                    ),
                    max(
                        self.bboxes[working_block_idx]["bbox"][2],
                        block["box_style"][1] + block["box_style"][3],
                    ),
                    max(
                        self.bboxes[working_block_idx]["bbox"][3],
                        block["box_style"][0] + block["box_style"][4],
                    ),
                ]

        self.save_bbox_to_db()

    def save_bbox_to_db(self):
        nosql_db.save_bbox_bulk(
            file_idx=self.file_idx,
            bboxes=list(self.bboxes.values()),
        )

    def convert_tika_html_to_features(self):
        if not self.tika_html:
            return

        def get_style_kv(style_str):
            parts = style_str.split(";")
            input_style = {}
            for part in parts:
                kv = part.split(":")
                if len(kv) == 2:
                    input_style[kv[0].strip()] = kv[1].strip()
            return input_style

        def get_font_weight(font_weight):
            if font_weight.isdigit():
                return int(font_weight)
            elif font_weight == "normal":
                return 400
            elif font_weight == "bold":
                return 600
            elif font_weight == "bolder":
                return 800
            elif font_weight == "lighter":
                return 300
            else:
                return 400

        def get_font_stats(page):
            font_family_list = []
            font_size_list = []
            largest_font = 0
            smallest_font = 999
            for p in page:
                input_style = get_style_kv(p["style"])
                font_family = input_style["font-family"]
                font_size = float(input_style["font-size"].split("px")[0])
                font_family_list.append(font_family)
                font_size_list.append(font_size)

                largest_font = max(largest_font, font_size)
                smallest_font = min(smallest_font, font_size)
            return {
                "mode_family": max(set(font_family_list), key=font_family_list.count)
                if font_family_list
                else None,
                "largest_size": largest_font,
                "smallest_size": smallest_font,
                "mode_size": max(set(font_size_list), key=font_size_list.count)
                if font_size_list
                else None,
            }

        def extract_style_features(input_style, page_stats):
            # [fontsize, fontweight, font style, font family]
            features = np.zeros(4)

            font_size = float(input_style["font-size"].split("px")[0])
            features[0] = font_size - page_stats["mode_size"]

            font_weight = get_font_weight(input_style["font-weight"])
            features[1] = font_weight / 400

            if input_style["font-style"] != "normal":
                features[2] = 1

            font_family = input_style["font-family"]
            if page_stats["mode_family"] == font_family:
                features[3] = 1

            return features

        def extract_token_features(text):
            # [istitle, isupper, ends with puncuation, start with list, ratio of isdigit, ratio of isalpha, isstopword]
            features = np.zeros(7)

            if text.istitle():
                features[0] = 1
            if text.isupper():
                features[1] = 1
            if text[-1] in PUNCTUATION:
                features[2] = 1
            if text[0] in LIST_CHAR:
                features[3] = 1

            features[4] = sum(c.isdigit() for c in text) / len(text)
            features[5] = sum(c.isalpha() for c in text) / len(text)

            if text.lower() in STOP_WORDS:
                features[6] = 1

            return features

        with open(self.tika_html) as f:
            soup = BeautifulSoup(f, "html.parser")

            pages = soup.findAll("div", class_=lambda x: x not in ["annotation"])

        file_metadata = {
            "file_idx": self.file_idx,
            "workspace_idx": "",
            "features": [
                "font_size",
                "font_weight",
                "font_style",
                "font_family",
                "isCapitalized",
                "isUpper",
                "endsPunctuaion",
                "startsList",
                "isdigit",
                "isalpha",
                "isStopWord",
            ],
        }
        file_data = []

        for page in pages:
            all_p = page.find_all("p")
            page_data = []
            page_font_stats = get_font_stats(all_p)
            for p in all_p:
                input_style = get_style_kv(p["style"])
                style_features = extract_style_features(input_style, page_font_stats)

                word_start_pos = ast.literal_eval(input_style["word-start-positions"])
                word_end_pos = ast.literal_eval(input_style["word-end-positions"])
                # word_fonts = input_style["word-fonts"][2:-2].split("), (")
                font_size = float(input_style["font-size"].split("px")[0])
                for text, start, end in zip(
                    p.text.split(),
                    word_start_pos,
                    word_end_pos,
                ):
                    x1, y1, x2, y2 = (
                        int(start[0]),
                        int(start[1]) - int(font_size),
                        int(end[0]),
                        int(end[1]),
                    )
                    token_features = extract_token_features(text)
                    data = {
                        "position": {
                            "xyxy": [x1, y1, x2, y2],
                        },
                        "features": np.concatenate(
                            (style_features, token_features),
                        ).tolist(),
                    }
                    page_data.append(data)

            file_data.append(page_data)

        output = {"metadata": file_metadata, "data": file_data}
        return output
