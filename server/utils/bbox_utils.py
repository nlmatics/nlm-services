import json
import os
import tempfile
from functools import lru_cache

import numpy as np
from nlm_utils.storage import file_storage


@lru_cache(maxsize=128)
def load_json(file_idx):
    file_handler, filepath = tempfile.mkstemp()
    os.close(file_handler)

    file_storage.download_document(
        f"bbox/features/{file_idx}.json",
        dest_file_location=filepath,
    )
    with open(filepath) as f:
        data = json.load(f)

    os.unlink(filepath)
    return data


def ensure_bbox(bbox):
    x1, y1, x2, y2 = bbox
    return x1 < x2 and y1 < y2


def check_overlap(bbox1, bbox2):
    assert ensure_bbox(bbox1["bbox"])
    assert ensure_bbox(bbox2["bbox"])

    x1, y1, x2, y2 = bbox1["bbox"]
    _x1, _y1, _x2, _y2 = bbox2["bbox"]
    return max(x1, _x1) < min(x2, _x2) and max(y1, _y1) < min(y2, _y2)


def align_bbox_to_features(bbox):

    data = load_json(bbox["file_idx"])
    # HWC.
    # We hard code the padding to 0, thus image must in square (H:1344,W:1344 by default)
    features = np.zeros(
        (1344, 1344, 1),
        dtype=np.float32,
    )

    # make features, HWF
    for token in data["data"][bbox["page_idx"]]:
        # x1, y1, x2, y2 = xyxy_to_training(token["position"]["xyxy"])
        x1, y1, x2, y2 = (round(x) for x in token["position"]["xyxy"])

        # token position mask
        features[y1:y2, x1:x2, 0] = 1

    return correct_box(features, bbox["bbox"])


def correct_box(img, xyxy):
    def _correct_box(img, ax, fixed_ax_1, fixed_ax_2, pad_1):
        limit = img.shape[1]
        expanded = shrinked = ax
        while True:
            if (
                0 < expanded < limit + pad_1
                and np.count_nonzero(img[fixed_ax_1:fixed_ax_2, expanded - pad_1]) == 0
                and np.count_nonzero(img[fixed_ax_1:fixed_ax_2, expanded]) != 0
            ):
                return expanded
            if (
                0 < shrinked < limit + pad_1
                and np.count_nonzero(img[fixed_ax_1:fixed_ax_2, shrinked - pad_1]) == 0
                and np.count_nonzero(img[fixed_ax_1:fixed_ax_2, shrinked]) != 0
            ):
                return shrinked
            expanded -= 1
            shrinked += 1

            if expanded <= 0 and shrinked >= limit:
                raise ValueError("Can not find the box in the image")

    if len(img.shape) == 3:
        img = img[:, :, 0]

    assert len(img.shape) == 2

    x1, y1, x2, y2 = (int(x) for x in xyxy)

    x1 = _correct_box(img, x1, y1, y2, 1)

    x2 = _correct_box(img, x2, y1, y2, -1)

    y1 = _correct_box(img.transpose(1, 0), y1, x1, x2, 1)

    y2 = _correct_box(img.transpose(1, 0), y2, x1, x2, -1)

    return x1, y1, x2, y2
