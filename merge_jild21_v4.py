#!/usr/bin/env python3
"""Merge Fatawa Razviya Jild 21 easy-Urdu tashreeh v4 chunks into jild-21.json.

Reads the 12 leaf chunk files listed in chunks-manifest-v4-jild21.json,
concatenates them in order 01 -> 02 -> ... -> 12, JOINS the 3 simple
duplicate-id pairs (masala-166, 180, 203-206) and the 1 cross-id
group (masala-172-175: head + shifa-ul-walah tail) into single items,
STABLE SORTS by start_page (mirroring the v3 merge), derives
start_page = min(pages) where the field is absent (0 derived: all
chunks carry start_page), normalises page markers, validates, and
writes {"masail": [...]} to jild-21.json.

ORDER -- chunk order is the order, then a stable sort by start_page:
  The v3 merged file ~/workspace/fatawa-app/content/tashreeh/jild-21.json
  has ID order == stable sort by start_page of the concatenation
  (verified locally by the coordinator: the v3 order IS the stable
  sort, including its tied start_pages). Do NOT sort or reorder before
  the joins. Within each chunk, items keep file order.
  CHUNK_ORDER is an explicit 12-leaf list.

JOINS: parts are found BY ID anywhere in the list. Parts must be in
  ascending page order. Sawal modes: "first" (166, 180, 203-206 — the
  single non-empty sawal, in the first part), "head_only" (172-175 —
  the head's sawal; the tail has an empty sawal). Titles: the joined
  title is the FIRST/HEAD part's title, asserted against the embedded
  merged-v3 title (V3_TITLES). title_must_match is deliberately NOT
  used: the two halves' titles differ by design in the v3 chunks.
  masala-166 [339-350, 351-395], masala-180 [479-480, 481-484],
  masala-203-206 [619-620, 621-622].
CROSS-ID JOINS (1):
  masala-172-175: c07 head jild21-masala-172-175 [425-440] (sawal) +
    c08 jild21-shifa-ul-walah [441-458] (sawal empty) -> ONE item
    jild21-masala-172-175; title from the HEAD (== merged-v3 title,
    'مسئلہ ۱۷۲ تا ۱۷۵: حضور ﷺ و براق و جبرئیل کی تصویریں اور نقشہ
    روضہ'); the rename is effected by the combine. Net -1 item.
  After the joins there are 181 items and 181 unique ids — the
  duplicate-id assertion runs AFTER the joins, not before.
  NO renames at merge: the 5 print-error renames (masala-197,
  masala-82-85, masala-89, masala-213, masala-253) were already
  applied in the chunk files; the merge only asserts they are
  present in the leaves and in merged v3. NO folds. All chunks carry
  start_page == min(pages) (0 derived). p.151 is shared by
  masala-18 [150-151] and masala-19-22 [151-153] by design.
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = "chunks-manifest-v4-jild21.json"

# Concatenation order: chunk files 01 -> 18, in order.
CHUNK_ORDER = ["jild-21-chunk-01.json", "jild-21-chunk-02.json", "jild-21-chunk-03.json", "jild-21-chunk-04.json", "jild-21-chunk-05.json", "jild-21-chunk-06.json", "jild-21-chunk-07.json", "jild-21-chunk-08.json", "jild-21-chunk-09.json", "jild-21-chunk-10.json", "jild-21-chunk-11.json", "jild-21-chunk-12.json"]

EXPECTED_ITEMS = 181
EXPECTED_PAGES = 577
EXPECTED_MIN_PAGE = 5
EXPECTED_MAX_PAGE = 674
EXPECTED_VOLUME_IDS = {43}
EXPECTED_FIRST_ID = "jild21-pesh-lafz"
EXPECTED_LAST_ID = "jild21-khatima-note"

# v3 ID sequence, verbatim from content/tashreeh/jild-21.json.
V3_IDS = ["jild21-pesh-lafz", "jild21-masala-1", "jild21-masala-2", "jild21-masala-3", "jild21-masala-4", "jild21-masala-5", "jild21-masala-6", "jild21-masala-7", "jild21-masala-8", "jild21-masala-9", "jild21-masala-10", "jild21-masala-11", "jild21-masala-12", "jild21-masala-13", "jild21-masala-14", "jild21-masala-15", "jild21-masala-16", "jild21-masala-17", "jild21-masala-18", "jild21-masala-19-22", "jild21-masala-23", "jild21-masala-24", "jild21-masala-25", "jild21-masala-26-29", "jild21-masala-30", "jild21-masala-31", "jild21-masala-32", "jild21-masala-33-34", "jild21-masala-35", "jild21-masala-36-41", "jild21-masala-42", "jild21-masala-43", "jild21-masala-44-52", "jild21-masala-53", "jild21-masala-54", "jild21-masala-55", "jild21-masala-56-58", "jild21-masala-59", "jild21-masala-60-62", "jild21-masala-63", "jild21-masala-64", "jild21-masala-65", "jild21-masala-66", "jild21-masala-67", "jild21-masala-68-69", "jild21-masala-70", "jild21-masala-71-73", "jild21-masala-74", "jild21-masala-75", "jild21-masala-76", "jild21-masala-77", "jild21-masala-78", "jild21-masala-79", "jild21-masala-80", "jild21-masala-81", "jild21-masala-82-85", "jild21-masala-86", "jild21-masala-87", "jild21-masala-88", "jild21-masala-89", "jild21-masala-90", "jild21-masala-91", "jild21-masala-92-94", "jild21-masala-95-97", "jild21-masala-98", "jild21-masala-99", "jild21-masala-100-102", "jild21-masala-103", "jild21-masala-104", "jild21-masala-105-124", "jild21-masala-125", "jild21-masala-126-136", "jild21-masala-137", "jild21-masala-138", "jild21-masala-139", "jild21-masala-140", "jild21-masala-141-143", "jild21-masala-144", "jild21-masala-145-149", "jild21-masala-150", "jild21-masala-151", "jild21-masala-152", "jild21-masala-153-154", "jild21-masala-155-158", "jild21-masala-159", "jild21-masala-160-161", "jild21-masala-162-164", "jild21-masala-165", "jild21-masala-166", "jild21-masala-167", "jild21-masala-168", "jild21-masala-169", "jild21-masala-170", "jild21-masala-171", "jild21-masala-172-175", "jild21-masala-176", "jild21-masala-177", "jild21-masala-178", "jild21-masala-179", "jild21-masala-180", "jild21-masala-181", "jild21-masala-182", "jild21-masala-183", "jild21-masala-184", "jild21-masala-185", "jild21-masala-186", "jild21-masala-187", "jild21-masala-188", "jild21-masala-189", "jild21-masala-190", "jild21-masala-191-192", "jild21-masala-193", "jild21-masala-194", "jild21-masala-195", "jild21-masala-196", "jild21-masala-197", "jild21-masala-198", "jild21-masala-199", "jild21-masala-200", "jild21-masala-201", "jild21-masala-202", "jild21-masala-203-206", "jild21-masala-207", "jild21-masala-208", "jild21-masala-209", "jild21-masala-210", "jild21-masala-211", "jild21-masala-212", "jild21-masala-213", "jild21-masala-214", "jild21-masala-215-216", "jild21-masala-217-223", "jild21-masala-224", "jild21-masala-225", "jild21-masala-226-228", "jild21-masala-229", "jild21-masala-230", "jild21-masala-231", "jild21-masala-232", "jild21-masala-233-237", "jild21-masala-238", "jild21-masala-239", "jild21-masala-240", "jild21-masala-241", "jild21-masala-242", "jild21-masala-243", "jild21-masala-244-246", "jild21-masala-247-248", "jild21-masala-249-251", "jild21-masala-252", "jild21-masala-253", "jild21-masala-254", "jild21-masala-255", "jild21-masala-256", "jild21-masala-257", "jild21-masala-258", "jild21-masala-259", "jild21-masala-260-261", "jild21-masala-262", "jild21-masala-263-265", "jild21-masala-266", "jild21-masala-267", "jild21-masala-268", "jild21-masala-269", "jild21-masala-270", "jild21-masala-271-272", "jild21-masala-273", "jild21-masala-274", "jild21-masala-275", "jild21-masala-276-277", "jild21-masala-278", "jild21-masala-279", "jild21-masala-280-282", "jild21-masala-283", "jild21-masala-284", "jild21-masala-285-286", "jild21-masala-287", "jild21-masala-288", "jild21-masala-289-290", "jild21-masala-291", "jild21-khatima-note"]

# v3 pages per item id, verbatim from content/tashreeh/jild-21.json.
V3_PAGES = {
    "jild21-pesh-lafz": [5, 6, 7, 8, 9, 10],
    "jild21-masala-1": [101, 102, 103, 104, 105],
    "jild21-masala-2": [105, 106, 107, 108, 109, 110, 111],
    "jild21-masala-3": [111, 112],
    "jild21-masala-4": [112, 113],
    "jild21-masala-5": [114, 115, 116],
    "jild21-masala-6": [116, 117, 118, 119],
    "jild21-masala-7": [119, 120],
    "jild21-masala-8": [120, 121],
    "jild21-masala-9": [121, 122, 123],
    "jild21-masala-10": [123, 124, 125, 126],
    "jild21-masala-11": [126, 127, 128, 129],
    "jild21-masala-12": [130],
    "jild21-masala-13": [130],
    "jild21-masala-14": [131, 132, 133, 134, 135],
    "jild21-masala-15": [135, 136],
    "jild21-masala-16": [136, 137, 138, 139],
    "jild21-masala-17": [139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150],
    "jild21-masala-18": [150, 151],
    "jild21-masala-19-22": [151, 152, 153],
    "jild21-masala-23": [153],
    "jild21-masala-24": [153, 154],
    "jild21-masala-25": [154, 155],
    "jild21-masala-26-29": [155, 156],
    "jild21-masala-30": [156, 157],
    "jild21-masala-31": [157, 158, 159, 160, 161],
    "jild21-masala-32": [161],
    "jild21-masala-33-34": [162, 163],
    "jild21-masala-35": [163, 164],
    "jild21-masala-36-41": [165, 166, 167, 168],
    "jild21-masala-42": [169],
    "jild21-masala-43": [170, 171],
    "jild21-masala-44-52": [171, 172, 173, 174],
    "jild21-masala-53": [174, 175, 176, 177, 178],
    "jild21-masala-54": [178, 179, 180, 181],
    "jild21-masala-55": [182, 183],
    "jild21-masala-56-58": [183, 184, 185],
    "jild21-masala-59": [185, 186, 187],
    "jild21-masala-60-62": [187, 188, 189],
    "jild21-masala-63": [189],
    "jild21-masala-64": [189, 190, 191, 192, 193],
    "jild21-masala-65": [194, 195],
    "jild21-masala-66": [195, 196, 197, 198, 199, 200],
    "jild21-masala-67": [201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215],
    "jild21-masala-68-69": [215, 216],
    "jild21-masala-70": [216],
    "jild21-masala-71-73": [216, 217, 218, 219],
    "jild21-masala-74": [219, 220],
    "jild21-masala-75": [220],
    "jild21-masala-76": [220, 221, 222],
    "jild21-masala-77": [222],
    "jild21-masala-78": [222, 223],
    "jild21-masala-79": [223],
    "jild21-masala-80": [223, 224, 225],
    "jild21-masala-81": [225],
    "jild21-masala-82-85": [226, 227, 228, 229, 230],
    "jild21-masala-86": [231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245],
    "jild21-masala-87": [246],
    "jild21-masala-88": [246],
    "jild21-masala-89": [247],
    "jild21-masala-90": [247, 248],
    "jild21-masala-91": [248],
    "jild21-masala-92-94": [248, 249, 250],
    "jild21-masala-95-97": [251, 252, 253],
    "jild21-masala-98": [254, 255],
    "jild21-masala-99": [255, 256, 257],
    "jild21-masala-100-102": [257, 258],
    "jild21-masala-103": [259, 260],
    "jild21-masala-104": [260, 261],
    "jild21-masala-105-124": [261, 262, 263, 264, 265, 266, 267, 268],
    "jild21-masala-125": [268, 269],
    "jild21-masala-126-136": [269, 270, 271, 272, 273, 274],
    "jild21-masala-137": [274, 275, 276],
    "jild21-masala-138": [276, 277],
    "jild21-masala-139": [277, 278],
    "jild21-masala-140": [278, 279],
    "jild21-masala-141-143": [279, 280, 281],
    "jild21-masala-144": [281, 282],
    "jild21-masala-145-149": [282, 283, 284, 285, 286],
    "jild21-masala-150": [287],
    "jild21-masala-151": [288, 289, 290],
    "jild21-masala-152": [290, 291, 292, 293, 294],
    "jild21-masala-153-154": [294],
    "jild21-masala-155-158": [294, 295, 296, 297],
    "jild21-masala-159": [297],
    "jild21-masala-160-161": [298],
    "jild21-masala-162-164": [298, 299],
    "jild21-masala-165": [301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337],
    "jild21-masala-166": [339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395],
    "jild21-masala-167": [397, 398, 399, 400, 401, 402, 403],
    "jild21-masala-168": [403, 404, 405, 406, 407, 408, 409, 410, 411, 412],
    "jild21-masala-169": [412, 413, 414],
    "jild21-masala-170": [414, 415, 416, 417, 418, 419, 420],
    "jild21-masala-171": [420, 421, 422, 423, 424],
    "jild21-masala-172-175": [425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458],
    "jild21-masala-176": [459, 460],
    "jild21-masala-177": [461, 462, 463, 464, 465, 466, 467],
    "jild21-masala-178": [467, 468, 469, 470, 471, 472, 473, 474],
    "jild21-masala-179": [474, 475, 476, 477, 478, 479],
    "jild21-masala-180": [479, 480, 481, 482, 483, 484],
    "jild21-masala-181": [484, 485, 486, 487, 488, 489, 490, 491],
    "jild21-masala-182": [491, 492],
    "jild21-masala-183": [492, 493, 494],
    "jild21-masala-184": [494, 495],
    "jild21-masala-185": [496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520],
    "jild21-masala-186": [521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568],
    "jild21-masala-187": [569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596],
    "jild21-masala-188": [596, 597, 598, 599],
    "jild21-masala-189": [599],
    "jild21-masala-190": [599, 600, 601, 602],
    "jild21-masala-191-192": [602, 603],
    "jild21-masala-193": [603, 604],
    "jild21-masala-194": [604, 605],
    "jild21-masala-195": [605],
    "jild21-masala-196": [605, 606],
    "jild21-masala-197": [606],
    "jild21-masala-198": [607, 608],
    "jild21-masala-199": [608, 609],
    "jild21-masala-200": [609, 610, 611, 612],
    "jild21-masala-201": [612, 613, 614, 615, 616],
    "jild21-masala-202": [616, 617, 618, 619],
    "jild21-masala-203-206": [619, 620, 621, 622],
    "jild21-masala-207": [622],
    "jild21-masala-208": [622, 623, 624, 625, 626, 627],
    "jild21-masala-209": [627, 628],
    "jild21-masala-210": [628, 629, 630, 631],
    "jild21-masala-211": [631, 632],
    "jild21-masala-212": [632, 633],
    "jild21-masala-213": [633, 634],
    "jild21-masala-214": [634],
    "jild21-masala-215-216": [634, 635],
    "jild21-masala-217-223": [635, 636, 637, 638],
    "jild21-masala-224": [638],
    "jild21-masala-225": [638, 639, 640],
    "jild21-masala-226-228": [640, 641, 642],
    "jild21-masala-229": [642],
    "jild21-masala-230": [642],
    "jild21-masala-231": [642],
    "jild21-masala-232": [642, 643],
    "jild21-masala-233-237": [643, 644],
    "jild21-masala-238": [644, 645],
    "jild21-masala-239": [645, 646],
    "jild21-masala-240": [646, 647],
    "jild21-masala-241": [647, 648],
    "jild21-masala-242": [648],
    "jild21-masala-243": [648, 649, 650],
    "jild21-masala-244-246": [650, 651],
    "jild21-masala-247-248": [651, 652],
    "jild21-masala-249-251": [652, 653],
    "jild21-masala-252": [653, 654],
    "jild21-masala-253": [654, 655],
    "jild21-masala-254": [655, 656],
    "jild21-masala-255": [656],
    "jild21-masala-256": [656, 657],
    "jild21-masala-257": [657],
    "jild21-masala-258": [657, 658],
    "jild21-masala-259": [658],
    "jild21-masala-260-261": [658, 659],
    "jild21-masala-262": [659, 660],
    "jild21-masala-263-265": [660, 661],
    "jild21-masala-266": [661, 662],
    "jild21-masala-267": [662],
    "jild21-masala-268": [662, 663],
    "jild21-masala-269": [663],
    "jild21-masala-270": [663],
    "jild21-masala-271-272": [663, 664],
    "jild21-masala-273": [664],
    "jild21-masala-274": [664, 665, 666],
    "jild21-masala-275": [666],
    "jild21-masala-276-277": [666, 667],
    "jild21-masala-278": [667],
    "jild21-masala-279": [667, 668],
    "jild21-masala-280-282": [668],
    "jild21-masala-283": [668, 669],
    "jild21-masala-284": [669],
    "jild21-masala-285-286": [669, 670],
    "jild21-masala-287": [670],
    "jild21-masala-288": [670],
    "jild21-masala-289-290": [670, 671, 672, 673, 674],
    "jild21-masala-291": [674],
    "jild21-khatima-note": [674],
}

# The Urdu word for "page" is safha; the canonical marker is [safha N].
# Lenticular brackets are U+3010 (open) and U+3011 (close); the short
# form is "[safha-initial N]" with the single-letter abbreviation.
# All three patterns are built from escapes so this file never contains
# a literal lenticular-bracket pair.
SAFHA = "\u0635\u0641\u062d\u06c1"  # the Urdu word for page
RE_LENTICULAR = re.compile("\u3010" + SAFHA + " (\\d+)\u3011")
RE_SHORT = re.compile("\\[\u0635 (\\d+)\\]")
RE_FULL = re.compile("\\[" + SAFHA + " \\d+\\]")
LENTICULAR_PREFIX = "\u3010" + SAFHA  # for the "none remain" check
SHORT_PREFIX_RE = re.compile("\\[\u0635 \\d+\\]")

# Canonical output key order for every item (the JSON contract).
ITEM_KEYS = ["id", "title", "volume_id", "pages", "start_page",
             "tashreeh_sawal", "tashreeh_jawab", "notes"]


def normalise_markers(text):
    """Normalise page markers in one string. Returns (new_text, counts)."""
    counts = {"lenticular": 0, "short": 0, "full_before": 0}
    counts["full_before"] = len(RE_FULL.findall(text))
    text, n1 = RE_LENTICULAR.subn("[" + SAFHA + " \\1]", text)
    counts["lenticular"] = n1
    text, n2 = RE_SHORT.subn("[" + SAFHA + " \\1]", text)
    counts["short"] = n2
    return text, counts


def normalise_item(item):
    """Normalise markers in every string field of one item (recursively
    through lists/dicts). Returns per-form counts for this item."""
    totals = {"lenticular": 0, "short": 0, "full_before": 0}

    def walk(value):
        if isinstance(value, str):
            new, c = normalise_markers(value)
            for k in totals:
                totals[k] += c[k]
            return new
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in list(item.keys()):
        item[key] = walk(item[key])
    return totals


def main():
    assert len(V3_IDS) == EXPECTED_ITEMS, "embedded V3_IDS length mismatch"
    assert set(V3_PAGES) == set(V3_IDS), "embedded V3_PAGES keys != V3_IDS"
    assert sum(len(v) for v in V3_PAGES.values()) >= EXPECTED_PAGES

    manifest = json.load(open(os.path.join(BASE, MANIFEST), encoding="utf-8"))
    entries = manifest["chunks"]
    assert len(entries) == 12, f"expected 12 manifest entries, got {len(entries)}"
    assert all(e.get("status") == "done" for e in entries), "not all chunks done"
    by_file = {e["file"]: e for e in entries}
    assert set(by_file) == set(CHUNK_ORDER), (
        f"manifest files != CHUNK_ORDER: "
        f"missing={set(CHUNK_ORDER) - set(by_file)} "
        f"extra={set(by_file) - set(CHUNK_ORDER)}"
    )

    all_items = []
    derived = []
    for fname in CHUNK_ORDER:
        path = os.path.join(BASE, fname)
        d = json.load(open(path, encoding="utf-8"))
        items = d["masail"]
        entry = by_file[fname]
        assert len(items) == entry["items"], (
            f"{fname}: {len(items)} items, manifest says {entry['items']}"
        )
        # Marker normalisation, with before/after counts per chunk.
        before = {"lenticular": 0, "short": 0, "full_before": 0}
        for it in items:
            c = normalise_item(it)
            for k in before:
                before[k] += c[k]
        after_full = sum(
            len(RE_FULL.findall(json.dumps(it, ensure_ascii=False)))
            for it in items
        )
        print(
            f"{fname}: {len(items)} items | markers before: "
            f"lenticular={before['lenticular']} short={before['short']} "
            f"full={before['full_before']} | after: full={after_full}"
        )
        assert after_full == (
            before["lenticular"] + before["short"] + before["full_before"]
        ), f"{fname}: marker count changed by normalisation!"
        # Derive start_page = min(pages) where absent (none expected: all chunks carry start_page).
        for it in items:
            if "start_page" not in it:
                it["start_page"] = min(it["pages"])
                derived.append(it["id"])
        all_items.extend(items)

        # SIMPLE JOINS (3): parts found BY ID anywhere. Sawal mode:
    # "first" (the single non-empty sawal, in the first part).
    # CROSS-ID JOINS (1): 172-175 (head + shifa-ul-walah tail).
    # Sawal = head.

    def join_consecutive(item_id, expected_parts, seam_checks, sawal_mode="first", title_suffix=None):
        idxs = [i for i, it in enumerate(all_items) if it["id"] == item_id]
        assert len(idxs) == expected_parts, (
            f"{item_id}: expected {expected_parts} parts, found {len(idxs)}"
        )
        parts = [all_items[i] for i in idxs]
        assert [min(p["pages"]) for p in parts] == sorted(min(p["pages"]) for p in parts), (
            f"{item_id}: parts not in page order"
        )
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{item_id}: volume mismatch {vols}"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{item_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{item_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        if sawal_mode == "first":
            sawals = [p.get("tashreeh_sawal") for p in parts if p.get("tashreeh_sawal")]
            assert len(sawals) <= 1, f"{item_id}: {len(sawals)} non-empty sawals"
            if sawals:
                assert parts[0].get("tashreeh_sawal"), f"{item_id}: non-empty sawal not in first part"
            sawal = sawals[0] if sawals else ""
        else:
            raise AssertionError(f"{item_id}: unknown sawal_mode {sawal_mode}")
        title = parts[0]["title"]
        if title_suffix:
            assert title.endswith(title_suffix), f"{item_id}: first-part title lacks suffix {title_suffix!r}"
            assert all(p["title"].endswith(title_suffix) for p in parts), f"{item_id}: a part title lacks the suffix"
            title = title[: -len(title_suffix)]
        assert title == V3_TITLES[item_id], f"{item_id}: joined title != v3 title"
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[item_id], f"{item_id}: joined pages != v3"
        joined = {
            "id": item_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {item_id}: {expected_parts} parts -> 1 item, {len(pages)} pages")

    def join_cross_id(result_id, head_id, tail_ids, sawal_mode, seam_checks):
        seq = [head_id] + tail_ids
        idxs = [i for i, it in enumerate(all_items) if it["id"] in seq]
        assert [all_items[i]["id"] for i in idxs] == seq, (
            f"{result_id}: cross-id order mismatch"
        )
        parts = [all_items[i] for i in idxs]
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{result_id}: volume mismatch {vols}"
        if sawal_mode == "head_only":
            assert parts[0].get("tashreeh_sawal"), f"{result_id}: head sawal empty"
            assert all(not p.get("tashreeh_sawal") for p in parts[1:]), (
                f"{result_id}: a tail sawal is non-empty"
            )
            sawal = parts[0]["tashreeh_sawal"]
        else:
            raise AssertionError(f"{result_id}: unknown sawal_mode {sawal_mode}")
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{result_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{result_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[result_id], f"{result_id}: joined pages != v3"
        title = parts[0]["title"]
        assert title == V3_TITLES[result_id], f"{result_id}: joined title != v3 title"
        joined = {
            "id": result_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {result_id}: {1 + len(tail_ids)} parts -> 1 item, {len(pages)} pages")

    V3_TITLES = {
        "jild21-masala-166": "رسالہ فقہ شہنشاہ — لفظ «شہنشاہ» اور قلوب پر محبوبانِ خدا کا قبضہ (پہلا حصہ)",
        "jild21-masala-180": "مسئلہ ۱۸۰: بے پیر شخص کے بارے میں زید کے اقوال — «من لا شیخ لہ فشیخہ الشیطان» (نامکمل — جاری ہے)",
        "jild21-masala-203-206": "مسئلہ ۲۰۳ تا ۲۰۶: انگریزی مکھن، دودھ، برش اور انگریز کے ساتھ کھانا (پہلا حصہ)",
        "jild21-masala-172-175": "مسئلہ ۱۷۲ تا ۱۷۵: حضور ﷺ و براق و جبرئیل کی تصویریں اور نقشہ روضہ",
    }

    join_consecutive(
        "jild21-masala-166", 2,
        [
            (0, "end", " — عتاب کا مطلب ہے ڈانٹ، سزا — اور اس مقام پر جوابِ سوالِ اول کا یہ حصہ جاری ہے۔"),
            (1, "start", "یہاں سے پہلے سوال کے جواب کی بحث آگے بڑھتی ہے۔ اعلیٰ حضرت فرماتے ہیں کہ اس بات ک"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild21-masala-180", 2,
        [
            (0, "end", " دن ہم ہر گروہ کو اس کے امام کے ساتھ بلائیں گے، امام کا مطلب ہے پیشوا اور رہنما۔"),
            (1, "start", "جواب کا یہ آخری حصہ اسی بات کو آگے بڑھاتا ہے کہ جس شخص نے ائمہ ہدیٰ کو اپنا مرشد"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild21-masala-203-206", 2,
        [
            (0, "end", "ور کا یہاں تک کہ مردار کا بھی پاک ہے — اور جواب یہیں صفحہ ۶۲۰ کے آخر پر جاری ہے۔"),
            (1, "start", "جواب کے اس بقیہ حصے میں پہلے تین سوالوں کے بارے میں مزید بتایا گیا ہے کہ سینگ، ی"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_cross_id(
        "jild21-masala-172-175",
        "jild21-masala-172-175",
        ["jild21-shifa-ul-walah"],
        "head_only",
        [
            (0, "end", " ۴۴۰ پر اصل متن کا یہ حصہ ختم ہوتا ہے اور جواب کا بقیہ حصہ اگلے حصے میں جاری ہے۔"),
            (1, "start", "یہ رسالے کا آخری حصہ ہے۔ اس میں اعلیٰ حضرت رحمۃ اللہ علیہ قبورِ مقدسہ کی ترتیب ا"),
        ],
    )
    assert len(all_items) == len({it["id"] for it in all_items}), (
        f"DUPLICATE IDS after joins: "
        f"{[i for i in [it['id'] for it in all_items] if [it['id'] for it in all_items].count(i) > 1]}"
    )

    print(f"after joins: {len(all_items)} items (185 concatenated)")
    # NO renames at merge: the 5 print-error renames were already
    # applied in the chunk files; assert them present.
    assert "jild21-masala-197" in [it["id"] for it in all_items], f"rename jild21-masala-197 missing from merged items"
    assert "jild21-masala-82-85" in [it["id"] for it in all_items], f"rename jild21-masala-82-85 missing from merged items"
    assert "jild21-masala-89" in [it["id"] for it in all_items], f"rename jild21-masala-89 missing from merged items"
    assert "jild21-masala-213" in [it["id"] for it in all_items], f"rename jild21-masala-213 missing from merged items"
    assert "jild21-masala-253" in [it["id"] for it in all_items], f"rename jild21-masala-253 missing from merged items"
    print("renames applied: [] (5 print-error renames already in the leaves)")
    # STABLE SORT by start_page, mirroring the v3 merge.
    all_items.sort(key=lambda it: it["start_page"])
    print(f"sorted by start_page: {len(all_items)} items")
    print(f"start_page derived for {len(derived)} items (expected 0: all chunks carry start_page)")
    assert len(all_items) == EXPECTED_ITEMS, (
        f"expected {EXPECTED_ITEMS} items, got {len(all_items)}"
    )

    # Rebuild each item in the canonical key order (the JSON contract).
    canonical = []
    for it in all_items:
        assert set(it.keys()) == set(ITEM_KEYS), (
            f"{it.get('id')}: keys {sorted(it.keys())} != {sorted(ITEM_KEYS)}"
        )
        canonical.append({k: it[k] for k in ITEM_KEYS})
    all_items = canonical

    # Validation.
    ids = [it["id"] for it in all_items]
    assert ids == V3_IDS, (
        f"ID sequence != v3: first diff at "
        f"{next((i for i, (a, b) in enumerate(zip(ids, V3_IDS)) if a != b), min(len(ids), len(V3_IDS)))}"
    )
    assert len(ids) == len(set(ids)), (
        f"DUPLICATE IDS: {[i for i in ids if ids.count(i) > 1]}"
    )
    empty = [it["id"] for it in all_items if not it.get("tashreeh_jawab")]
    assert not empty, f"EMPTY JAWAB: {empty}"
    vols = set(it.get("volume_id") for it in all_items)
    assert vols == EXPECTED_VOLUME_IDS, f"volume_ids {vols} != {EXPECTED_VOLUME_IDS}"
    pages = set()
    for it in all_items:
        assert it["pages"] == V3_PAGES[it["id"]], (
            f"{it['id']}: pages {it['pages']} != v3 {V3_PAGES[it['id']]}"
        )
        pages.update(it["pages"])
    assert len(pages) == EXPECTED_PAGES, (
        f"expected {EXPECTED_PAGES} unique pages, got {len(pages)}"
    )
    assert min(pages) == EXPECTED_MIN_PAGE, f"min page {min(pages)}"
    assert max(pages) == EXPECTED_MAX_PAGE, f"max page {max(pages)}"
    exceptions = {
        it["id"] for it in all_items if it["start_page"] != min(it["pages"])
    }
    assert not exceptions, f"start_page != min(pages): {sorted(exceptions)}"
    assert ids[0] == EXPECTED_FIRST_ID, f"first id {ids[0]}"
    assert ids[-1] == EXPECTED_LAST_ID, f"last id {ids[-1]}"

    out = {"masail": all_items}
    out_text = json.dumps(out, ensure_ascii=False, indent=1)
    assert LENTICULAR_PREFIX not in out_text, "lenticular markers remain in output!"
    assert not SHORT_PREFIX_RE.search(out_text), "short markers remain in output!"
    total_markers = len(RE_FULL.findall(out_text))
    print(
        f"VALIDATION PASSED: {len(all_items)} items, {len(pages)} unique pages "
        f"(min {min(pages)}, max {max(pages)}), volume_ids {sorted(vols)}, "
        f"first {ids[0]}, last {ids[-1]}, start_page == min(pages) for all, "
        f"derived start_page for {len(derived)} items, markers {total_markers}"
    )

    with open(os.path.join(BASE, "jild-21.json"), "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"WROTE jild-21.json: {len(out_text.encode('utf-8'))} bytes")


if __name__ == "__main__":
    main()
