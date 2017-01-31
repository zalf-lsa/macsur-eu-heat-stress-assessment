# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import os
import sys
import time
from collections import defaultdict


HEADER = ""
ASC = []
with open("macsur-heat-stress-study-extent.asc") as file_:
    for _ in range(0, 6):
        HEADER = HEADER + file_.next()
    for line in file_:
        line_ = []
        for rowcol_str in line.strip().split(" "):
            line_.append(rowcol_str)
        ASC.append(line_)


PATH_TO_OUTPUTS = "P:/macsur-eu-heat-stress-assessment/outputs/rerun-2017-01-30/"
FILES = os.listdir(PATH_TO_OUTPUTS)

OUTPUTS = [
    {"name": "yield", "idx": 11},
    {"name": "biom-ma", "idx": 16},
    {"name": "max-lai", "idx": 17}
]

data = {}
for idx, filename in enumerate(FILES):
    with open(PATH_TO_OUTPUTS + filename) as _:
        reader = csv.reader(_)
        reader.next()
        start = time.clock()
        for line in reader:
            if line[3] != "C1":
                continue

            row_, col_ = line[1].split("_")
            row, col = (int(row_), int(col_))
            
            crop = line[2]
            prodcase = line[9]
            if not crop in data:
                data[crop] = {}
            if not prodcase in data[crop]:
                data[crop][prodcase] = {}
            if not (row, col) in data[crop][prodcase]:
                data[crop][prodcase][(row, col)] = defaultdict(list)

            for output in OUTPUTS:
                val = line[output["idx"]]
                if val != "na":
                    data[crop][prodcase][(row, col)][output["name"]].append(float(val))

        stop = time.clock()
        print "read " + str(idx) + " file(s) name: " + filename + " in " + str(stop - start) + " s"


FILES = []
for crop, d in data.iteritems():
    for prodcase, _ in d.iteritems():
        for output in OUTPUTS:
            FILES.append({
                "crop": crop, 
                "prodcase": prodcase, 
                "name": output["name"], 
                "file": open("result-maps/" + output["name"] + "-" + crop + "-" + prodcase + ".asc", "w")
            })

for ddd in FILES:
    ddd["file"].write(HEADER)

for row in ASC:
    line = []
    for col_no, rowcol_str in enumerate(row):
        row_, col_ = (int(rowcol_str[:2]), int(rowcol_str[2:])) if len(rowcol_str) == 5 else (int(rowcol_str[:3]), int(rowcol_str[3:])) 
        for idx, ddd in enumerate(FILES):
            crop = ddd["crop"]
            prodcase = ddd["prodcase"]
            name = ddd["name"]
            if rowcol_str == "-9999" or (row_, col_) not in data[crop][prodcase]:
                ddd["file"].write("-9999")
            else:
                lll = data[crop][prodcase][(row_, col_)][name]
                ddd["file"].write(str(sum(lll)/len(lll)))

            if col_no == 133:
                ddd["file"].write("\n")
            else:
                ddd["file"].write(" ")

for ddd in FILES:
    ddd["file"].close()

