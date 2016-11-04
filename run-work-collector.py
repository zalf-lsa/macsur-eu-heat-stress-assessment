#!/usr/bin/python
# -*- coding: UTF-8

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

import sys
sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
print sys.path

import csv
import types
import os
from datetime import datetime
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
#print "path to monica_io: ", monica_io.__file__

def create_year_output(oids, row, col, rotation, prod_level, values):
    "create year output lines"
    row_col = "{}{:03d}".format(row, col)
    out = []
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                if iii == 1:
                    vals[oid["name"]] = (values[iii+1][kkk] - val) / val if val > 0 else 0.0
                elif iii == 2:
                    continue
                else:
                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[oid["name"]] = val_
                    else:
                        vals[oid["name"]] = val

            if vals.get("Year", 0) > 1982:
                out.append([
                    row_col,
                    rotation,
                    prod_level,
                    vals.get("Year", "NA"),
                    vals.get("SOC", "NA"),
                    vals.get("Rh", "NA"),
                    vals.get("NEP", "NA"),
                    vals.get("Act_ET", "NA"),
                    vals.get("Act_Ev", "NA"),
                    vals.get("PercolationRate", "NA"),
                    vals.get("Irrig", "NA"),
                    vals.get("NLeach", "NA"),
                    vals.get("ActNup", "NA"),
                    vals.get("NFert", "NA"),
                    vals.get("N2O", "NA")
                ])

    return out


def create_crop_output(oids, row, col, rotation, prod_level, values):
    "create crop output lines"
    row_col = "{}{:03d}".format(row, col)
    out = []
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                if iii == 2:
                    start = datetime.strptime(val, "%Y-%m-%d")
                    end = datetime.strptime(values[iii+1][kkk], "%Y-%m-%d")
                    vals[oid["name"]] = (end - start).days
                elif iii == 3:
                    continue
                elif iii == 4:
                    vals[oid["name"]] = (values[iii+1][kkk] - val) / val if val > 0 else 0.0
                elif iii == 5:
                    continue
                else:
                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[oid["name"]] = val_
                    else:
                        vals[oid["name"]] = val

            if vals.get("Year", 0) > 1982:
                out.append([
                    row_col,
                    rotation,
                    vals.get("Crop", "NA").replace("/", "_").replace(" ", "-"),
                    prod_level,
                    vals.get("Year", "NA"),
                    vals.get("Date", "NA"),
                    vals.get("SOC", "NA"),
                    vals.get("Rh", "NA"),
                    vals.get("NEP", "NA"),
                    vals.get("Yield", "NA"),
                    vals.get("Act_ET", "NA"),
                    vals.get("Act_Ev", "NA"),
                    vals.get("PercolationRate", "NA"),
                    vals.get("Irrig", "NA"),
                    vals.get("NLeach", "NA"),
                    vals.get("ActNup", "NA"),
                    vals.get("NFert", "NA"),
                    vals.get("N2O", "NA"),
                    vals.get("Nstress", "NA")
                ])

    return out

def write_data(region_id, year_data, crop_data):
    "write data"

    path_to_crop_file = "out/" + str(region_id) + "_crop.csv"
    path_to_year_file = "out/" + str(region_id) + "_year.csv"

    if not os.path.isfile(path_to_year_file):
        with open(path_to_year_file, "w") as _:
            _.write("ID.cell,rotation,prod.level,year,delta.OC,CO2.emission,NEP,ET,EV,water.perc,irr,N.leach,N.up,N.fert,N2O.em\n")

    with open(path_to_year_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in year_data[region_id]:
            writer.writerow(row_)
        year_data[region_id] = []

    if not os.path.isfile(path_to_crop_file):
        with open(path_to_crop_file, "w") as _:
            _.write("ID.cell,rotation,crop,prod.level,year,cycle.length,delta.OC,CO2.emission,NEP,yield,ET,EV,water.perc,irr,N.leach,N.up,N.fert,N2O.em,N.stress\n")

    with open(path_to_crop_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in crop_data[region_id]:
            writer.writerow(row_)
        crop_data[region_id] = []


def collector():
    "collect data from workers"

    year_data = defaultdict(list)
    crop_data = defaultdict(list)

    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind("tcp://*:7777")
    socket.RCVTIMEO = 1000
    leave = False
    write_normal_output_files = True
    start_writing_lines_threshold = 1000
    while not leave:

        try:
            result = socket.recv_json()
        except:
            for region_id in year_data.keys():
                if len(year_data[region_id]) > 0:
                    write_data(region_id, year_data, crop_data)
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", ""), " len(year_data): ", len((year_data.values()[:1] or [[]])[0])

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            crop_id = ci_parts[0]
            row_, col_ = ci_parts[1][1:-1].split("/")
            row, col = (int(row_), int(col_))
            period = ci_parts[2]
            gcm = ci_parts[3]
            sim_id = ci_parts[4]

            for data in result.get("data", []):
                results = data.get("results", [])
                orig_spec = data.get("origSpec", "")
                output_ids = data.get("outputIds", [])
                if len(results) > 0:
                    if orig_spec == '"yearly"':
                        res = []#create_year_output(output_ids, row, col, rotation, prod_level, results)
                        year_data[region_id].extend(res)
                    elif orig_spec == '"crop"':
                        res = []#create_crop_output(output_ids, row, col, rotation, prod_level, results)
                        crop_data[region_id].extend(res)

            for region_id in year_data.keys():
                if len(year_data[region_id]) > start_writing_lines_threshold:
                    write_data(region_id, year_data, crop_data)

            i = i + 1

        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            with open("out/out-" + str(i) + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")

                for data in result.get("data", []):
                    results = data.get("results", [])
                    orig_spec = data.get("origSpec", "")
                    output_ids = data.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            i = i + 1


collector()
