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

import time
import os
import json
import csv
#import copy
from StringIO import StringIO
from datetime import date, datetime, timedelta
#import types
import sys
sys.path.append("C:/Users/berg.ZALF-AD/GitHub/monica/project-files/Win32/Release")	 # path to monica_python.pyd or monica_python.so
#sys.path.append('C:/Users/berg.ZALF-AD/GitHub/util/soil')
#from soil_conversion import *
#import monica_python
import zmq
import monica_io

#print "pyzmq version: ", zmq.pyzmq_version()
#print "sys.path: ", sys.path
#print "sys.version: ", sys.version

#PATH_TO_CLIMATE_DATA = "A:/macsur-eu-heat-stress-transformed/"
PATH_TO_CLIMATE_DATA_SERVER = "../../macsur-heat-stress-transformed/"
PATH_TO_CLIMATE_DATA = "U:/development/macsur-heat-stress-transformed/"

def main():
    "main"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 if len(sys.argv) == 1 else sys.argv[1]
    socket.bind("tcp://*:" + str(port))

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    sim["include-file-base-path"] = "C:/Users/berg.ZALF-AD/MONICA"
    #sim["climate.csv"] = "35_120_v1.csv"
    #sim["climate.csv"] = "C:/Users/stella/MONICA/Examples/Hohenfinow2/climate.csv"

    def read_pheno(path_to_file):
        "read phenology data"
        with open(path_to_file) as _:
            ppp = {}
            reader = csv.reader(_)
            reader.next()
            for row in reader:
                ppp[(int(row[0]), int(row[1]))] = {
                    "sowing-doy": int(row[6]),
                    "flowering-doy": int(row[7]),
                    "harvest-doy": int(row[8])
                }
            return ppp

    www_pheno = read_pheno("WW_pheno.csv")
    sm_pheno = read_pheno("Maize_pheno.csv")

    soil = {}
    with open("JRC_soil_macsur.csv") as _:
        reader = csv.reader(_)
        reader.next()
        for row in reader:
            soil[(int(row[1]), int(row[0]))] = {
                "elevation": float(row[4]),
                "latitude": float(row[5]),
                "depth": float(row[6]),
                "pwp": float(row[7]),
                "fc": float(row[8]),
                "sat": float(row[9]),
                "sw-init": float(row[10]),
                "oc-topsoil": float(row[11]),
                "oc-subsoil": float(row[12]),
                "bd-topsoil": float(row[13]),
                "bd-subsoil": float(row[14])
            }


    def update_soil_crop_dates(row, col):
        "update function"
        sss = soil[(row, col)]
        #p, crop_type = (www_pheno[(row, col)], "WW")
        ppp, crop_type = (sm_pheno[(row, col)], "SM")

        start_date = date(1980, 1, 1)# + timedelta(days = p["sowing-doy"])
        sim["start-date"] = start_date.isoformat()
        sim["end-date"] = date(2010, 12, 31).isoformat()
        #sim["debug?"] = True

        is_wintercrop = ppp["sowing-doy"] > ppp["harvest-doy"]
        seeding_date = date(1980, 1, 1) + timedelta(days=ppp["sowing-doy"])
        crop["cropRotation"][0]["worksteps"][0]["date"] = seeding_date.strftime("0000-%m-%d")
        crop["cropRotation"][0]["worksteps"][0]["crop"][2] = crop_type
        harvest_date = date(1980 + (1 if is_wintercrop else 0), 1, 1) + timedelta(days=ppp["harvest-doy"])
        crop["cropRotation"][0]["worksteps"][1]["date"] = harvest_date.strftime("000" + ("1" if is_wintercrop else "0") + "-%m-%d")
        crop["cropRotation"][0]["worksteps"][1]["crop"][2] = crop_type

        site["Latitude"] = sss["latitude"]
        pwp = sss["pwp"]
        fc_ = sss["fc"]
        sm_percent_fc = (((sss["sw-init"] - pwp) / 0.7) + pwp) / fc_ * 100.0
        top = {
            "Thickness": [0.3, "m"],
            "SoilOrganicCarbon": [sss["oc-topsoil"], "%"],
            "SoilBulkDensity": [sss["bd-topsoil"] * 1000, "kg m-3"],
            "FieldCapacity": [fc_, "m3 m-3"],
            "PermanentWiltingPoint": [pwp, "m3 m-3"],
            "PoreVolume": [sss["sat"], "m3 m-3"],
            "SoilMoisturePercentFC": [sm_percent_fc, "% [0-100]"],
            "Lambda": 0.5
            }
        sub = {
            "Thickness": [1.7, "m"],
            "SoilOrganicCarbon": [sss["oc-subsoil"], "%"],
            "SoilBulkDensity": [sss["bd-subsoil"] * 1000, "kg m-3"],
            "FieldCapacity": [fc_, "m3 m-3"],
            "PermanentWiltingPoint": [pwp, "m3 m-3"],
            "PoreVolume": [sss["sat"], "m3 m-3"],
            "SoilMoisturePercentFC": [sm_percent_fc, "% [0-100]"],
            "Lambda": 0.5
        }

        site["SiteParameters"]["SoilProfileParameters"] = [top, sub]
        #print site["SiteParameters"]["SoilProfileParameters"]

    def read_climate(path_to_file):
        "read climate data locally"

        def csv_to_string(data):
            sii = StringIO()
            cww = csv.writer(sii)
            cww.writerow(data)
            return sii.getvalue().strip('\r\n')

        #climate_csv_string = ""
        #last_in_section = False
        with open(path_to_file) as f:
            c = {}
            reader = csv.reader(f)
            reader.next()

            csv_header = csv_to_string(["iso-date", "tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid"]) + "\n"
            prev = ("", "")
            for row in reader:
                period_gcm_rcp = (row[0], row[1])

                line = [
                    datetime.strptime(row[2], "%Y%m%d").date().isoformat(),
                    float(row[4]),
                    (float(row[3]) + float(row[4])) / 2,
                    float(row[3]),
                    float(row[7]),
                    float(row[8]),
                    float(row[6]) / 24 / 3.6,
                    (float(row[9]) + float(row[10])) / 2,
                ]
                if prev != period_gcm_rcp:
                    print period_gcm_rcp
                    prev = period_gcm_rcp

                c[period_gcm_rcp] = c.get(period_gcm_rcp, csv_header) + csv_to_string(line) + "\n"

            return c


    all_rows_cols = set(soil.iterkeys())
    all_rows_cols.intersection_update(www_pheno, sm_pheno)
    print "# of rowsCols = ", len(all_rows_cols)

    #sim["climate.csv-options"]["start-date"] = sim["start-date"]
    #sim["climate.csv-options"]["end-date"] = sim["end-date"]
    sim["climate.csv-options"]["use-leap-years"] = sim["use-leap-years"]

    read_climate_data_locally = False
    i = 0
    envs = []
    start_store = time.clock()
    for row, col in all_rows_cols:
        update_soil_crop_dates(row, col)
        env = monica_io.createEnvJsonFromJsonConfig({
            "crop": crop,
            "site": site,
            "sim": sim,
            "climate": ""
        })
        if not read_climate_data_locally:
            env["csvViaHeaderOptions"] = sim["climate.csv-options"]

        if i > 1500:
            break

        for period in os.listdir(PATH_TO_CLIMATE_DATA):
            for gcm in os.listdir(os.path.join(PATH_TO_CLIMATE_DATA, period)):

                climate_filename = "{}_{:03d}_v1.csv".format(row, col)
                path_to_climate_file = os.path.join(PATH_TO_CLIMATE_DATA, period, gcm, climate_filename)
                #climate = read_climate("{}{}_{:03d}_v1.csv".format(PATH_TO_CLIMATE_DATA, row, col))

                if not os.path.exists(path_to_climate_file):
                    continue

                #read climate data on client and send them with the env
                if read_climate_data_locally:
                    with open(path_to_climate_file) as cf:
                        climate_data = cf.read()
                    monica_io.addClimateDataToEnv(env, sim, climate_data)

                #read climate data on the server and send just the path to the climate data csv file
                if not read_climate_data_locally:
                    env["pathToClimateCSV"] = os.path.join(PATH_TO_CLIMATE_DATA_SERVER, period, gcm, climate_filename)

                socket.send_json(env)
                print "sent env ", i
                #envs.append(copy.deepcopy(env))
                #print "stored env ", i

                i += 1

    stop_store = time.clock()

    print "sending ", i, " envs took ", (stop_store - start_store), " seconds"
    return

    kkk = 0
    start_send = time.clock()
    for env in envs:
        socket.send_json(env)
        print "send message ", kkk
        kkk += 1
    stop_send = time.clock()

    print "storing ", i, " envs took ", (stop_store - start_store), " seconds"
    print "sending ", kkk, " envs took ", (stop_send - start_send), " seconds"


main()