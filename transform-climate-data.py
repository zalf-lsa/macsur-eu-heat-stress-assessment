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

import csv
import os
import datetime

def main():
    
  def transformClimate(pathToFile, pathToOutput):
    prevPeriodGcm = ("","")
    filename = os.path.split(pathToFile)[1]
    with open(pathToFile) as f:
      reader = csv.reader(f)
      reader.next()
      of = None

      csvHeader = ["iso-date", "tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid", "vaporpress", "dewpoint_temp", "relhumid_tmin", "relhumid_tmax"]
      csvUnits = ["-", "°C", "°C", "°C", "mm", "MJ m-2", "m s-1", "% 0-100", "kPa", "°C", "% 0-100", "% 0-100"]
      for row in reader:
        period, gcm = period_gcmRcp = (row[0], row[1])
        
        line = [
          datetime.datetime.strptime(row[2], "%Y%m%d").date().isoformat()
          , float(row[4])
          , round((float(row[3]) + float(row[4])) / 2, 1)
          , float(row[3])
          , float(row[7])
          , float(row[8])
          , round(float(row[6]) / 24 / 3.6, 1)
          , round((float(row[9]) + float(row[10])) / 2, 1)
          , float(row[5])
          , float(row[11])
          , float(row[9])
          , float(row[10])
          ]

        if prevPeriodGcm != period_gcmRcp:
          if of != None:
            of.close()

          fullPathToOutputDir = os.path.join(pathToOutput, period, gcm, "")
          if not os.path.exists(fullPathToOutputDir):
            os.makedirs(fullPathToOutputDir)

          of = open(fullPathToOutputDir + filename, 'wb')
          writer = csv.writer(of, delimiter = ",")
          writer.writerow(csvHeader)
          writer.writerow(csvUnits)
          prevPeriodGcm = period_gcmRcp
        
        writer.writerow(line)

      if of != None:
        of.close()		


  pathToClimateCSVs = "/beegfs/common/data/climate/macsur_european_climate_scenarios_v2/original/"
  pathToOutput = "/beegfs/common/data/climate/macsur_european_climate_scenarios_v2/transformed/"

  files = os.listdir(pathToClimateCSVs)
  print "read directory ..."
  for f in files:
    fullPath = os.path.join(pathToClimateCSVs, f)
    
    if os.path.isfile(fullPath):
      transformClimate(fullPath, pathToOutput)
      print "transformed ", f 

if __name__ == '__main__':
    main()