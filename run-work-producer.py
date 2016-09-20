#!/usr/bin/python
# -*- coding: ISO-8859-15-*-

# pragma pylint: disable=mixed-indentation

import time
import zmq
import json
import csv
from StringIO import StringIO
from datetime import date, datetime, timedelta
#import types
import sys
sys.path.append("C:/Users/berg.ZALF-AD/GitHub/monica/project-files/Win32/Release")	 # path to monica_python.pyd or monica_python.so
#sys.path.append('C:/Users/berg.ZALF-AD/GitHub/util/soil')
#from soil_conversion import *
#import monica_python
from env_json_from_json_config import createEnvJsonFromJsonConfig

#print "pyzmq version: ", zmq.pyzmq_version()
#print "sys.path: ", sys.path
#print "sys.version: ", sys.version

def main():
	with open("sim.json") as f:
		sim = json.load(f)

	with open("site.json") as f:
		site = json.load(f)

	with open("crop.json") as f:
		crop = json.load(f)

	sim["include-file-base-path"] = "C:/Users/berg.ZALF-AD/MONICA"
	#sim["climate.csv"] = "35_120_v1.csv"
	sim["climate.csv"] = "C:/Users/berg.ZALF-AD/MONICA/Examples/Hohenfinow2/climate.csv"

	def readPheno(pathToFile):
		with open(pathToFile) as f:
			p = {}
			reader = csv.reader(f)
			reader.next()
			for row in reader:
				p[(int(row[0]), int(row[1]))] = {
					"sowing-doy": int(row[6]) 
					, "flowering-doy": int(row[7]) 
					, "harvest-doy": int(row[8])
					}
			return p

	wwPheno = readPheno("WW_pheno.csv")
	smPheno = readPheno("Maize_pheno.csv")
	
	soil = {}
	with open("JRC_soil_macsur.csv") as f:
		reader = csv.reader(f)
		reader.next()
		for row in reader:
			soil[(int(row[1]), int(row[0]))] = { 
				  "elevation": float(row[4])
				, "latitude": float(row[5])
				, "depth": float(row[6])
				, "pwp": float(row[7])
				, "fc": float(row[8])
				, "sat": float(row[9])
				, "sw-init": float(row[10])
				, "oc-topsoil": float(row[11])
				, "oc-subsoil": float(row[12])
				, "bd-topsoil": float(row[13])
				, "bd-subsoil": float(row[14])
				}

	row = 48
	col = 42
	s = soil[(row, col)]
	#p, cropType = (wwPheno[(row, col)], "WW")
	p, cropType = (smPheno[(row, col)], "SM")
	
	startDate = date(1980, 1, 1)# + timedelta(days = p["sowing-doy"])
	sim["start-date"] = startDate.isoformat()
	sim["end-date"] = date(2010, 12, 31).isoformat()
	sim["debug?"] = True

	isWintercrop = p["sowing-doy"] > p["harvest-doy"]
	seedingDate = date(1980, 1, 1) + timedelta(days = p["sowing-doy"])
	crop["cropRotation"][0]["worksteps"][0]["date"] = seedingDate.strftime("0000-%m-%d")
	crop["cropRotation"][0]["worksteps"][0]["crop"][2] = cropType
	harvestDate = date(1980 + (1 if isWintercrop else 0), 1, 1) + timedelta(days = p["harvest-doy"])
	crop["cropRotation"][0]["worksteps"][1]["date"] = harvestDate.strftime("000" + ("1" if isWintercrop else "0") + "-%m-%d")
	crop["cropRotation"][0]["worksteps"][1]["crop"][2] = cropType

	site["Latitude"] = s["latitude"]
	pwp = s["pwp"]
	fc = s["fc"]
	smPercentFC = (((s["sw-init"] - pwp) / 0.7) + pwp) / fc * 100.0
	top = { 
		  "Thickness": [0.3, "m"]
		, "SoilOrganicCarbon": [s["oc-topsoil"], "%"]
		, "SoilBulkDensity": [s["bd-topsoil"] * 1000, "kg m-3"]
		, "FieldCapacity": [fc, "m3 m-3"]
		, "PermanentWiltingPoint": [pwp, "m3 m-3"]
		, "PoreVolume": [s["sat"], "m3 m-3"]
		, "SoilMoisturePercentFC": [smPercentFC, "% [0-100]"]
		, "Lambda": 0.5
		}
	sub = {
		  "Thickness": [1.7, "m"] 
		, "SoilOrganicCarbon": [s["oc-subsoil"], "%"]
		, "SoilBulkDensity": [s["bd-subsoil"] * 1000, "kg m-3"]
		, "FieldCapacity": [fc, "m3 m-3"]
		, "PermanentWiltingPoint": [pwp, "m3 m-3"]
		, "PoreVolume": [s["sat"], "m3 m-3"]
		, "SoilMoisturePercentFC": [smPercentFC, "% [0-100]"]
		, "Lambda": 0.5
		} 

	site["SiteParameters"]["SoilProfileParameters"] = [top, sub] 
	#print site["SiteParameters"]["SoilProfileParameters"]

	period = "0"
	gcm_rcp = "0_0" #"GFDL-CM3_45"
	
	def csv2string(data):
		si = StringIO()
		cw = csv.writer(si)
		cw.writerow(data)
		return si.getvalue().strip('\r\n')

	climateCSVString = ""
	lastInSection = False
	with open("35_120_v1.csv") as f:
		reader = csv.reader(f)
		reader.next()
		climateCSVString += csv2string(["iso-date", "tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid"]) + "\n"
		for row in reader:
			line = [
				  datetime.strptime(row[2], "%Y%m%d").date().isoformat()
				, float(row[4])
				, (float(row[3]) + float(row[4])) / 2
				, float(row[3])
				, float(row[7])
				, float(row[8])
				, float(row[6]) / 24 / 3.6
				, (float(row[9]) + float(row[10])) / 2
				]

			currentlyInSection = row[0] == period and row[1] == gcm_rcp
			if lastInSection and not currentlyInSection:
				break
			else: 
				if currentlyInSection:
					climateCSVString += csv2string(line) + "\n" #str(line)[1:-1] + "\n"
			lastInSection = currentlyInSection

	env = createEnvJsonFromJsonConfig({
		  "crop": crop
		, "site": site
		, "sim": sim
		, "climate": climateCSVString
		})
	#print env

	producer(env)


def producer(env):
	context = zmq.Context()
	socket = context.socket(zmq.PUSH)
	socket.bind("tcp://*:6666")

	#consumerSocket = context.socket(zmq.PUSH)
	#consumerSocket.connect("tcp://localhost:7777");
	
	#workerControlSocket = context.socket(zmq.PUB)
	#consumerSocket.bind("tcp://*:6666");

	#time.sleep(2)

	#for i in range(1):
	socket.send_json(env)

	# Start your result manager and workers before you start your producers
	
	#time.sleep(10)
	#workerControlSocket.send("finish " + json.dumps({"type": "finish"}))
	#consumerSocket.send_json({"type": "finish"})



main()