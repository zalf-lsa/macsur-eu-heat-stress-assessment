#!/usr/bin/python
# -*- coding: UTF-8

# pragma pylint: disable=mixed-indentation

import time
import os
import zmq
import json
import csv
import copy
import sys
from StringIO import StringIO
from datetime import date, datetime, timedelta
#import types
import sys
sys.path.append("C:/Users/berg.ZALF-AD/GitHub/monica/project-files/Win32/Release")	 # path to monica_python.pyd or monica_python.so
#sys.path.append('C:/Users/berg.ZALF-AD/GitHub/util/soil')
#from soil_conversion import *
#import monica_python
import monica_io

#print "pyzmq version: ", zmq.pyzmq_version()
#print "sys.path: ", sys.path
#print "sys.version: ", sys.version

def main():
	context = zmq.Context()
	socket = context.socket(zmq.PUSH)
	port = 6666 if len(sys.argv) == 1 else sys.argv[1]
	socket.bind("tcp://*:" + str(port))

	with open("sim.json") as f:
		sim = json.load(f)

	with open("site.json") as f:
		site = json.load(f)

	with open("crop.json") as f:
		crop = json.load(f)

	sim["include-file-base-path"] = "C:/Users/berg.ZALF-AD/MONICA"
	#sim["climate.csv"] = "35_120_v1.csv"
	#sim["climate.csv"] = "C:/Users/stella/MONICA/Examples/Hohenfinow2/climate.csv"

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
	
	
	def updateSoilCropDates(row, col):
		s = soil[(row, col)]
		#p, cropType = (wwPheno[(row, col)], "WW")
		p, cropType = (smPheno[(row, col)], "SM")
		
		startDate = date(1980, 1, 1)# + timedelta(days = p["sowing-doy"])
		sim["start-date"] = startDate.isoformat()
		sim["end-date"] = date(2010, 12, 31).isoformat()
		#sim["debug?"] = True

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

	def readClimate(pathToFile):

		def csv2string(data):
			si = StringIO()
			cw = csv.writer(si)
			cw.writerow(data)
			return si.getvalue().strip('\r\n')

		climateCSVString = ""
		lastInSection = False
		with open(pathToFile) as f:
			c = {}
			reader = csv.reader(f)
			reader.next()

			csvHeader = csv2string(["iso-date", "tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid"]) + "\n"
			prev = ("","")
			for row in reader:
				period_gcmRcp = (row[0], row[1])
				
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
				if prev != period_gcmRcp:
					print period_gcmRcp
					prev = period_gcmRcp

				c[period_gcmRcp] = c.get(period_gcmRcp, csvHeader) + csv2string(line) + "\n"
			
			return c

	
	pathToClimateData = "A:/macsur-eu-heat-stress-transformed/"

	allRowsCols = set(soil.iterkeys())
	allRowsCols.intersection_update(wwPheno, smPheno)
	print "# of rowsCols = ", len(allRowsCols)

	#sim["climate.csv-options"]["start-date"] = sim["start-date"]
	#sim["climate.csv-options"]["end-date"] = sim["end-date"]
	sim["climate.csv-options"]["use-leap-years"] = sim["use-leap-years"]

	readClimateDataLocally = True
	i = 0
	envs = []
	startStore = time.clock()
	for row, col in allRowsCols:
		updateSoilCropDates(row, col)
		env = monica_io.createEnvJsonFromJsonConfig({
			  "crop": crop
			, "site": site
			, "sim": sim
			, "climate": ""
			})
		if not readClimateDataLocally:
			env["csvViaHeaderOptions"] = sim["climate.csv-options"]	

		if i > 500:
			break

		for period in os.listdir(pathToClimateData):
			for gcm in os.listdir(os.path.join(pathToClimateData, period)):

				climateFileName = "{}_{:03d}_v1.csv".format(row, col)
				pathToClimateFile = os.path.join(pathToClimateData, period, gcm, climateFileName)
				#climate = readClimate("{}{}_{:03d}_v1.csv".format(pathToClimateData, row, col))

				if not os.path.exists(pathToClimateFile):
					continue
				
				#read climate data on client and send them with the env
				if readClimateDataLocally:
					with open(pathToClimateFile) as cf:
						climateData = cf.read()
					monica_io.addClimateDataToEnv(env, sim, climateData)

				#read climate data on the server and send just the path to the climate data csv file
				if not readClimateDataLocally:				
					env["pathToClimateCSV"] = pathToClimateFile

				#for i in range(0, 2 + 1):
				#	env["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]["OrganIdsForPrimaryYield"][i]["yieldPercentage"] = 1
				#	env["cropRotation"][0]["worksteps"][1]["crop"]["cropParams"]["cultivar"]["OrganIdsForPrimaryYield"][i]["yieldPercentage"] = 1

				#socket.send_json(env)
				envs.append(copy.deepcopy(env))
				print "stored env ", i

				i += 1

	stopStore = time.clock()
	
	#print "storing ", i, " envs took ", (stopStore - startStore), " seconds"
	#return

	k = 0
	startSend = time.clock()
	for env in envs:
		socket.send_json(env)
		print "send message ", k
		k += 1			
	stopSend = time.clock()

	print "storing ", i, " envs took ", (stopStore - startStore), " seconds"
	print "sending ", k, " envs took ", (stopSend - startSend), " seconds"

def producer(env):
	
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