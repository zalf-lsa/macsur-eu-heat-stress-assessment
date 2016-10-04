#!/usr/bin/python
# -*- coding: UTF-8

# pragma pylint: disable=mixed-indentation

import monica_io
import zmq
#print zmq.pyzmq_version()
import json
import csv
import sys
sys.path.append("C:/Users/berg.ZALF-AD/GitHub/monica/project-files/Win32/Release")

def collector():
	with open("sim.json") as f:
		sim = json.load(f)

	i = 0

	context = zmq.Context()
	socket = context.socket(zmq.PULL)
	socket.bind("tcp://*:7777")
	leave = False
	while not leave:
		result = socket.recv_json()
		if result["type"] == "finish":
			print "received finish message"
			leave = True
		else:
			print "received work result ", i
			d = result["daily"]
			
			outputIds = monica_io.parseOutputIds(sim["output"]["daily"])
						
			hout = monica_io.writeOutputHeaderRows(outputIds, includeHeaderRow = True, includeUnitsRow = True, includeTimeAgg = False)
			rout = monica_io.writeOutput(outputIds, d)
			
			with open("out/out-" + str(i) + ".csv", 'wb') as f:
				writer = csv.writer(f, delimiter = ",")
				for row in hout:
					writer.writerow(row)
				for row in rout:
					writer.writerow(row)

			i = i + 1

			#c = result["crop"]
			#print c
			#for cropName, data in c.iteritems(): 
			#	collector_data[cropName] = collector_data.setdefault(cropName, 0) + data[2]
		#print "intermediate results: ", collector_data

	#print "final results: ", collector_data

collector()

