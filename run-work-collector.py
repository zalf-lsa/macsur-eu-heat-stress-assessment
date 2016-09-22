#!/usr/bin/python
# -*- coding: ISO-8859-15-*-

# pragma pylint: disable=mixed-indentation

import zmq
#print zmq.pyzmq_version()
import time
import pprint
import types
import json
import csv
import sys
sys.path.append("C:/Users/berg.ZALF-AD/GitHub/monica/project-files/Win32/Release")
import monica_python

OP_AVG = 0
OP_MEDIAN = 1
OP_SUM = 2
OP_MIN = 3
OP_MAX = 4
OP_FIRST = 5
OP_LAST = 6
OP_NONE = 7
OP_UNDEFINED_OP_ = 8

ORGAN_ROOT = 0
ORGAN_LEAF = 1
ORGAN_SHOOT = 2
ORGAN_FRUIT = 3
ORGAN_STRUCT = 4
ORGAN_SUGAR = 5
ORGAN_UNDEFINED_ORGAN_ = 6

def oidIsOrgan(oid):
	return oid["organ"] != ORGAN_UNDEFINED_ORGAN_

def oidIsRange(oid):
	return oid["fromLayer"] >= 0 \
		and oid["toLayer"] >= 0 \
		and oid["fromLayer"] < oid["toLayer"] 

def opToString(op):
	return { 
		  OP_AVG: "AVG"
		, OP_MEDIAN: "MEDIAN"
		, OP_SUM: "SUM"
		, OP_MIN: "MIN"
		, OP_MAX: "MAX"
		, OP_FIRST: "FIRST"
		, OP_LAST: "LAST"
		, OP_NONE: "NONE"
		, OP_UNDEFINED_OP_: "undef"
		}.get(op, "undef")

def organToString(organ):
	return {
		  ORGAN_ROOT: "Root"
		, ORGAN_LEAF: "Leaf"
		, ORGAN_SHOOT: "Shoot"
		, ORGAN_FRUIT: "Fruit"
		, ORGAN_STRUCT: "Struct"
		, ORGAN_SUGAR: "Sugar"
		, ORGAN_UNDEFINED_ORGAN_: "undef"
		}.get(organ, "undef")

def oidToString(oid, includeTimeAgg):
	oss = ""
	oss += "["
	oss += oid["name"]
	if oidIsOrgan(oid):
		oss += ", " + organToString(oid["organ"])
	elif oidIsRange(oid):
		oss += ", [" + str(oid["fromLayer"] + 1) + ", " + str(oid["toLayer"] + 1) \
		+ (", " + opToString(oid["layerAggOp"]) if oid["layerAggOp"] != OP_NONE else "") \
		+ "]"
	elif oid["fromLayer"] >= 0:
		oss += ", " + str(oid["fromLayer"] + 1)
	if includeTimeAgg:
		oss += ", " + opToString(oid["timeAggOp"])
	oss += "]"

	return oss

def writeOutputHeaderRows(outputIds, includeHeaderRow, includeUnitsRow, includeTimeAgg):
	row1 = []
	row2 = []
	row3 = []
	row4 = []
	for oid in outputIds:
		fromLayer = oid["fromLayer"]
		toLayer = oid["toLayer"]
		isOrgan = oidIsOrgan(oid)
		isRange = oidIsRange(oid) and oid["layerAggOp"] == OP_NONE
		if isOrgan:
			toLayer = fromLayer = oid["organ"] # organ is being represented just by the value of fromLayer currently
		elif isRange:
			fromLayer += 1
			toLayer += 1     # display 1-indexed layer numbers to users
		else:
			toLayer = fromLayer # for aggregated ranges, which aren't being displayed as range

		for i in range(fromLayer, toLayer+1):
			str1 = ""
			if isOrgan:
				str1 += oid["name"] + "/" + organToString(oid["organ"])
			elif isRange:
				str1 += oid["name"] + "_" + str(i)
			else:
				str1 += oid["name"]
			row1.append(str1)
			row4.append("j:" + oid["jsonInput"].replace("\"", ""))
			row3.append("m:" + oidToString(oid, includeTimeAgg))
			row2.append("[" + oid["unit"] + "]")

	out = []
	if includeHeaderRow:
		out.append(row1)
	if includeUnitsRow:
		out.append(row4)
	if includeTimeAgg:
		out.append(row3)
		out.append(row2)
	
	return out


def writeOutput(outputIds, values):
	out = []
	if len(values) > 0:
		for k in range(0, len(values[0])):
			i = 0
			row = []
			for oid in outputIds:
				j = values[i][k]
				if type(j) == types.ListType:
					for jv in j:
						row.append(jv)
				else:
					row.append(j)
				i += 1
			out.append(row)
	return out


csvSep = ","
includeHeaderRow = True
includeUnitsRow = True


def parseOutputIds(oids):
		j = json.dumps(oids)
		rs = monica_python.parseOutputIdsToJsonString(j)
		p = json.loads(rs, "latin-1")
		return p


def collector():
	with open("sim.json") as f:
		sim = json.load(f)

	i = 0

	context = zmq.Context()
	socket = context.socket(zmq.PULL)
	socket.bind("tcp://*:7777")
	collector_data = {}
	leave = False
	while not leave:
		result = socket.recv_json()
		if result["type"] == "finish":
			print "received finish message"
			leave = True
		else:
			print "received work result ", i
			d = result["daily"]
			
			outputIds = parseOutputIds(sim["output"]["daily"])
						
			hout = writeOutputHeaderRows(outputIds, True, True, True)
			rout = writeOutput(outputIds, d)
			
			with open("out-" + str(i) + ".csv", 'wb') as f:
				writer = csv.writer(f)
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

