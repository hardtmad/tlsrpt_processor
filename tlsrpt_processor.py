#!/usr/bin/env python
#tlsrpt_processor
#Copyright 2018 Comcast Cable Communications Management, LLC
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
#See the License for the specific language governing permissions and
#limitations under the License
#
#This product includes software developed at Comcast ( http://www.comcast.com/)
#
#
# Authors: Alex Brotman (alex_brotman@comcast.com)
#          Madeleine Hardt (hardtmad@gmail.com)
#
# Purpose: Parse a TLSRPT report, and output as specified
#
# Notes: RFC-8460
#
# URL: https://github.com/Comcast/tlsrpt_processor/
#

import json,sys,getopt,time,gzip,requests

csv_separator = "|"
httpMaxRetries = 5

def show_help():
	print("")
	print("This script should process a TLSRPT JSON file passed as an argument")
	print("Options are as follows:")
	print("-h				Show this help message")
	print("-i/-input 			Input file")
	print("-o/-output-style		Output Style (values: kv,csv,gzip-json)")
	print("-s/-send-method		Send Method  (values: http)")
	print("-d/-destination 		Destination (http endpoint if applicable)")
	print("")

# Load input file information into a JSON object
def parse_input(input_file, process_time):
	rptJson = {}
	with open(input_file) as json_file:
		try:
			data = json.load(json_file)
		except ValueError:
			print("Invalid JSON file")
			sys.exit(1)

		try:
			rptJson["organization_name"] = data["organization-name"]
		except KeyError:
			rptJson["organization_name"] = ""
		try:
			rptJson["start_date_time"] = data["date-range"]["start-datetime"]
		except KeyError:
			rptJson["start_date_time"] = ""
		try:
			rptJson["end_date_time"] = data["date-range"]["end-datetime"]
		except KeyError:
			rptJson["end_date_time"] = ""
		try:
			rptJson["contact_info"] = data["contact-info"]
		except KeyError:
			rptJson["contact_info"] = ""
		try:
			rptJson["report_id"] = data["report-id"]
		except KeyError:
			rptJson["report_id"] = ""

		for policy_set in data["policies"]:
			try:
				rptJson["policy_type"] = policy_set["policy"]["policy-type"]
			except KeyError:
				rptJson["policy_type"] = ""
			try:
				rptJson["policy_string"] = policy_set["policy"]["policy-string"]
			except KeyError:
				rptJson["policy_string"] = ""
			try:
				rptJson["policy_domain"] = policy_set["policy"]["policy-domain"]
			except KeyError:
				rptJson["policy_domain"] = ""
			try:
				rptJson["policy_mx_host"] = policy_set["policy"]["mx-host"]
			except KeyError:
				rptJson["policy_mx_host"] = ""
			try:
				rptJson["policy_success_count"] = policy_set["summary"]["total-successful-session-count"]
			except KeyError:
				rptJson["policy_success_count"] = 0
			try:
				rptJson["policy_failure_count"] = policy_set["summary"]["total-failure-session-count"]
			except KeyError:
				rptJson["policy_failure_count"] = 0

			if 'failure-details' in policy_set:
				rptJson["hasFailureDetails"] = True
				for failure_details_set in policy_set["failure-details"]:
					try:
						rptJson["result_type"] = failure_details_set["result-type"]
					except KeyError:
						rptJson["result_type"] = ""
					try:
						rptJson["sending_ip"] = failure_details_set["sending-mta-ip"]
					except KeyError:
						rptJson["sending_ip"] = ""
					try:
						rptJson["receiving_mx_hostname"] = failure_details_set["receiving-mx-hostname"]
					except KeyError:
						rptJson["receiving_mx_hostname"] = ""
					try:
						rptJson["receiving_mx_helo"] = failure_details_set["receiving-mx-helo"]
					except KeyError:
						rptJson["receiving_mx_helo"] = None  # optional per RFC
					try:
						rptJson["receiving_ip"] = failure_details_set["receiving-ip"]
					except KeyError:
						rptJson["receiving_ip"] = ""
					try:
						rptJson["failed_session_count"] = failure_details_set["failed-session-count"]
					except KeyError:
						rptJson["failed_session_count"] = 0
					try:
						rptJson["additional_info"] = failure_details_set["additional-information"]
					except KeyError:
						rptJson["additional_info"] = None  # optional per RFC
					try:
						rptJson["failure_error_code"] = failure_details_set["failure-error-code"]
					except KeyError:
						rptJson["failure_error_code"] = ""
			else:
				rptJson["hasFailureDetails"] = False
	return rptJson

# Convert JSON data to specified output style
def convert_to_output_style(output_style, rptJson, process_time):
	if output_style == "gzip-json":
		rptFinal = (json.dumps(rptJson)).encode('utf-8')
		# rpt_fn = rptJson["report_id"] + ".json.gz"
		# with gzip.GzipFile(rpt_fn, 'w') as fout:
		# 	fout.write(rptFinal)
	elif output_style == "csv":
		rptFinal = (process_time + csv_separator)
		rptFinal += (rptJson["report_id"] + csv_separator)
		rptFinal += ('"' + rptJson["organization_name"] + '"' + csv_separator)
		rptFinal += (rptJson["start_date_time"] + csv_separator)
		rptFinal += (rptJson["end_date_time"] + csv_separator)
		rptFinal += (rptJson["contact_info"] + csv_separator)
		rptFinal += (rptJson["policy_type"] + csv_separator)
		rptFinal += ('"' + csv_separator.join(rptJson["policy_string"]) + '"' + csv_separator)
		rptFinal += (rptJson["policy_domain"] + csv_separator)
		rptFinal += ('"' + rptJson["policy_mx_host"] + '"' + csv_separator)
		rptFinal += (str(rptJson["policy_success_count"]) + csv_separator)
		rptFinal += (str(rptJson["policy_failure_count"]))
		if rptJson["hasFailureDetails"]:
			rptFinal += (csv_separator + rptJson["result_type"] + csv_separator)
			rptFinal += (rptJson["sending_ip"] + csv_separator)
			rptFinal += (rptJson["receiving_mx_hostname"] + csv_separator)
			if rptJson["receiving_mx_helo"]:
				rptFinal += (rptJson["receiving_mx_helo"] + csv_separator)
			rptFinal += (rptJson["receiving_ip"] + csv_separator)
			rptFinal += (str(rptJson["failed_session_count"]) + csv_separator)
			if rptJson["additional_info"]:
				rptFinal += ('"' + rptJson["additional_info"] + '"' + csv_separator)
			rptFinal += (rptJson["failure_error_code"])
	elif output_style == "kv":
		rptFinal = ('process-time="' + process_time + '"')
		rptFinal += (' report-id="' + rptJson["report_id"] + '"')
		rptFinal += (' organization-name="' + rptJson["organization_name"] + '"')
		rptFinal += (' start-date-time="' + rptJson["start_date_time"] + '"')
		rptFinal += (' end-date-time="' + rptJson["end_date_time"] + '"')
		rptFinal += (' contact-info="' + rptJson["contact_info"] + '"')
		rptFinal += (' policy-type="' + rptJson["policy_type"] + '"')
		rptFinal += (' policy-string="' + ",".join(rptJson["policy_string"]) + '"')
		rptFinal += (' policy-domain="' + rptJson["policy_domain"] + '"')
		rptFinal += (' policy-mx-host="' + rptJson["policy_mx_host"] + '"')
		rptFinal += (' policy-success-count="' + str(rptJson["policy_success_count"]) + '"')
		rptFinal += (' policy-failure-count="' + str(rptJson["policy_failure_count"]) + '"')
		if rptJson["hasFailureDetails"]:
			rptFinal += (' result-type="' + rptJson["result_type"] + '"')
			rptFinal += (' sending-ip="' + rptJson["sending_ip"] + '"')
			rptFinal += (' receiving-mx-hostname="' + rptJson["receiving_mx_hostname"] + '"')
			if rptJson["receiving_mx_helo"]:
				rptFinal += (' receiving-mx-helo="' + rptJson["receiving_mx_helo"] + '"')
			rptFinal += (' receiving-ip="' + rptJson["receiving_ip"] + '"')
			rptFinal += (' failed-count="' + str(rptJson["failed_session_count"]) + '"')
			if rptJson["additional_info"]:
				rptFinal += (' additional-info="' + rptJson["additional_info"] + '"')
			rptFinal += (' failure-error-code="' + rptJson["failure_error_code"] + '"')
	else:
		print ("Unrecognized output style")
		show_help()
		exit(1)
	return rptFinal

def main(input_file, output_style, send_method, destination):
	global httpMaxRetries
	process_time = "%15.0f" % time.time()
	process_time = process_time.strip()
	try:
		rptJson = parse_input(input_file, process_time)
	except:
		print("Error parsing input file")
		exit(2)
	try:
		rptFinal = convert_to_output_style(output_style, rptJson, process_time)
	except Exception as e:
		print("Error converting to output style")
		exit(2)
	if send_method == "http":
		success = False
		while httpMaxRetries > 0 and not success:
			try:
				print("POST %s" % destination)
				rptHeaders = {'Content-type': 'application/tlsrpt+gzip'}  # RFC specified
				r = requests.post(url=destination, data=rptFinal, headers=rptHeaders)
				print("Got response: %s %s" % (r.status_code, r.reason))
				success = True
			except:
				print("Error sending to %s" % destination)
				httpMaxRetries -= 1
	else:
		print(rptFinal)


if __name__ == "__main__":
	try:
		opts, args = getopt.getopt(sys.argv[1:],"i:o:h:s:d:",["input=","output-style=","help","send-method=","destination="])
	except getopt.GetoptError as err:
		print (str(err))
		show_help()
		sys.exit(2)
	input_file = None
	output_style = None
	send_method = None
	destination = None
	for o,a in opts:
		if o in ("-h","-help"):
			show_help()
			sys.exit()
		elif o in ("-i","-input"):
			input_file = a
		elif o in ("-o","-output-style"):
			output_style = a
			if a not in ("kv","csv","gzip-json"):
				show_help()
				sys.exit(1)
		elif o in ("-s","-send-method"):
			send_method = a
			if a not in ("http"):
				show_help()
				sys.exit(1)
		elif o in ("-d","-destination"):
			destination = a
		else:
			assert False, "Unrecognized option"
	# Arg validation
	if input_file is None:
		print("\nERROR: Input file is required")
		show_help()
		sys.exit(1)
	try:
		open(input_file,"r")
	except IOError:
		print("Input File does not exist or does not have the proper permissions")
		sys.exit(1)
	if send_method == "http":
		if not destination:
			print("\nERROR: Destiantion is required if send_method is http")
			sys.exit(1)
		if output_style != "gzip-json":
			if output_style != None:
				print("WARNING: Overriding output_style, must be gzip-jsop if send_method is http")
			output_style = "gzip-json"
	main(input_file, output_style, send_method, destination)
