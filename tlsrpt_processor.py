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
# Author: Alex Brotman (alex_brotman@comcast.com)
#
# Purpose: Parse a TLSRPT report, and output as specified
#
# Notes: RFC-TBD
#
# URL: https://github.com/Comcast/tlsrpt_processor/
#

import json,sys,getopt,time
import gzip

def show_help():
	print("")
	print("This script should process a TLSRPT JSON file pass as an argument")
	print("Options are as follows:")
	print("-h				Show this help message")
	print("-i/-input 			Input file")
	print("-o/-output-style		Output Style (values: kv,csv,gzip-json)")
	print("")

try:
	opts, args = getopt.getopt(sys.argv[1:],"i:o:h",["input=","output-style=","help"])
except getopt.GetoptError as err:
	print (str(err))
	print (show_help())
	sys.exit(2)
input_file = None
output_style = None
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
	else:
		assert False, "Unrecognized option"

if input_file is None:
	print("\nERROR: Input file is required")
	show_help()
	sys.exit(1)


try:
	open(input_file,"r")
except IOError:
	print("Input File does not exist or does not have the proper permissions")
	sys.exit(1)

process_time = "%15.0f" % time.time()
process_time = process_time.strip()
csv_separator = "|"

with open(input_file) as json_file:
	try:
		data = json.load(json_file)
	except ValueError:
		print("Invalid JSON file")
		sys.exit(1)

	try:
		organization_name = data["organization-name"]
	except KeyError:
		organization_name = ""
	try: 
		start_date_time = data["date-range"]["start-datetime"] 
	except KeyError: 
		start_date_time="" 
	try:
		end_date_time = data["date-range"]["end-datetime"]
	except KeyError:
		end_date_time = ""
	try:
		contact_info = data["contact-info"]
	except KeyError:
		contact_info = ""
	try:
		report_id = data["report-id"]
	except KeyError:
		report_id = ""


	for policy_set in data["policies"]:
		try:
			policy_type = policy_set["policy"]["policy-type"]
		except KeyError:
			policy_type = ""
		try:
			policy_string = policy_set["policy"]["policy-string"]
		except KeyError:
			policy_string = ""
		try:
			policy_domain = policy_set["policy"]["policy-domain"]
		except KeyError:
			policy_domain = ""
		try:
			policy_mx_host = policy_set["policy"]["mx-host"]
		except KeyError:
			policy_mx_host = ""
		try: 
			policy_success_count = policy_set["summary"]["total-successful-session-count"]
		except KeyError:
			policy_success_count = 0
		try:
			policy_failure_count = policy_set["summary"]["total-failure-session-count"]
		except KeyError:
			policy_failure_count = 0


		if 'failure-details' in policy_set:
			for failure_details_set in policy_set["failure-details"]:
				try:
					result_type = failure_details_set["result-type"]
				except KeyError:
					result_type = ""
				try:
					sending_ip = failure_details_set["sending-mta-ip"]
				except KeyError:
					sending_ip = ""
				try:
					receiving_mx_hostname = failure_details_set["receiving-mx-hostname"]
				except KeyError:
					receiving_mx_hostname = ""
				try:
					receiving_mx_helo = failure_details_set["receiving-mx-helo"]
				except KeyError:
					receiving_mx_helo = ""
				try:
					receiving_ip = failure_details_set["receiving-ip"]
				except KeyError:
					receiving_ip = ""
				try:
					failed_session_count = failure_details_set["failed-session-count"]
				except KeyError:
					failed_session_count = 0
				try:
					additional_info = failure_details_set["additional-information"]
				except KeyError:
					additional_info = ""
				try:
					failure_error_code = failure_details_set["failure-error-code"]
				except KeyError:
					failure_error_code = ""

				if output_style in (['kv', 'gzip-json']):

					rpt = ('process-time="' + process_time + '"')
					rpt += (' report-id="' + report_id + '"')
					rpt += (' organization-name="' + organization_name + '"')
					rpt += (' start-date-time="' + start_date_time + '"')
					rpt += (' end-date-time="' + end_date_time + '"')
					rpt += (' contact-info="' + contact_info + '"')
					rpt += (' policy-type="' + policy_type + '"')
					rpt += (' policy-string="' + ",".join(policy_string) + '"')
					rpt += (' policy-domain="' + policy_domain + '"')
					rpt += (' policy-mx-host="' + policy_mx_host + '"')
					rpt += (' policy-success-count="' + str(policy_success_count) + '"')
					rpt += (' policy-failure-count="' + str(policy_failure_count) + '"')
					rpt += (' result-type="' + result_type + '"')
					rpt += (' sending-ip="' + sending_ip + '"')
					rpt += (' receiving-mx-hostname="' + receiving_mx_hostname + '"')
					rpt += (' receiving-mx-helo="' + receiving_mx_helo + '"')
					rpt += (' receiving-ip="' + receiving_ip + '"')
					rpt += (' failed-count="' + str(failed_session_count) + '"')
					rpt += (' additional-info="' + additional_info + '"')
					rpt += (' failure-error-code="' + failure_error_code + '"')
					if output_style == 'kv':
						print(rpt)
					else:
						rpt_fn = report_id + ".json.gz"
						rpt_bytes = rpt.encode('utf-8')
						with gzip.GzipFile(rpt_fn, 'w') as fout:
							fout.write(rpt_bytes)

				elif output_style in ('csv'):

					rpt = (process_time + csv_separator)
					rpt += (report_id + csv_separator)
					rpt += ('"' + organization_name + '"' + csv_separator)
					rpt += (start_date_time + csv_separator)
					rpt += (end_date_time + csv_separator)
					rpt += (contact_info + csv_separator)
					rpt += (policy_type + csv_separator)
					rpt += ('"' + csv_separator.join(policy_string) + '"' + csv_separator)
					rpt += (policy_domain + csv_separator)
					rpt += ('"' + policy_mx_host + '"' + csv_separator)
					rpt += (str(policy_success_count) + csv_separator)
					rpt += (str(policy_failure_count) + csv_separator)
					rpt += (result_type + csv_separator)
					rpt += (sending_ip + csv_separator)
					rpt += (receiving_mx_hostname + csv_separator)
					rpt += (receiving_mx_helo + csv_separator)
					rpt += (receiving_ip + csv_separator)
					rpt += (str(failed_session_count) + csv_separator)
					rpt += ('"' + additional_info + '"' + csv_separator)
					rpt += (failure_error_code)
					print(rpt)

				else:
					print ("Unrecognized output style")
		else:
			if output_style in (['kv', 'gzip-json']):

				rpt = ('process-time="' + process_time + '"')
				rpt += (' report-id="' + report_id + '"')
				rpt += (' organization-name="' + organization_name + '"')
				rpt += (' start-date-time="' + start_date_time + '"')
				rpt += (' end-date-time="' + end_date_time + '"')
				rpt += (' contact-info="' + contact_info + '"')
				rpt += (' policy-type="' + policy_type + '"')
				rpt += (' policy-string="' + ",".join(policy_string) + '"')
				rpt += (' policy-domain="' + policy_domain + '"')
				rpt += (' policy-mx-host="' + policy_mx_host + '"')
				rpt += (' policy-success-count="' + str(policy_success_count) + '"')
				rpt += (' policy-failure-count="' + str(policy_failure_count) + '"')
				if output_style == 'kv':
					print(rpt)
				else:
					rpt_fn = report_id + ".json.gz"
					rpt_bytes = rpt.encode('utf-8')
					with gzip.GzipFile(rpt_fn, 'w') as fout:
						fout.write(rpt_bytes)

			elif output_style in ('csv'):

				rpt = (process_time + csv_separator)
				rpt += (report_id + csv_separator)
				rpt += ('"' + organization_name + '"' + csv_separator)
				rpt += (start_date_time + csv_separator)
				rpt += (end_date_time + csv_separator)
				rpt += (contact_info + csv_separator)
				rpt += (policy_type + csv_separator)
				rpt += ('"' + csv_separator.join(policy_string) + '"' + csv_separator)
				rpt += (policy_domain + csv_separator)
				rpt += ('"' + policy_mx_host + '"' + csv_separator)
				rpt += (str(policy_success_count) + csv_separator)
				rpt += (str(policy_failure_count) + csv_separator)
				rpt += (result_type + csv_separator)
				print(rpt)

			else:
				print ("Unrecognized output style")
