#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# __author__ = 'Julien Bernard'
# __email = 'julien.bernard@gmail.com'
# __description__ = 'Script to run queries using sumologic search job api'

# required modules
import logging
import argparse
import yaml
import urllib3
from datetime import datetime, timedelta
import re
import json
import time
import csv
import sys
from tabulate import tabulate

# logging 
logging.basicConfig(level = logging.INFO, 
                    format = '%(asctime)s | %(levelname)-10s | %(message)s', 
                    datefmt = '%Y-%m-%d %H:%M:%S')

# search class
class sumologicSearchQuery(object):
    def __init__(self, config):
        self.config = config
        
        self.api_base_url = self.config['sumologic_environment']['api_base_url']
        api_access_id = self.config['sumologic_environment']['api_access_id']
        api_access_key = self.config['sumologic_environment']['api_access_key']

        auth = "{}:{}".format(api_access_id, api_access_key)

        self.headers = urllib3.make_headers(basic_auth=auth)
        self.headers.setdefault('Content-Type', 'application/json')
        self.headers.setdefault('Accept', 'application/json')

        search_from = self.config['sumologic_search']['from']
        self.search_date_from = self.parse_variable_value("search_from", search_from)

        search_to = self.config['sumologic_search']['to']
        self.search_date_to = self.parse_variable_value("search_from", search_to)

        self.search_type = self.config['sumologic_search']['type']
        self.search_query = self.config['sumologic_search']['query']
        self.search_timeZone = self.config['sumologic_search']['timeZone']
        self.search_byReceiptTime = self.config['sumologic_search']['byReceiptTime']
        self.search_autoParsingMode = self.config['sumologic_search']['autoParsingMode']

        self.debug = self.config['processing']['debug']
        if(self.debug):
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.getLogger().setLevel(logging.CRITICAL)

        self.search_timeout = self.config['processing']['timeout']
        self.search_batch = self.config['processing']['batch']

        self.output_type = self.config['processing']['output_type']
        self.screen_max_cell_width = self.config['processing']['screen_max_cell_width']
        self.output_destination = self.config['processing']['output_destination']

        self.create_search_job()
        self.check_job_status()

        if(self.search_type == 'records'):
            self.download_records()
        elif(self.search_type == 'messages'):
            self.download_messages()
        else:
            logging.critical('Error: "{}" search type not supported!'.format(self.search_type))
            exit()

        self.get_fields()

        if(self.output_type == "screen"):
            self.export_screen()
        elif(self.output_type == "csv"):
            self.export_csv()
        else:
            logging.critical('Error: "{}" output format not supported!'.format(self.output_type))
            exit()

    def parse_variable_value(self, variable_name, variable_value):
        if variable_value == "now":
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        try:
            datetime.strptime(variable_value, "%Y-%m-%dT%H:%M:%S")
            return variable_value
        except ValueError:
            pass

        pattern = r'^-(\d+)([mhdw])$'
        match = re.match(pattern, variable_value)
        if match:
            quantity = int(match.group(1))
            unit = match.group(2)

            if unit == 'm':
                time_delta = timedelta(minutes=quantity)
            elif unit == 'h':
                time_delta = timedelta(hours=quantity)
            elif unit == 'd':
                time_delta = timedelta(days=quantity)
            elif unit == 'w':
                time_delta = timedelta(weeks=quantity)
            else:
                logging.critical('Invalid time unit "{}" for {}'.format(variable_value, variable_name))
                exit()

            new_time = datetime.now() - time_delta
            return new_time.strftime("%Y-%m-%dT%H:%M:%S")

        logging.critical('Invalid input format "{}" for {}'.format(variable_value, variable_name))
        exit()

    def create_search_job(self):
        logging.info('Creating search job')

        http = urllib3.PoolManager()
        request_url = "{}/v1/search/jobs".format(self.api_base_url)
        
        data = json.dumps({
            "query": self.search_query,
            "from": self.search_date_from,
            "to": self.search_date_to,
            "timeZone": self.search_timeZone,
            "byReceiptTime": self.search_byReceiptTime,
            "autoParsingMode": self.search_autoParsingMode
        })

        request = http.request('POST', request_url, headers=self.headers, body=data)
        request_status = request.status

        if request_status != 202:
            response = json.loads(request.data)
            logging.critical('Error: {} ({}): "{}"'.format(request_status, request.reason, response['message']))
            exit(0)
        
        else:
            response = json.loads(request.data)
            logging.info('Search job created: {}'.format(response['id']))
            
        self.search_job_id = response['id']

    def check_job_status(self):
        http = urllib3.PoolManager()
        request_url = "{}/v1/search/jobs/{}".format(self.api_base_url, self.search_job_id)

        logging.info('Checking search job {} status'.format(self.search_job_id))

        start_time = time.time()
        response = {'state': 'INITIAL'}

        while response['state'] != 'DONE GATHERING RESULTS':
            if time.time() - start_time > self.search_timeout:
                logging.critical('Error: timeout.')
                exit()

            request = http.request('GET', request_url, headers=self.headers)
            request_status = request.status

            if request_status != 200:
                response = json.loads(request.data)
                logging.critical('Error: {} ({}): "{}"'.format(request_status, request.reason, response['message']))
                exit(0)
            else:
                response = json.loads(request.data)
                logging.info('Search job status: {}'.format(response['state']))
                
            time.sleep(1)

        logging.info('Number of records: {} | messages: {}'.format(response['recordCount'], response['messageCount']))
        self.search_record_count = response['recordCount']
        self.search_message_count = response['messageCount']

    def download_records(self):
        offset = 0
        limit = self.search_batch
        all_records = []

        while len(all_records) < self.search_record_count:
            http = urllib3.PoolManager()
            request_url = "{}/v1/search/jobs/{}/records?offset={}&limit={}".format(self.api_base_url, self.search_job_id, offset, limit)

            logging.info('Downloading records for {}: from {} to {} (total: {})'.format(self.search_job_id, offset, (offset+limit), self.search_record_count))

            request = http.request('GET', request_url, headers=self.headers)
            request_status = request.status

            if request_status != 200:
                response = json.loads(request.data)
                logging.critical('Error: {} ({}): "{}"'.format(request_status, request.reason, response['message']))
                exit(0)

            else:
                response = json.loads(request.data)
                records = response.get('records', [])
                all_records.extend(records)

                logging.info('Downloaded {} records, total downloaded: {}'.format(len(records), len(all_records)))
                offset += len(records)
                
                if len(records) < limit:
                    break
        
        self.all_records = all_records

    def download_messages(self):
        offset = 0
        limit = self.search_batch
        all_messages = []

        while len(all_messages) < self.search_message_count:
            http = urllib3.PoolManager()
            request_url = "{}/v1/search/jobs/{}/messages?offset={}&limit={}".format(self.api_base_url, self.search_job_id, offset, limit)

            logging.info('Downloading messages for {}: from {} to {} (total: {})'.format(self.search_job_id, offset, (offset+limit), self.search_message_count))

            request = http.request('GET', request_url, headers=self.headers)
            request_status = request.status

            if request_status != 200:
                response = json.loads(request.data)
                logging.critical('Error: {} ({}): "{}"'.format(request_status, request.reason, response['message']))
                exit(0)

            else:
                response = json.loads(request.data)
                messages = response.get('messages', [])
                all_messages.extend(messages)

                logging.info('Downloaded {} messages, total downloaded: {}'.format(len(messages), len(all_messages)))
                offset += len(messages)
                
                if len(messages) < limit:
                    break
        
        self.all_messages = all_messages
    
    def get_fields(self):
        http = urllib3.PoolManager()

        if(self.search_type == 'records'):
            request_url = "{}/v1/search/jobs/{}/records?offset=0&limit=1".format(self.api_base_url, self.search_job_id)
        elif(self.search_type == 'messages'):
            request_url = "{}/v1/search/jobs/{}/messages?offset=0&limit=1".format(self.api_base_url, self.search_job_id)
        else:
            logging.critical('Error: "{}" search type not supported!'.format(self.search_type))
            exit() 


        logging.info('Getting list of fields for {}'.format(self.search_job_id))

        request = http.request('GET', request_url, headers=self.headers)
        request_status = request.status

        if request_status != 200:
            response = json.loads(request.data)
            logging.critical('Error: {} ({}): "{}"'.format(request_status, request.reason, response['message']))
            exit(0)

        else:
            response = json.loads(request.data)
            fields = response.get('fields', [])
            self.results_fields = fields

    def export_screen(self):
        table = []
        max_width = self.screen_max_cell_width

        header_row = [field['name'] for field in self.results_fields]
        table.append(header_row)

        def truncate(content):
            return (content if len(content) <= max_width else content[:max_width-3] + '...')

        if(self.search_type == 'records'):
            for record in self.all_records:
                row = [truncate(str(record['map'][field['name']])) for field in self.results_fields]
                table.append(row)
        elif(self.search_type == 'messages'):
            for message in self.all_messages:
                row = [truncate(str(message['map'][field['name']])) for field in self.results_fields]
                table.append(row)
        else:
            logging.critical('Error: "{}" search type not supported!'.format(self.search_type))
            exit()     

        print(tabulate(table, headers="firstrow", tablefmt="fancy_grid"))

    def export_csv(self):
        with open(self.output_destination, 'w', newline='') as file:
            writer = csv.writer(file)
        
            header_row = [field['name'] for field in self.results_fields]
            writer.writerow(header_row)

            for record in self.all_records:
                row = [record['map'][field['name']] for field in self.results_fields]
                writer.writerow(row)

# main
def main(config_file):
    try:
        with open(config_file, 'r') as config:
            config = yaml.safe_load(config)
    except Exception as e:
        logging.critical('Error while loading configuration file: {}'.format(e))
        exit()

    try:
        search = sumologicSearchQuery(config=config)
    except Exception as e:
        logging.critical('Error while creating new Sumologic search: {}'.format(e))
        exit()

# main script execution
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to run queries against Sumologic Search Job API')

    required = parser.add_argument_group('required arguments')
    required.add_argument('-c', '--config', type=str, nargs=1, help='Configuration file', required=True)
    args = parser.parse_args()
    config_file = args.config[0]

    main(config_file)

