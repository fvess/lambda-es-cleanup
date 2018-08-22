from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.vendored import requests
from urllib.parse import urlparse, parse_qs, urlencode
from datetime import timedelta, datetime
from dateutil.parser import parse
import json
import os

# AWS
session = session.Session()
credentials = session.get_credentials()
region = os.environ['AWS_REGION']

# ES
hostname = os.environ['ENDPOINT']  # elasticsearch endpoint
period = int(os.environ['PERIOD']) # retention period in days


def handler(event, context):
    endpoint = 'https://' + hostname if not hostname.startswith(('http://', 'https://')) else hostname
    url = endpoint + '/' if not endpoint.endswith('/') else endpoint
    auth = AWSV4Sign(credentials, region, 'es')
    response = requests.get(url + '_cat/indices?format=json', auth=auth)
    payload = json.loads(response.text)
    indices = [element['index'] for element in payload]

    old_indices = 0
    for index in indices:
        try:
            old_date = datetime.now() - timedelta(days=period)
            idx_date = parse(index, fuzzy=True)
        except:
            print('Skipping index: %s (its name does not contain a date)' % index)
            continue
        if idx_date <= old_date:
            print('Deleting index: %s' % index)
            requests.delete(url + index, auth=auth)
            old_indices += 1

    if old_indices:
        print('Total indices deleted: %d' % old_indices)
    else:
        print('There are no indices older than %s days' % period)


class AWSV4Sign(requests.auth.AuthBase):
    def __init__(self, credentials, region, service):
        if not region:
            raise ValueError("You must supply an AWS region")
        self.credentials = credentials
        self.region = region
        self.service = service

    def __call__(self, r):
        url = urlparse(r.url)
        path = url.path or '/'
        querystring = ''
        if url.query:
            querystring = '?' + urlencode(parse_qs(url.query, keep_blank_values=True), doseq=True)
        safe_url = url.scheme + '://' + url.netloc.split(':')[0] + path + querystring
        request = AWSRequest(method=r.method.upper(), url=safe_url, data=r.body)
        SigV4Auth(self.credentials, self.service, self.region).add_auth(request)
        r.headers.update(dict(request.headers.items()))
        return r
