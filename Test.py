import requests
import json

base_url = 'http://localhost:8080/api'

# Mass insert
for i in range(1, 10001):
    pair = {'key': str(i), 'value': str(i)}
    response = requests.post(f'{base_url}/insert', data=json.dumps(pair), headers={'Content-Type': 'application/json'})
    print(response.text)

# Mass delete
for i in range(1, 3001):
    response = requests.delete(f'{base_url}/delete', params={'key': str(i)})
    print(response.text)

# Mass update
for i in range(3001, 5001):
    pair = {'key': str(i), 'value': str(i + 1)}
    response = requests.put(f'{base_url}/update', data=json.dumps(pair), headers={'Content-Type': 'application/json'})
    print(response.text)

# Get values
keys = ["4999", "5000", "1", "1854", "185", "184", "859", "848", "849", "867", "997", "998", "999", "2188", "3118", "3101", "3111", "4048", "4022", "4028", "4208", "4232", "4233", "4234", "4235", "5001", "5010", "5036", "1511", "156", "9000", "9001"]
for key in keys:
    response = requests.get(f'{base_url}/get', params={'key': key})
    print(response.text)