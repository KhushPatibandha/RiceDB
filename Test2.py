import requests
import json

base_url = 'http://localhost:8080/api'  # replace with your server's address

# Recover log
response = requests.put(f'{base_url}/recover')
print(response.text)

# Get values
keys = ["4999", "5000", "1", "1854", "185", "184", "859", "848", "849", "867", "997", "998", "999", "2188", "3118", "3101", "3111", "4048", "4022", "4028", "4208", "4232", "4233", "4234", "4235", "5001", "5010", "5036", "1511", "156", "9000", "9001"]
for key in keys:
    response = requests.get(f'{base_url}/get', params={'key': key})
    print(response.text)