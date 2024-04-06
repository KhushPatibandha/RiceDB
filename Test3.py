import requests
import json
import random
import string

base_url = 'http://localhost:8080/api'

# Mass insert
for i in range(1, 5001):
    random_letter = random.choice(string.ascii_letters)
    pair = {'key': random_letter + str(i), 'value': random_letter + str(i)}
    response = requests.post(f'{base_url}/insert', data=json.dumps(pair), headers={'Content-Type': 'application/json'})
    print(response.text)