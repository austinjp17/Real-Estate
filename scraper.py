import requests

payload = { 
        'api_key': '0861bae719981ddf7ae64ddfcb5193ad', 
        'url': 'https://www.redfin.com/zipcode/77532' 
    } 
r = requests.get('https://api.scraperapi.com/', params=payload)
print(r.text)
