import http.client

conn = http.client.HTTPSConnection("therundown-therundown-v1.p.rapidapi.com")

headers = {
    'x-rapidapi-host': "therundown-therundown-v1.p.rapidapi.com",
    'Content-Type': "application/json"
}

conn.request("GET", "/sports/1/conferences", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))