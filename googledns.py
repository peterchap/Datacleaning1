from publicdns.client import PublicDNS

client = PublicDNS()
result = client.query("www.google.com", "A")
ip = client.resolve("www.google.com")
print(ip)
