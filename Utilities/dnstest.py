import polars as pl

import pubdns as pub

# df = pl.read_csv("data.csv")
result = pub.dns.resolver("amazon.com", ["A"])
for r in result:
    print(r)
