from github import Github

token = "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"
g = Github(token)


repo = g.get_repo("woomai/whois-servers")

contents = repo.get_contents("/list.json")

decoded = contents.decoded_content
for line in decoded.splitlines():
    # print(line)
    print(line.decode("utf-8").strip())
# print(decoded)
"""
with open('dataset.csv', 'wb') as f:
    f.write(decoded)
"""
