import dns.resolver

answers = dns.resolver.query('xtremetrainingacademy.org', 'MX')
for rdata in answers:
    print('Host', rdata.exchange)