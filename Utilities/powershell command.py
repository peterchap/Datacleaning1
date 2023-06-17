import subprocess,sys

p = subprocess.run(["powershell", "Resolve-DnsName google.com -Type MX | Get-Member"],capture_output=True)
x=p.stdout
print(x)




