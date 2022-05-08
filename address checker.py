from geopy.geocoders import Nominatim
import postcodes_io_api

api  = postcodes_io_api.Api(debug_http=False)
x = api.get_postcode('BN214HR')

a = str(x['result']['latitude'])
b = str(x['result']['longitude'])
c= a +"," + b
print(a,b)

geolocator = Nominatim(user_agent="peter.test")
location = geolocator.reverse(c)
print(location.address)
