import os, requests, time

import lib.currentconditions as current_conditions


from lib.apiutil import request_headers

def handleFail(err):
  # API call failed...
  print('Status code: %d' % (err.status_code) )

def callCurrentConditions(lat, lon, units = 'm'):
  url, params = current_conditions.request_options(lat, lon, language, units)
  headers = request_headers()
  r = requests.get(url, params=params, headers=headers)
  if r.status_code == 200:
    current_conditions.handle_response(r.json())
  else:
    handleFail(r)

loc = {
  'boston': { 'lat': '42.3600', 'lon': '-71.06536' }, # Boston, MA, United States
  'raleigh': { 'lat': '35.843686', 'lon': '-78.78548' }, # Raleigh, NC, United States
  'losangeles': { 'lat': '34.040873', 'lon': '-118.482745' }, # Los Angeles, CA, United States
  'lakecity': { 'lat': '44.4494119', 'lon': '-92.2668435' }, # Lake CIty, MN, United States
  'newyork': { 'lat': '40.742089', 'lon': '-73.987908' }, # New York, NY, United States
  'hawaii': { 'lat': '33.40', 'lon': '-83.42' }, # Hawaii, United States
  'puntacana': { 'lat': '18.57001', 'lon': '-68.36907' }, # Punta Cana, Dominican Republic
  'jakarta': { 'lat': '-5.7759349', 'lon': '106.1161341' } # Jakarta, Indonesia
}

########################
# Make a single API call
########################
language = 'en-US'
callCurrentConditions(loc['raleigh']['lat'], loc['raleigh']['lon'], language)
# callWeatherAlertHeadlines(loc['lakecity']['lat'], loc['lakecity']['lon'])
# callSevereWeatherPowerDisruption(loc['jakarta']['lat'], loc['jakarta']['lon'])
# callTropicalForecastProjectedPath()
# callWeatherAlertDetails('06439e88-320a-3722-ae90-097484ff2277')
