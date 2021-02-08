
import chardet
import requests
import pytz
from datetime import datetime
import os
from pathlib import Path

TIMEZONE = pytz.timezone('America/Montreal')

with open('tableau-rpa-new.csv', 'rb') as file:
	print()


resp = requests.get('https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/ciusss.csv')

#print('\n\n\n', resp.content)

#print('\n\n\n', resp.content.decode('cp1252'))


date_tag = datetime.now(tz=TIMEZONE).date().isoformat()


#rint(os.path.join(os.path.dirname(__file__), date_tag))
allfiles = Path(__file__).parent.glob('*')

#sortedfiles = sorted(allfiles)
file = sorted(allfiles)
print(file[-1])