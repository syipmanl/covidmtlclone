""" Refresh data files for the COVID-19 MTL dashboard """

import datetime as dt
# import logging
import io
import os
import shutil
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path

import dateparser
import numpy as np
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

pd.options.mode.chained_assignment = None

DATA_DIR = os.path.join(os.path.dirname(__file__), 'app', 'data')
NB_RETRIES = 3
TIMEZONE = pytz.timezone('America/Montreal')
# Data sources mapping
# {filename: url}
# Montreal
SOURCES_MTL = {
    # HTML
    'data_mtl.html':
    'https://santemontreal.qc.ca/en/public/coronavirus-covid-19/situation-of-the-coronavirus-covid-19-in-montreal',
    # CSV (Note: ";" separated; Encoding: windows-1252/cp1252)
    'data_mtl_ciuss.csv':
    'https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/ciusss.csv',
    'data_mtl_municipal.csv':
    'https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/municipal.csv',
    'data_mtl_age.csv':
    'https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/grage.csv',
    'data_mtl_sex.csv':
    'https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/sexe.csv',
    'data_mtl_new_cases.csv':
    'https://santemontreal.qc.ca/fileadmin/fichiers/Campagnes/coronavirus/situation-montreal/courbe.csv',
}

# INSPQ
SOURCES_INSPQ = {
    # HTML
    'INSPQ_main.html': 'https://www.inspq.qc.ca/covid-19/donnees',
    'INSPQ_region.html': 'https://www.inspq.qc.ca/covid-19/donnees/regions',
    'INSPQ_par_region.html': 'https://www.inspq.qc.ca/covid-19/donnees/par-region',
    # CSV (Note: "," separated; Encoding: UTF-8)
    'data_qc.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/covid19-hist.csv',
    'data_qc_regions.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/regions.csv',
    'data_qc_manual_data.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/manual-data.csv',
    'data_qc_cases_by_network.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/tableau-rls-new.csv',
    'data_qc_death_loc_by_region.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/tableau-rpa-new.csv',
    # updated once a week on Tuesdays
    'data_qc_preconditions.csv': 'https://www.inspq.qc.ca/sites/default/files/covid/donnees/comorbidite.csv',
}

# Quebec.ca/coronavirus
SOURCES_QC = {
    # HTML
    'QC_situation.html':
    'https://www.quebec.ca/en/health/health-issues/a-z/2019-coronavirus/situation-coronavirus-in-quebec/',
    'QC_vaccination.html':
    'https://www.quebec.ca/en/health/health-issues/a-z/2019-coronavirus/situation-coronavirus-in-quebec/covid-19-vaccination-data/',  # noqa: E501
    # CSV
    'data_qc_outbreaks.csv':
    'https://cdn-contenu.quebec.ca/cdn-contenu/sante/documents/Problemes_de_sante/covid-19/csv/eclosions-par-milieu.csv',  # noqa: E501
    'data_qc_vaccines.csv':
    'https://cdn-contenu.quebec.ca/cdn-contenu/sante/documents/Problemes_de_sante/covid-19/csv/doses-vaccins.csv',
    'data_qc_vaccines_received.csv':
    'https://cdn-contenu.quebec.ca/cdn-contenu/sante/documents/Problemes_de_sante/covid-19/csv/doses-vaccins-7jours.csv',  # noqa: E501
    'data_qc_vaccines_situation.csv':
    'https://cdn-contenu.quebec.ca/cdn-contenu/sante/documents/Problemes_de_sante/covid-19/csv/situation-vaccination-en.csv',  # noqa: E501
    'data_qc_7days.csv':
    'https://cdn-contenu.quebec.ca/cdn-contenu/sante/documents/Problemes_de_sante/covid-19/csv/synthese-7jours.csv',
}

def fetch(url):
	"""Get the data at 'url'. Unreliable sources, so retry few times.

	Parameters
	------
	url : str
		URL of data to fetch (csv or html file).

	Returns
	------
	str
		utf-8 or cp1252 decoded string
	
	Raises
	------
	RuntimeError
		Failed to retrieve data from URL.
	"""

	for _ in range(NB_RETRIES):
		resp = requests.get(url)
		if resp.status_code != 200:
			continue

		# try utf-8 first,
		# then cp-1252

		try: 
			return resp.content.decode('utf-8')
		except UnicodeDecodeError:
			return resp.content.decode('cp1252')
		
		raise RuntimeError('Failed to retrieve {}'.format(url))

def save_datafile(filename, data):
	"""Save 'data' to 'filename'

	Parameters
	---------
	filename : str
		Absolute path of file where data is to be saved.
	data : str
		Data to be saved
	"""

	with open(filename, 'w', encoding='utf-8') as f:
		f.write(data)


def backup_processed_dir(processed_dir, processed_backups_dir):
	"""Copy all files from data/processed to data/processed_backups/YYYY-MM-DD{_v#}

	Parameters
	--------
	processed_dir: dict
		Absolute path of dir that contains processed files to backup.
	processedbackups_dir : str
		Absolute path of dir in which to save the backup.
	"""
	date_tag = datetime.now(tz=TIMEZONE).date().isoformat()

	# make backup dir
	current_bkp_dir = os.path.join(processed_backups_dir, date_tag)
	i = 1
	while os.path.isdir(current_bkp_dir):
		i+= 1
		current_bkp_dirname = date_tag + '_v' + str(i)
		current_bkp_dir = os.path.join(processed_backups_dir, current_bkp_dirname)
	else:
		os.mkdir(current_bkp_dir)

	# Copy all files from data/processed to data/processed_backups/YYYY-MM-DD{_v#}
	for file in os.listdir(processed_dir):
		filepath = os.path.join(processed_dir, file)
		shutil.copy(file_path, current_bkp_dir)

def download_source_files(sources, sources_dir, version=True):
	"""Download files from URL

	Downloaded files will be downloaded into data/sources/YYYY-MM-DD{_v#}/

	Parameters
	----------
	sources : dict
		dict in the format {filename:source} where source is a URL and filename is the
		name of the file in which to save the downloaded data.
	sources_dir : str
		Absolute path of dir in which to save downloaded files.
	version : bool
		True, if source directories should be versioned if sources for the same date already exist,
		False otherwise
	"""
	# create data/sources/YYYY-MM-DD{_v#}/ dir, us previous day date (data is rpeorted for previous day)
	yesterday_date = datetime.now(tz=TIMEZONE) - timedelta(days=1)
	date_tag = yesterday_date.date().isoformat()

	current_sources_dir = Path(sources_dir, date_tag)

	if version:
		i = 1
		while current_sources_dir.is_dir():
			i += 1
			current_sources_dirname = date_tag + '_v' + str(i)
			current_sources_dir = current_sources_dir.parent.joinpath(current_sources_dirname)

	current_sources_dir.mkdir(exist_ok=True)

	# Download all source data files to sources dir
	for file, url in sources.items():
		data = fetch(url)
		fq_path = current_sources_dir.joinpath(file)

		if not fq_path.exists():
			save_data(fq_path, data)
		else:
			raise TypeError(f'{fq_path} already exists')

def get_latest_source_dir(sources_dir):
	"""Get the latest source dir in data/sources.

	Parameters
	----------
	sources_dir : str
		Absolute path of sources dir.

	Returns
	-------
	str
		Name of latest sources dir (e.g. 2020-06-01_v2) in data/sources/
	"""
	source_dirs = os.listdir(sources_dir)
	source_dirs.sort()
	latest_source_dir = source_dirs[-1]

	return latest_source_dir

def get_latest_dir_for_date(sources_dir, date):
	"""Get the last source dir in data/sources for the given date.

	Parameters
	----------
	sources_dir : str
		Absolute path of sources dir.
	date : str
		ISO-8601 formatted date string.

	Returns
	-------
	str
		Name of latest source dir in data/sources/

	Returns
	-------
	str
		Name of latest source dir (e.g. 2020-06-01_v2) in data/sources/
	"""

	# get all directories with starting with the date
	source_dirs = [directory for directory in Path(sources_dir).glob(f'{date}*/')]

	# take the last one from the sorted lsit
	latest_source_dir = sorted(source_dirs)[-1]

	return latest_source_dir.name

def is_new_inspq_data_available(expected_date: dt.date):
	"""Returns whether new data provided by INSPQ is available.
	Data is available if the data's last date is equal the given expected ate.

	Parameters
	----------
	expected_date : date
		the date that new data needs to have to be considered new.

	Returns
	-------
	bool
		True, if new data is available, False otherwise
	"""
	# check the manual CSV with the date provided in it 




























