"""
This helper script contains the

Author: Qingshi

Developed based on: 
	- https://doi.org/10.1039/D0SE01637C

[CAUTIONS]
	- use xlrd==1.2.0, v.2.0 and later do not support .xlsx file
"""

"""
================
Import libraries 
===============
"""
import brightway2 as bw
import pandas as pd
import numpy as np
from bw2io.importers.base_lci import LCIImporter
from bw2io.strategies import add_database_name, csv_restore_tuples
from functools import partial
import logging
from copy import deepcopy
import os
import json
from config import db_mgmt_config as config
from typing import List, Dict, Tuple

import xlrd
from xlrd import open_workbook
# workaround for the 'AttributeError: 'ElementTree' object has no attribute 'getiterator'': https://stackoverflow.com/questions/64264563/attributeerror-elementtree-object-has-no-attribute-getiterator-when-trying
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True

class MultiColImporter:
	"""
	creates an importer object to handle multiple columns (e.g., multiple entries of amount for each row of LCI)
	"""

	def __init__(self, wb_path, db_name: str, multicol_start: int, exc_row_start: int, default_proc_attr_dict: Dict):
		# store attributes
		self.db_name = db_name
		self.exc_row_start = exc_row_start
		self.default_proc_attr_dict = default_proc_attr_dict
		self.multicol_start = multicol_start

		# load the worksheet of interest
		try:
			self.ws = open_workbook(wb_path).sheet_by_name("db_to_import")
		except KeyError: # if no such sheet, raise the exception
			print("[ERROR] please make sure the 'db_to_import' sheet is included in the workbook")

		# collect exchange metadata labels
		self.exchange_metadata_labels = [self.ws.cell(1, y).value for y in range(self.multicol_start)] 

		# initiate an importer and configure importor strategies
		self.importer = LCIImporter(self.db_name)
		self.importer.strategies.append(partial(add_database_name, name=self.db_name))

		# gets or creates a logger
		self.logger = logging.getLogger(__name__)  

		# set log level
		self.logger.setLevel(logging.INFO)

		# define file handler and set formatter
		log_output_path = os.path.sep.join([config.LOG_OUTPUT_PATH,'Multiple-column importing.log'])
		file_handler = logging.FileHandler(log_output_path)
		formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
		file_handler.setFormatter(formatter)

		# add file handler to logger
		self.logger.addHandler(file_handler) 

		# get the list of locations from geodata.json (downloaded from bw2io.data.lci, see bw2io github repo)
		with open(os.path.sep.join([config.BASE_PATH,'geodata.json']),'r') as f:
			self.loc_lst = json.load(f)['names']


	def get_exchanges(self, amt_column: int):
		# initiate list of exchanges
		exchanges = []

		for row in range(self.exc_row_start, self.ws.nrows):
			data = dict(zip(self.exchange_metadata_labels, [self.ws.cell(row, col).value for col in range(len(self.exchange_metadata_labels))]))
			data['amount'] = self.ws.cell(row, amt_column).value #don't forget to put zero for the row where exc is not part of the process
			exchanges.append(data)

		return exchanges


	def create_process(self, proc_name_column: int):
		"""
		Arguments:
			- proc_name_column: the column containing the name of process to be created (the same col contains the amt of 
				product and exchanges for this particular process)
		"""

		# get process name and exchanges
		_name = self.ws.cell(0, proc_name_column).value # [caution] hardcode: first row in the worksheet has to be reserved for proc names
		_code = _name
		_exchanges = self.get_exchanges(proc_name_column)

		# create the process by updating the default process attribute dict (which comes from config file)
		tmp_dict = {
			'name': _name,
			'code': _code,
			'exchanges': _exchanges,
			#'database': self.db_name,
			}
		# update location if information is present in the name
		try:
			for str_ in _name.strip().split(","): # [caution] location is not validated (i.e., may cause unlinked exc if used as exc in other activities)
				if str_.strip() in self.loc_lst:
					tmp_dict['location'] = str_.strip()
		except:
			# possible exceptions: no ',' is used in _name
			pass

		proc_created = {**self.default_proc_attr_dict, **tmp_dict}

		# log the process and its exchanges
		self.logger.info(f"==== RECORDING EXCHANGE FOR {proc_created['name']} ====")
		for exc_dict in proc_created['exchanges']:
			self.logger.info(exc_dict)
		self.logger.info(" ")

		return proc_created


	def buildNimport_db(self, db_match_dict: Dict):
		"""
		builds the database by looping over the columns of interest (e.g., 10 different 'amt' for each exchange)
		Arguments:
			- db_match_dict: a dict storing the database name and fields to match, {db_name:('field_1','field_2',...)} 
			 [Caution] the exchanges to be imported HAVE TO be from other db, not from this particular db being built
		"""

		self.importer.data = [self.create_process(column) for column in range(self.multicol_start, self.ws.ncols)]

		# apply strategies and match db
		self.importer.apply_strategies()
		for db_to_match_name,fields_to_match in db_match_dict.items():
			if db_to_match_name=='self':
				self.importer.match_database(fields=fields_to_match) #link within the foreground processes
				print("SELF MATCHING DONE")
			else:
				self.importer.match_database(db_to_match_name,fields=fields_to_match) #match processes in other db
		
		return self.importer

		# write database
		#self.importer.write_database()
		#self.importer.statistics()
		#self.imported_multicol_db=Database(self.db_name)



