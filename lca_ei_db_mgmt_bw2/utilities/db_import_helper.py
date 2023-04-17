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


class SeqImporter:
	"""
	Methods of this class:
		- import_bkgr_db: import one or more background databases (e.g., ecoinvent3.5 in EcoSpold2 format, customized db using bw2 template)
		- import_foreground_db: import a foreground database (e.g., inventory table of a product system of interest)

	"""


	def __init__ (self,project_name: str):
		
		"""
		=================
		set up the logger
		=================
		"""
		# gets or creates a logger
		self.logger = logging.getLogger(__name__)  

		# set log level
		self.logger.setLevel(logging.INFO)

		# define file handler and set formatter
		log_output_path = os.path.sep.join([config.LOG_OUTPUT_PATH,'log_seq_import.log'])
		file_handler = logging.FileHandler(log_output_path)
		formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
		file_handler.setFormatter(formatter)

		# add file handler to logger
		self.logger.addHandler(file_handler)  


		"""
		===========================
		create a brightway2 project
		===========================
		"""
		self.project_name = project_name
		projects.set_current(self.project_name)
		#print (projects.current)
		
		# set up default dataset (biosphere3) and LCIA methods for current project
		# code below is almost the same as bw2io.bw2setup(), other than changing the argument of 'overwrite' to True to avoid
		# the error msg when you have impact assessment methods installed from previous setup
		if "biosphere3" in databases:
			print("Biosphere database already present!!! No setup is needed")
		else:
			print("Creating default biosphere\n")
			create_default_biosphere3()
			print("Creating default LCIA methods\n")
			create_default_lcia_methods(overwrite=True)
			print("Creating core data migrations\n")
			create_core_migrations()

		
		# get a copy of databases already imported
		self.imported_db_lst = list(databases)
		print(f"already imported databases: {self.imported_db_lst}")


		"""
		====================
		initiate db mgmt obj
		====================
		"""
		self.db_mgmt_obj = DB_mgmt(self.project_name) #initiate the DB_mgmt object will print the already imported db again


	def import_bkgr_db (self,db_path_name_dict: Dict, db_match_dict: Dict):
		
		"""
		==============================
		import database(s) of interest
		==============================
		Params:
			- db_path_name_dict: a dict storing name, path and type of the db to import, {db_name: (db_path, db_format, option_label)}
			- db_match_dict: a dict storing the database name and fields to match, {db_name:('field_1','field_2',...)}
		"""
		
		# import individual databases
		# [caution] it is NOT guaranteed that "ground lvl" db (e.g., ecoinvent) is installed before customized db (which relies on it)
		# 			which may lead to error --> so need to explicitly import "ground lvl" db in a separate call first
		for db_name, (db_path, db_format, option_label) in db_path_name_dict.items():
			if db_name in self.imported_db_lst: # skip this db if it is already imported
				print(f"DATABASE {db_name} has been imported already!!!")
				continue
			elif db_format.lower() == 'ecospold2':
				import_obj = SingleOutputEcospold2Importer(db_path,db_name, use_mp=False)
				import_obj.apply_strategies()
			elif db_format.lower() == 'bw2 template':
				if option_label == None:
					import_obj = ExcelImporter(db_path)
					import_obj.apply_strategies()
					# need to match database
					for db_to_match_name,fields_to_match in db_match_dict.items():
						if  db_to_match_name == 'self':
							import_obj.match_database(fields=fields_to_match) #link with processes in other tabs, if any
						else:
							import_obj.match_database(db_to_match_name,fields=fields_to_match) #match processes in other db
				elif option_label.lower() == "multicolumn":
					# use the helper function to handle multiple columns (e.g., multiple amounts for the same LCI row)
					import_obj = MultiColImporter(db_path, db_name, config.MULTICOL_START, config.EXC_ROW_START, 
													config.DEFAULT_PROC_ATTR_DICT)
					import_obj = import_obj.buildNimport_db(db_match_dict)

			# write database
			try:
				import_obj.statistics()
				import_obj.write_database()
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db
			except bw2data.errors.InvalidExchange:
				print("exception for InvalidExchange is raised!!!")
				self.logger.info("exception for InvalidExchange is raised!!!")
				# log the unlinked exchanges
				self.logger.info(f"the file path to the record of unlinked exchanges: {write_lci_matching(import_obj,db_name,only_unlinked=True)}")
				# remove the db from bw.databases
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db first
				self.db_mgmt_obj.remove_db(db_name)
			except Exception: # catch all other exceptions 
				exceptiondata = traceback.format_exc().splitlines()
				exceptionarray = [exceptiondata[-1]] + [exceptiondata[-2]] #get the error msg and last line of traceback (where the error occured)
				print(f"[ERROR msg] {exceptionarray}")
				# remove the db from bw.databases
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db first
				self.db_mgmt_obj.remove_db(db_name)

		

		# log all the db loaded
		self.logger.info("=== DATABASE IMPORTED ===")
		self.logger.info(list(databases))
		self.logger.info(" ")


	def import_foreground_db (self, foreground_db_path_name_dict: Dict, foreground_db_match_dict: Dict):
		
		"""
		=========================================
		import foreground data of the LCA project
		=========================================
		Params:
			- foreground_db_path_name_dict: a dict storing name, path and type of the foreground db to import, {db_name: (db_path, db_format, option_label)}
			- foreground_db_match_dict: a dict storing the database name and fields to match, {db_name:('field_1','field_2',...)}
		[Caution]:
			- this method is intended for importing ONE foreground db at a time
		"""
		
		# uppack the tuple from the foreground_db_path_name_dict
		foreground_db_name = list(foreground_db_path_name_dict.keys())[0] # [caution] this assumes there is only ONE foreground db to be imported
		db_path, db_format, option_label = list(foreground_db_path_name_dict.values())[0] # need to convert the 'dict_value' object to a list first

		# check if foreground db has already been imported
		self.imported_db_lst = list(databases) # update the list of db first
		if foreground_db_name in self.imported_db_lst: # skip this db if it is already imported
				print(f"DATABASE {foreground_db_name} has been imported already!!!")
		else:
			import_foreground_obj=ExcelImporter(db_path)
			import_foreground_obj.apply_strategies()

			for db_to_match_name,fields_to_match in foreground_db_match_dict.items():
				if db_to_match_name=='self':
					import_foreground_obj.match_database(fields=fields_to_match) #link within the foreground processes
				else:
					import_foreground_obj.match_database(db_to_match_name,fields=fields_to_match) #match processes in other db
			import_foreground_obj.statistics()

			try:
				import_foreground_obj.write_database()
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db
			except bw2data.errors.InvalidExchange:
				print("exception for InvalidExchange is raised!!!")
				self.logger.info("exception for InvalidExchange is raised!!!")
				# log the unlinked exchanges
				self.logger.info(f"the file path to the record of unlinked exchanges: {write_lci_matching(import_foreground_obj,foreground_db_name,only_unlinked=True)}")
				# remove the db from bw.databases
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db first
				self.db_mgmt_obj.remove_db(foreground_db_name)
			except Exception: # catch all other exceptions 
				exceptiondata = traceback.format_exc().splitlines()
				exceptionarray = [exceptiondata[-1]] + [exceptiondata[-2]] #get the error msg and last line of traceback (where the error occured)
				print(f"[ERROR msg] {exceptionarray}")
				# remove the db from bw.databases
				self.db_mgmt_obj.imported_db_lst = list(databases) # update the list of db first
				self.db_mgmt_obj.remove_db(db_name)

		# log all the db loaded
		self.logger.info("=== DATABASE IMPORTED ===")
		self.logger.info(list(databases))
		self.logger.info(" ")

		# prepare the foregound db for lca calculation
		self.foreground_db=Database(foreground_db_name)


class MultiColImporter:
	"""
	creates an importer object to handle multiple columns (e.g., multiple entries of amount for each row of LCI) in a spreadsheet of invenotry table
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



