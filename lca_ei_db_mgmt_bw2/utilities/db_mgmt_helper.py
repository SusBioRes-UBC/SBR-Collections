"""
This script helps to manage the dbs of a LCA project

Author: Qingshi

"""

"""
===============
Import packages
===============
"""
import brightway2 as bw
from bw2io.export.excel import write_lci_excel
import logging
from config import lohc_config as config
import os


class DB_mgmt:
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
		log_output_path = os.path.sep.join([config.LOG_OUTPUT_PATH,'db_mgmt.log'])
		file_handler = logging.FileHandler(log_output_path)
		formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
		file_handler.setFormatter(formatter)

		# add file handler to logger
		self.logger.addHandler(file_handler)  


		"""
		===========================================
		point to the brightway2 project of interest
		===========================================
		"""
		self.project_name = project_name
		bw.projects.set_current(project_name)
		#print (projects.current)

		# get a copy of databases already imported
		self.imported_db_lst = list(bw.databases)
		print(f"already imported databases: {self.imported_db_lst}")


	def remove_db(self, db_name: str):
		"""
		removes the db of interest from current project
		"""
		
		# make sure the database list is up-to-date
		self.imported_db_lst = list(bw.databases)

		if db_name in self.imported_db_lst:
			del bw.databases[db_name]
			# additional step to deregister method is needed, otherwise brightway will complain about 
			# the methods you are trying to save already existing, and quit: 
			# https://stackoverflow.com/questions/43938614/update-brightway-without-changing-project
			if db_name.lower() == 'biosphere3': 
				for m in list(bw.methods):
					bw.Method(m).deregister()
			print(f"database {db_name} has been removed")
		else:
			print(f"[CAUTION] database {db_name} does not exist")

		# log the change
		self.logger.info(f"database {db_name} has been removed")
		self.logger.info(f"remaining databases in current project {self.project_name} are: ")
		self.logger.info(bw.databases)
		self.logger.info(" ")


	def purge_db(self):
		"""
		removes ALL the imported db from current project
		"""

		# make sure the database list is up-to-date
		self.imported_db_lst = list(bw.databases)

		# remove all imported db
		for db_name in self.imported_db_lst:
			del bw.databases[db_name]

		print(f"BEFORE purge, these db are imported {self.imported_db_lst}")

		# update the database list
		self.imported_db_lst = list(bw.databases)

		print(f"AFTER purge, these db are left {self.imported_db_lst} -> should be an empty list")


	def export_lci_to_excel(self, db_name: str):
		"""
		exports the lci database into an Excel spreadsheet
		"""

		# make sure the database list is up-to-date
		self.imported_db_lst = list(bw.databases)

		if db_name in self.imported_db_lst:
			# log the file path of the exported spreadsheet
			self.logger.info(f"exported_file_path is {write_lci_excel(db_name)}")
		else:
			print(f"[CAUTION] database {db_name} does not exist")









