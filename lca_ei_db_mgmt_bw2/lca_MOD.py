"""
This is the LCA module

@author: Qingshi Tu
"""

"""
===============
import packages
===============
"""
from brightway2 import*
from bw2io.export.excel import write_lci_matching
from bw2analyzer import ContributionAnalysis
import bw2data
import stats_arrays # documentation of this package: https://stats-arrays.readthedocs.io/en/latest/
import collections
import pandas as pd
import numpy as np
import logging
from config import db_mgmt_config as config
from utilities.db_import_helper import MultiColImporter
from utilities.db_mgmt_helper import DB_mgmt
import os
import traceback
import progressbar
import uuid
import pickle
from typing import List, Dict, Tuple

class LCA_MOD:
	"""
	Methods of this class:
	- import_bkgr_db: import one or more background databases (e.g., ecoinvent3.5 in EcoSpold2 format, customized db using bw2 template)

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
		log_output_path = os.path.sep.join([config.LOG_OUTPUT_PATH,'log_LCA_calculation.log'])
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

	def calc_lca (self,lcia_methods: List,db: str,FU_activity_code='ThisIsFU',amount_FU=1,calc_done=False):
		"""
		Params:
			- lcia_methods: a list of LCIA methods of interest: [(method1),(method2)...]
			- db: name of the foreground db of interest
			- calc_done: a label to indicate if lca calucation has ever been performed
		"""


		"""
		=====================
		calculate LCA results
		=====================
		"""
		self.FU_activity_code=FU_activity_code
		self.FU_activity=[act for act in db if act['code']==self.FU_activity_code][0]
		self.amount_FU=amount_FU
		self.lcia_methods=lcia_methods
		self.calc_done=calc_done
	   
		# create dict to store: (1) LCA results, (2) top processes (including backgr db)
		self.LCA_results_dict={}
		self.top_processes_dict={}
		
		# create a ContributionAnalysis object
		self.contribut_anal_obj=ContributionAnalysis()

		for method in self.lcia_methods:
			self.lca=LCA({self.FU_activity:self.amount_FU},
				method)
			self.lca.lci()
			self.lca.lcia()
			self.LCA_results_dict[method]=self.lca.score
			self.top_processes_dict[method]=self.contribut_anal_obj.annotated_top_processes(self.lca) #'.annotated_top_processes' returns a list of tuples: (lca score, supply, activity).
		# [note] alternatively (maybe faster), you create a lca object and use lca.switch_method(new_method) to switch to another lcia method, then
		# 		use lca.redo_lcia({FU_activity:amount_FU}) to calculate the result for the new lcia method

		# update the label to True
		self.calc_done=True


	def analyze_lca (self,impact_of_interest: Tuple,n_top_items=5,analysis_done=False):
		
		"""
		===================
		Analyze LCA results
			outputs for a given impact cateogry: 
				(1) top technosphere processes from all db (including backgr db)
				(2) "group_tag" results for foreground db
		Params:
			- impact_of_interest: a tuple containing the impact assessment method of interest (a nest tuple)
		===================
		"""
		
		self.impact_of_interest = impact_of_interest
		self.n_top_items = n_top_items #number of top items (e.g., top processes) of interest
		assert self.impact_of_interest in self.LCA_results_dict.keys(), "This method is not in your LCIA method list!"
		self.analysis_done = analysis_done
		# create a dict to store impact results by 'group_tag' (technoshpere exchanges only)
		self.techno_impact_results_grouped = collections.defaultdict(list)

		while not self.analysis_done: #if analysis has not been done yet
			# find top technosphere processes (including background db)
			self.top_processes = {self.impact_of_interest : self.top_processes_dict[self.impact_of_interest][:self.n_top_items]}

			# group the results by tag
			for exc in self.FU_activity.technosphere():
				self.lca2 = LCA({exc.input : exc['amount']},
							   self.impact_of_interest)
				self.lca2.lci()
				self.lca2.lcia()
				self.techno_impact_results_grouped[exc['group_tag']].append(self.lca2.score)
			
			self.techno_impact_results_grouped = {key : sum(val) for key, val in self.techno_impact_results_grouped.items()}
			
			# finally, update the label to True
			self.analysis_done = True
	
	
	def parse_uncertainty (self,db,act_name: str,n_iter: int):
		"""
		==============================================
		Parse the uncertainty data of a given activity
		Params:
			- db: foreground db object
			- act_name: str, name of the activity of interest
			- n_iter: int, number of iterations
		==============================================
		"""
		# initiate the check
		self.no_uncertainty_dist = False
		
		self.n_iter = n_iter #save number of iterations for Monte Carlo simulation
		
		# identify the actitvity of interest
		self.act_uncertain = [act for act in db if act['name']==act_name][0]
		
		# parse uncertainty data into a list of dicts
		self.uncertain_list = [{'loc':exc['loc'],'scale':exc['scale'],'uncertainty type':exc['uncertainty type']} for exc in self.act_uncertain.technosphere() if exc['uncertainty type']!=0]
		
		""" check if uncertainty type is specified """
		if len(self.uncertain_list) == 0:
			print ("\n no uncertainty distribution is specified! \n")
			self.no_uncertainty_dist=True
		else:            
			# get the corresponding name of the exchanges
			self.uncertain_names = [exc['name'] for exc in self.act_uncertain.technosphere() if exc['uncertainty type']!=0]    
				
			# create uncertainty variables
			self.uncertain_var = stats_arrays.UncertaintyBase.from_dicts(*self.uncertain_list)
			
			# generate random samples
			self.rand_sample_gen = stats_arrays.MCRandomNumberGenerator(self.uncertain_var)
			self.rand_samples = np.array([self.rand_sample_gen.next() for _ in range(self.n_iter)])
			
			#link random samples to the corresponding names of exchanges
			"""Caution: self.uncertain_names[col_i] could have the same name as self.uncertain_names[col_j], 
					if there exist more than one of the same exchange
			"""
			self.linked_rand_samples={}
			for col in range(self.rand_samples.shape[1]):
				self.linked_rand_samples[self.uncertain_names[col]]=self.rand_samples[:,col]
		
			
	def foreground_monte_carlo (self,linked_rand_samples: Dict):
		"""
		=====================================================================================
		Perform Monte Carlo simulation for foreground activities only
			Key assumptions:
				-same db as what's used in '.parse_uncertainty'
				-same activity of interest as what's used in '.parse_uncertainty'
				-number of iterations must be the same as that's used in '.parse_uncertainty'
				-deterministic LCA must be done before doing MC (so that lcia methods are
				imported already)
		Params:
			- linked_rand_samples: dict, random samples to evaluate for each foreground variable of interest
				{"act_name": sample_to_eval,"act_name": sample_to_eval,... }. 
				*The term "linked" means the same samples are used both in LCA and TEA modeling
		[caution]:
			- self.foreground_MC_LCA_results is pickled as "saved MC results.pickle"
		=====================================================================================
		"""
		# check if a dterministric LCA has been performed
		assert self.calc_done==True,"Please perform a deterministic LCA using '.calc_lca' method first!"
		
		# initiate results dict: {iter_1: {lcia1:result,lcia2:result,...}, iter_2:{lcia1:result,lcia2:result,...}...}
		self.foreground_MC_LCA_results = {}
		self.pooled_results = collections.defaultdict(list) # to store the results for each impact category: {lcia1: [xx, xx, xx], lcia2: [xx, xx, xx], ..}

		# initialize the progress bar
		widgets = ["Conducting uncertainty analysis: ", progressbar.Percentage(), " ", progressbar.Bar(), " ", progressbar.ETA()]
		pbar = progressbar.ProgressBar(maxval=self.n_iter,widgets=widgets).start()

		# perform MC for linked samples
		for iter_ in range(self.n_iter):
			# update the exchanges of the activity of interest
			for k,v in linked_rand_samples.items():
				for exc in self.act_uncertain.technosphere(): #self.act_uncertain from '.parse_uncertainty'
					if exc['name']==k:
						exc['amount']=v[iter_]
						exc.save()
			# do LCA
			self.calc_lca(self.lcia_methods,self.foreground_db)
			self.foreground_MC_LCA_results[iter_] = self.LCA_results_dict

			# add lca results of this iteration to the corresponding pooled list (for statisitcal analysis later)
			for k,v in self.LCA_results_dict.items():
				self.pooled_results[k].append(v)

			# update the progress bar
			pbar.update(iter_)

		# finish progressbar
		pbar.finish()

		# obtain descriptive statistics
		self.percentiles = {}
		for k,v in self.pooled_results.items():
			self.percentiles[k] = [np.percentile(v, perc) for perc in [5,25,50,75,95]]
		
		# log the percentiles
		self.logger.info("=== Percentiles of MC results by impact category ===")
		self.logger.info(self.percentiles)
		self.logger.info(" ")
		print(f"the percentiles of the MC results are: {self.percentiles}")

		# pickle the MC results
		saved_MC_path = os.path.sep.join([config.OUTPUT_PATH,'saved MC results.pickle'])
		with open(saved_MC_path, 'wb') as f:
			pickle.dump(self.foreground_MC_LCA_results, f, protocol=pickle.HIGHEST_PROTOCOL)


	def export_LCA_results(self, lca_results_dict: Dict, scenario_name='undefined_scenario', unique_name=True):
		"""
		This method export the LCA results (in .xlsx format) to designated output folder (specified in config file)
		Params:
			- lca_results_dict: a dict containing the LCA results (nominal or MC)
				- nominal LCA results: {(LCIA method 1): result, (LCIA method 2): result, ...}
				- foreground uncertainty LCA results: {'iter_1': {(LCIA method 1): result, (LCIA method 2): result, ...},
													   'iter_2': {(LCIA method 1): result, (LCIA method 2): result, ...}, ...}
			- scenario_name: str, constructs part of the file name of the exported file
			- unique_name: boolean, whether or not to create a unique export file name (with UUID) each time
		"""

		# check if the input dict is a nested dict (i.e., foreground uncertainty LCA results)
		if isinstance(list(lca_results_dict.values())[0], dict):
			# if the 1st element of "the list of lca_results_dict.values()" is a dict --> this signals the input is a nested dict of uncertainty results

			# initiate index and column variables
			output_index = []
			output_col = set()
			output_values = []

			# format the LCA results
			for iter_, sub_dict in lca_results_dict.items():
				# initiate a temp lcia results list
				tmp_lcia_results = []
				# add iteration to the list of index
				output_index.append(iter_)
				for lcia_method_tuple, lca_result in sub_dict.items():
					# add the lcia method tuple to the column set, if it's not there yet
					if not (lcia_method_tuple in output_col):
						output_col.add(lcia_method_tuple)
					# store the lcia result to the temp lcia results list
					tmp_lcia_results.append(lca_result)
				# append the temp lcia results list to the overall output value list
				output_values.append(tmp_lcia_results)

			# create the pandas dataframe
			df_LCA_results = pd.DataFrame(data=output_values, index=output_index, columns=list(output_col))
			df_LCA_results.index.name = 'Iteration'

		else: # this indicates the input is a dict of nominal LCA results
			# format the LCA results
			df_LCA_results = pd.Series(lca_results_dict).reset_index()
			df_LCA_results.columns = ['Impact assessment method', 'Impact category_agg', 'Impact category_specific', 'Results']

		# prepare file name
		if unique_name:
			export_file_name = f"LCA_results_{scenario_name}_{str(uuid.uuid4())}.xlsx"
		else:
			export_file_name = f"LCA_results_{scenario_name}.xlsx"
		
		# export the LCA results
		output_path = os.path.sep.join([config.OUTPUT_PATH,export_file_name])
		df_LCA_results.to_excel(output_path)

		print(f"The LCA results have been exported to {config.OUTPUT_PATH}")




	