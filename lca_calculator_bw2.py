"""
This script outputs the LCIA results of interest for a given selection of activities
	- currently only support ecoinvent db, acceptiable names
		* "ei35_cutoff"
		* "ei36_cutoff"
	- through a config file, this script can:
		* execute LCA calculations for multiple (1) activities, (2) impacts assessment methods

Project name: "SE-EmbodiedImpacts"

"""


"""
===============
Import packages
===============
"""
from brightway2 import *
import argparse
import pandas as pd
import numpy as np
import sys
import os
import SE_config #this is a file that needs to be prepared separately
from collections import defaultdict


"""
=============
House keeping
=============
"""
# currently supported db
db_supported = ["ei35_cutoff","ei36_cutoff"]

# [caution] run the following code lock, ONLY when impact assessment methods are not properly created
#create_default_biosphere3()
#print("Creating default LCIA methods\n")
#create_default_lcia_methods(overwrite=True)
#print("Creating core data migrations\n")
#create_core_migrations()
# [caution] end of code block


"""
================
define functions
================
"""
def search_ei_act(ei_act_overview_df: pd.DataFrame, keywords_list: list) -> pd.DataFrame:
	"""
	this function uses keywords to identify the activities (unit processes) of interest from a given ecoinvent database
	Input params:
		- ei_act_overview_df: a dataframe containing the overview of all activities in a given ecoinvent database
		- keywords_list: a list of [(activity, location)] of interest
	Output params:
		- act_identified_df: a dataframe containing the activity names and locations
	"""

	# prepare a dict to store the activities identified
	act_identified_dict = {'name': [], 'location': []} # [CAUTION] the keys (i.e., 'name', 'location') should be exactly the same as the headers in bw2_cal_input.xlsx

	# obtained a list of (activity name, geography) from 'activity_overview_3.6_undefined_public_1.xlsx'
	avail_act_loc_list = list(zip(ei_act_overview_df['activity name'].tolist(), ei_act_overview_df['geography'].tolist()))
	#print(f"avail activities and geography list: {avail_act_loc_list}")

	# loop over the list of (activity, location) of interest
	for act_loc_tuple in keywords_list:
		for avail_act_loc_tuple in avail_act_loc_list:
			# kw of activitiy in activity name of ecoinvent AND locations are the same
			# [CAUTION] this may lead to multiple duplicated entries, as'activity_overview_3.6_undefined_public_1.xlsx' include the same activity for 
			#   each 'product name', if there are multiple products coming out of the same activity -> there will be mulitiple duplicated LCA results ->
			#   longer runtime and duplicate rows in the output file
			if (act_loc_tuple[0].lower() in avail_act_loc_tuple[0].lower()) and (act_loc_tuple[1].lower() == avail_act_loc_tuple[1].lower()):
				act_identified_dict['name'].append(avail_act_loc_tuple[0])
				act_identified_dict['location'].append(avail_act_loc_tuple[1])

	act_identified_df = pd.DataFrame(act_identified_dict)

	return act_identified_df


def calc_lca(act_sheet: pd.DataFrame, lcia_method_sheet: pd.DataFrame, imported_db) -> pd.DataFrame:
	# inspired by https://github.com/brightway-lca/brightway2/blob/master/notebooks/Meta-analysis%20of%20LCIA%20methods.ipynb

	# prepare a list of (act_name, loc)
	act_loc_dict = act_sheet.to_dict('list')
	act_loc_tuples = list(zip(act_loc_dict['name'],act_loc_dict['location']))
	#print(act_loc_tuples)

	# prepare a list of activities retrived from db
	act_list = [act for act in imported_db for act_loc_tuple in act_loc_tuples if act_loc_tuple[0] in act['name'] and act_loc_tuple [1] in act['location']]
	#print(f"activities identified from imported db: {act_list}")

	# prepare a list of impact assessment methods
	lcia_methods = []
	for bw_method in methods: # e.g., ('ReCiPe Endpoint (E,A) w/o LT','ecosystem quality w/o LT','freshwater eutrophication w/o LT')
		for idx_lvl_0,lvl_0_name in enumerate(lcia_method_sheet['LCIA_method_lvl_0']):
			if lvl_0_name in bw_method[0]:
				if lcia_method_sheet['LCIA_method_lvl_1'].iloc[idx_lvl_0] in bw_method[1]:
					if lcia_method_sheet['LCIA_method_lvl_2'].iloc[idx_lvl_0] in bw_method[2]:
						lcia_methods.append(bw_method)
					else: continue
				else: continue  
			else: continue
	
	print(f"impact assessment methods identified: {lcia_methods}")

	# create a numpy array to store results
	lcia_results = np.zeros((len(act_list),len(lcia_methods)))

	# creat the technosphere matrix for faster calculation
	lca = LCA({act_list[0]: 1}, method=lcia_methods[0])
	lca.lci()
	lca.decompose_technosphere() # A=LU speeds up the calculation, but when new technosphere matrix A is created, need to re-decompose
	lca.lcia() # load the method data

	# get the characterization factor matrix
	char_matrices = []
	for method in lcia_methods:
		lca.switch_method(method)
		char_matrices.append(lca.characterization_matrix.copy())

	# loop over all activities of interest
	for idx_1, act in enumerate(act_list):
		lca.redo_lci({act:1})
		#print(act)
		for idx_2, matrix in enumerate(char_matrices):
			lcia_results[idx_1,idx_2] = (matrix * lca.inventory).sum()

	# create a df to store the LCA results for export
	lcia_results_df = pd.DataFrame(lcia_results, columns=lcia_methods)
	attibute_dict = defaultdict(list)
	for act in act_list:
		attibute_dict['name'].append(act['name'])
		attibute_dict['location'].append(act['location'])
		attibute_dict['unit'].append(act['unit'])
	df_tmp = pd.DataFrame.from_dict(attibute_dict)
	lcia_results_df = pd.concat([df_tmp,lcia_results_df],axis=1)


	return lcia_results_df


"""
======================
Parse input arguments
=======================
"""
# set up arguments
ap = argparse.ArgumentParser()
ap.add_argument("-p", "--projectname", help="name of the project", required=True) # add an argument for project name
#ap.add_argument("-c", "--config", help="path to the configure file", required=True) # add an argument for path to the config file
# instead of asking user to input the values for the following arguments, read them from the config file
#ap.add_argument("-d", "--database", help="name of the ecoinvent database to use", required=True), # add an arguement for ei db name
#ap.add_argument("-o", "--output", help="path the folder for output results", required=False) # add an optinal argument for path to the output folder

# parse arguments
args = vars(ap.parse_args())

# load information of interest
lcia_method_sheet = pd.read_excel(SE_config.LCA_MODELS,sheet_name="LCIA_methods")
act_sheet = pd.read_excel(SE_config.LCA_MODELS,sheet_name="activities")
ei_db_name = SE_config.EI_DB_NAME
ei_db_path = SE_config.EI_DB_PATH
ei_act_overview_df = pd.read_excel(SE_config.EI_OVERVIEW_FILE_PATH,sheet_name="activity overview")
output_path = SE_config.OUTPUT_PATH


"""
==================
Set up the project
==================
"""
if not (args["projectname"] in projects):
	print("The project name you entered does not match any of the existing ones!!", "\n")
	user_response = input("Do you want to create the project? [y/n]")
	if user_response.lower() in ['yes','y']:
		projects.set_current(args["projectname"])
	else:
		sys.exit() # terminate the thread       

# set up db
bw2setup()

# import ecoinvent db, if it has not been imported
if ei_db_name in db_supported:
	# check if the ei db already imported
	if ei_db_name in databases:
		print("[caution] db alraedy imported!!", "\n")
	else:
		db = SingleOutputEcospold2Importer(ei_db_path,ei_db_name, use_mp=False)
		db.apply_strategies()
		db.statistics()
		db.write_database()
else:
	print(f"[caution] the db name you provided does not match any of the following: {db_supported}!!", "\n")

# set database to use
db = Database(ei_db_name)


"""
==========================
Output the LCIA results
==========================
"""
# add additional activities of interest by searching through the ecoinvent activities overview sheet
keywords_list = [('wood','GLO'), ] #('WOOD', 'CH'), ('STEEL', 'GLO'), ('STEEL', 'ROW')
act_identified_df = search_ei_act(ei_act_overview_df,keywords_list)
#print(act_identified_df)

act_sheet = pd.concat([act_sheet,act_identified_df])
#print(act_sheet)

# calculate the LCIA results
lcia_results_df = calc_lca(act_sheet,lcia_method_sheet, db)
#print (lcia_results_df)

# exprot the results as .csv file
export_name = 'LCA results.csv'
lcia_results_df.to_csv(os.path.sep.join([output_path,export_name]))