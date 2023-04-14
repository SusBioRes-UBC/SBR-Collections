"""
This is a sandbox script to try out database management
"""


"""
=============
Set the scene
=============
"""
# import packages
from config import db_mgmt_config as config
from utilities.db_import_helper import MultiColImporter
from utilities.db_mgmt_helper import DB_mgmt
import os
from lca_MOD import LCA_MOD #use its 'import_bkgr_db' method to import ecoinvent as base db
import openpyxl
import brightway2 as bw

# prepare variables
wb_path = os.path.sep.join([config.BW2_IMPORT_PATH, "ei35_HTL_db.xlsx"])
elegancy_path = os.path.sep.join([config.BW2_IMPORT_PATH, "elegancy_db2.xlsx"])
dummy_foreground_path = os.path.sep.join([config.BW2_IMPORT_PATH, "dummy_foreground_db.xlsx"])
db_name = "test multicol importer"
multicol_start = 7
exc_row_start = 4

# prepare db path dicts
ei_bkgr_db_path_name_dict = {'ei35_cutoff': (config.EI_DB_PATH, 'ecospold2', None),}
other_bkgr_db_path_name_dict = {
	'Elegancy': (elegancy_path, 'bw2 template', None),
	'trial_HTL': (wb_path, 'bw2 template', 'multicolumn'),
	}	
foreground_db_path_name_dict = {
	'H2_from_wood_gasify_ei35_cutoff': (dummy_foreground_path, 'bw2 template', None),
}

# prepare db matching dicts
bkgr_db_match_dict = {
	"self": ('name', 'unit', 'location','reference product'),
	"ei35_cutoff": ('name', 'unit', 'location','reference product'),
	"biosphere3": ('name', 'unit', 'location'),
} # other bkgr db should be indepedent from each other and hence, should not be included here

foreground_db_match_dict = {
	"self": ('name', 'unit', 'location','reference product'),
	"trial_HTL": ('name', 'unit', 'location','reference product'),
	'Elegancy': ('name', 'unit', 'location','reference product'),
	"ei35_cutoff": ('name', 'unit', 'location','reference product'),
	"biosphere3": ('name', 'unit', 'location'),
}
project_name = 'sandbox_db_mgmt'


"""
===========
try lca_mod
===========
"""

# initiate the importer object
lca_obj = LCA_MOD(project_name)

# import ei bkgr db first
lca_obj.import_bkgr_db(ei_bkgr_db_path_name_dict, bkgr_db_match_dict)

# import other bkgr db
lca_obj.import_bkgr_db(other_bkgr_db_path_name_dict, bkgr_db_match_dict)

# import foreground db
lca_obj.import_foreground_db(foreground_db_path_name_dict, foreground_db_match_dict)

print(f"currently, the following db are imported: {lca_obj.db_mgmt_obj.imported_db_lst}","\n")

# export imported db
lca_obj.db_mgmt_obj.export_lci_to_excel('trial_HTL')
lca_obj.db_mgmt_obj.export_lci_to_excel('Elegancy')
lca_obj.db_mgmt_obj.export_lci_to_excel('H2_from_wood_gasify_ei35_cutoff')

# remove an imported db
#db_mgmt_obj.remove_db('H2_from_wood_gasify_ei35_cutoff')
#db_mgmt_obj.imported_db_lst = list(bw.databases)
#print(f"after removal, the existing db are: {db_mgmt_obj.imported_db_lst}")