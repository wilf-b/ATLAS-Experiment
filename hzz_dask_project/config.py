"""
Configuration file for the HZZ to 4lep analysis.

This file contains all of the main settings used across the project, including:
- dataset selection (ATLAS Open Data release and skim)
- histogram binning
- sample definitions (data, background, signal)
- list of branches needed from the ROOT files

This is to keep all analysis parameters in one place so that the main
analysis code stays clean and easy to modify. For example, changing the binning
or adding/removing samples can be done here without touching the processing code.
Similalry the selection can be chosen manually here by the user if they wish to use 
another dataset other than: "2025e-13tev-beta"
"""



from __future__ import annotations

import numpy as np

# as taken from the notebook 
RELEASE = "2025e-13tev-beta"
SKIM = "exactly4lep" # Select the skim to use for the analysis
LUMI_FB = 36.6 # Set luminosity to 36.6 fb-1, data size of the full release
FRACTION = 1.0 # reduce this is if desire to run quicker
TREE_NAME = "analysis"
CHUNK_SIZE = "100 MB"
GEV = 1.0
XMIN = 80 * GEV # x-axis range of the plot
XMAX = 250 * GEV
STEP_SIZE = 2.5 * GEV # Histogram bin setup

BIN_EDGES = np.arange(start=XMIN, # The interval includes this value
                    stop=XMAX+STEP_SIZE, # The interval doesn't include this value
                    step=STEP_SIZE ) # Spacing between values
BIN_CENTRES = np.arange(start=XMIN+STEP_SIZE/2, # The interval includes this value
                        stop=XMAX+STEP_SIZE/2, # The interval doesn't include this value
                        step=STEP_SIZE ) # Spacing between values



#For convenient naming and identification purposes,
#we define a dictionary which stores all the important names of the samples we want to pull from the database.


DEFS = {
    r'Data':{'dids':['data']},
    r'Background $Z,t\bar{t},t\bar{t}+V,VVV$':{'dids': [410470,410155,410218,
                                                        410219,412043,364243,
                                                        364242,364246,364248,
                                                        700320,700321,700322,
                                                        700323,700324,700325], 'color': "#6b59d3" }, # purple
    r'Background $ZZ^{*}$':     {'dids': [700600],'color': "#ff0000" },# red
    r'Signal ($m_H$ = 125 GeV)':  {'dids': [345060, 346228, 346310, 346311, 346312,
                                          346340, 346341, 346342],'color': "#00cdff" },# light blue
}

# Define what variables are important to our analysis
VARIABLES = ['lep_pt','lep_eta','lep_phi','lep_e','lep_charge','lep_type','trigE','trigM','lep_isTrigMatched',
            'lep_isLooseID','lep_isMediumID','lep_isLooseIso','lep_type']


WEIGHT_VARIABLES =["filteff","kfac","xsec","mcWeight","ScaleFactor_PILEUP", "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]
