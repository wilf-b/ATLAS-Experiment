"""
Worker analysis logic.

This file contains the functions run on each worker node. Each worker processes
one ROOT file, applies the event selection cuts, and fills a histogram of the
four-lepton invariant mass.

The steps are:
1. Read the ROOT file in chunks with uproot
2. Apply the physics cuts
3. Calculate the 4-lepton invariant mass
4. Fill a histogram
5. Return a small summary dictionary

Only histogram-level outputs are returned, which keeps communication between
the workers and the main script lightweight.

This works well for scaling because each input file can be processed
independently on a separate worker before the histograms are merged.
"""

import awkward as ak
import numpy as np
import uproot
import vector

from config import BIN_EDGES, CHUNK_SIZE, FRACTION, LUMI_FB, TREE_NAME, VARIABLES, WEIGHT_VARIABLES


# Cut lepton type (electron type is 11, muon type is 13)
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool  # True means we should remove this entry


# Cut lepton charge
def cut_lep_charge(lep_charge):
    # First lepton in each event is [:, 0], second is [:, 1], etc.
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge  # True means we should remove this entry


# Keep events that pass either the electron or muon trigger
def cut_trig(trigE, trigM):
    return trigE | trigM


# Require at least one of the leptons to be matched to the trigger
def cut_trig_match(lep_trigmatch):
    trigmatch = lep_trigmatch
    cut1 = ak.sum(trigmatch, axis=1) >= 1
    return cut1


# Require all four leptons to pass the relevant ID and isolation cuts
def ID_iso_cut(IDel, IDmu, isoel, isomu, pid):
    thispid = pid
    return (ak.sum(((thispid == 13) & IDmu & isomu) | ((thispid == 11) & IDel & isoel), axis=1) == 4)


# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M  # .M gives the invariant mass
    return invariant_mass


# Calculate the event weight for MC events
# This scales the MC sample to the chosen luminosity and multiplies by
# the relevant scale factors stored in the input file
def calc_weight(events):
    total_weight = LUMI_FB * 1000.0 / events["sum_of_weights"]
    for variable in WEIGHT_VARIABLES:
        total_weight = total_weight * abs(events[variable])
    return total_weight


def iterate_tree(file_string, branches):
    """
    Open the ROOT tree and return an iterator over chunks of events.

    The file is read in chunks rather than all at once so memory use stays low.
    If FRACTION is less than 1, only part of the file is processed, which is
    useful for quick testing.
    """
    # Open the analysis tree inside the ROOT file
    tree = uproot.open(f"{file_string}:{TREE_NAME}")

    # By default the full tree is processed. FRACTION can be used to only
    # analyse part of the file while testing the workflow.
    entry_stop = None
    if FRACTION < 1.0:
        entry_stop = int(tree.num_entries * FRACTION)

    iterator = tree.iterate(
        list(branches),
        library="ak",
        step_size=CHUNK_SIZE,  # read in chunks to keep memory use manageable
        entry_stop=entry_stop,
    )

    return iterator, tree.num_entries


def process_one_file(file_string, sample_name, bin_edges=None):
    if bin_edges is None:
        bin_edges = BIN_EDGES

    # Data files and MC files do not need exactly the same branches:
    # MC needs the extra weighting information as well as sum_of_weights.
    is_data = sample_name == "Data"

    if is_data:
        branches = VARIABLES
    else:
        branches = VARIABLES + WEIGHT_VARIABLES + ["sum_of_weights"]

    # Histogram of invariant mass values
    hist = np.zeros(len(bin_edges) - 1, dtype=float)

    # Sum of squared weights in each bin, used later for MC statistical uncertainty
    sumw2 = np.zeros(len(bin_edges) - 1, dtype=float)

    total_events = 0
    selected_events = 0

    iterator, tree_entries = iterate_tree(file_string, branches)

    # Loop through chunks of events from this file
    for data in iterator:
        total_events += len(data)

        # Trigger-level cuts
        data = data[cut_trig(data["trigE"], data["trigM"])]
        data = data[cut_trig_match(data["lep_isTrigMatched"])]

        # Standard pT thresholds from the ATLAS open data HZZ example:
        # leading lepton > 20 GeV, second > 15 GeV, third > 10 GeV
        data = data[data["lep_pt"][:, 0] > 20]
        data = data[data["lep_pt"][:, 1] > 15]
        data = data[data["lep_pt"][:, 2] > 10]

        # Require all four leptons to pass ID and isolation cuts
        data = data[
            ID_iso_cut(
                data["lep_isLooseID"],
                data["lep_isMediumID"],
                data["lep_isLooseIso"],
                data["lep_isLooseIso"],
                data["lep_type"],
            )
        ]

        # These two cut functions return True for events to remove, so invert result using ~
        data = data[~cut_lep_type(data["lep_type"])]
        data = data[~cut_lep_charge(data["lep_charge"])]

        # If no events survive in this chunk, move to the next one
        if len(data) == 0:
            continue

        masses = calc_mass(data["lep_pt"], data["lep_eta"], data["lep_phi"], data["lep_e"])

        masses_np = ak.to_numpy(masses)
        selected_events += len(masses_np)

        if is_data:
            # Data events are filled with weight 1
            hist += np.histogram(masses_np, bins=bin_edges)[0]
            sumw2 += np.histogram(masses_np, bins=bin_edges)[0]
        else:
            # MC events are filled with their event weights.
            # sumw2 stores the sum of squared weights in each bin and is used
            # later to estimate the statistical uncertainty on the MC prediction.
            weights_np = ak.to_numpy(calc_weight(data))
            hist += np.histogram(masses_np, bins=bin_edges, weights=weights_np)[0]
            sumw2 += np.histogram(masses_np, bins=bin_edges, weights=weights_np**2)[0]

    return {
        "sample": sample_name,
        "file": file_string,
        "hist": hist,
        "sumw2": sumw2,
        "n_events": int(total_events),
        "n_selected": int(selected_events),
        "tree_entries": int(tree_entries),
        "is_data": is_data,
    }