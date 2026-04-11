'''

'''

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, List

import atlasopenmagic as atom
import matplotlib.pyplot as plt
import numpy as np
from dask.distributed import Client
from matplotlib.ticker import AutoMinorLocator

from config import BIN_CENTRES, BIN_EDGES, DEFS, RELEASE, SKIM, STEP_SIZE, XMAX, XMIN, LUMI_FB
from worker import process_one_file


def parse_args():
    '''
    This function allows the user to parse additional args to the analysis workflow controlling:
    - the scheduler address
    - the total num of files processed
    - the output directory 
    '''
    parser = argparse.ArgumentParser(description="Run the distributed HZZ analysis with Dask.")
    parser.add_argument("--scheduler", default=None) # Dask scheduler address, e.g. tcp://scheduler:8786
    parser.add_argument("--limit-files", type=int, default=None) # process the first N files per sample
    parser.add_argument("--output-dir", default="results") # Directory for plots and summary outputs
    return parser.parse_args()


def build_jobs(limit_files):

    atom.set_release(RELEASE) # sets which data release to use 
    samples = atom.build_dataset(DEFS, skim=SKIM, protocol="https", cache=True) # builds data dict from DEFS


    jobs: List[tuple[str, str]] = []
    for sample_name, sample_info in samples.items():
        file_list = sample_info["list"][:limit_files] if limit_files else sample_info["list"]
        for file_string in file_list:
            jobs.append((file_string, sample_name))
    return samples, jobs


def merge_results(results: list[dict]):
    '''
    adds all filelevel outputs into one result per sample. and retunrs merged
    '''
    merged: dict = {}
    for result in results:
        sample = result["sample"]
        # If a sample appears for the first time creates storage for it.
        if sample not in merged:
            merged[sample] = {
                "hist": np.zeros_like(result["hist"]),
                "sumw2": np.zeros_like(result["sumw2"]),
                "n_events": 0,
                "n_selected": 0,
                "is_data": result["is_data"],
            }
        merged[sample]["hist"] += result["hist"]
        merged[sample]["sumw2"] += result["sumw2"]
        merged[sample]["n_events"] += result["n_events"]
        merged[sample]["n_selected"] += result["n_selected"]
    return merged



def plot_results(samples, merged, output_dir):
    '''
    PLotting function that takes the merged dataset, and returns the hist to the output path
    independintly configs:
    - background 
        - uncertainty
    - text formatting for plot
    - axes and ranges 
    '''
    fig, ax = plt.subplots(figsize=(12, 8))

    # Data
    data_hist = merged["Data"]["hist"]
    data_err = np.sqrt(merged["Data"]["sumw2"])
    ax.errorbar(BIN_CENTRES, data_hist, yerr=data_err, fmt="ko", label="Data")


    # Backgrounds

    background_names = [s for s in samples if s not in ["Data", "Signal (mH = 125 GeV)"]]

    bg_hists = []
    bg_colors = []
    bg_labels = []

    for s in background_names:
        if s in merged:
            bg_hists.append(merged[s]["hist"])
            bg_colors.append(samples[s]["color"])
            bg_labels.append(s.replace("$", "$").replace("\\", "\\"))  # sanitize just in case

    cumulative = np.zeros_like(data_hist, dtype=float)

    for hist, color, label in zip(bg_hists, bg_colors, bg_labels):
        ax.bar(
            BIN_CENTRES,
            hist,
            bottom=cumulative,
            width=STEP_SIZE,
            color=color,
            label=label,
            align="center",
        )
        cumulative += hist


    # Background uncertainty banding

    #tweaked the logic of the error bounds to be simpler
    bg_sumw2 = np.zeros_like(data_hist, dtype=float)
    for i in background_names:
        if i in merged:
            bg_sumw2 += merged[i]["sumw2"]

    bg_err = np.sqrt(bg_sumw2)

    ax.bar(
        BIN_CENTRES, # x
        2 * bg_err, # heights
        bottom=cumulative - bg_err,
        width=STEP_SIZE,
        color="none",
        edgecolor="black",
        hatch="////",
        alpha=0.5, # half transparency
        label="Stat. Unc.",
    )

    # Performative Formatting

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.1, # x
            0.93, # y
            'ATLAS Open Data', # text
            transform=ax.transAxes, # coordinate system used is that of main_axes
            fontsize=16 )
    
    # Add text 'for education' on plot
    plt.text(0.1, # x
            0.88, # y
            'for education', # text
            transform=ax.transAxes, # coordinate system used is that of main_axes
            style='italic',
            fontsize=12 )

    # Add energy and luminosity
    lumi_used = str(LUMI_FB) # luminosity to write on the plot
    plt.text(0.1, # x
            0.82, # y
            r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
            transform=ax.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

    # Add a label for the analysis carried out
    plt.text(0.1, # x
            0.76, # y
            r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text
            transform=ax.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

    # Axes
    ax.set_xlim(left=XMIN, right=XMAX)

    ymax = max(float(np.max(data_hist)), float(np.max(cumulative)))
    ax.set_ylim(bottom=0, top=max(1.0, ymax * 1.3))

    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(which="both", direction="in", top=True, right=True)

    
    ax.set_xlabel("4-lepton invariant mass m4l [GeV]", fontsize=13, x=1, ha="right")
    ax.set_ylabel(f"Events / {STEP_SIZE} GeV", y=1, ha="right")

    ax.legend(frameon=False, fontsize=16 ) # no box around the legend

    # save
    plot_path = output_dir / "hzz_mass_plot.png" # outputs plot along provided path

    fig.savefig(plot_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    return plot_path


def main():
    '''
    main f that calls all relevant functions to 
    '''
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    samples, jobs = build_jobs(limit_files=args.limit_files)
    
    # Create a Dask client
    if args.scheduler:
        client = Client(args.scheduler)
    else:
        client = Client()

    print(client) # <Client: 'tcp://127.0.0.1:33779' processes=4 threads=12, memory=7.60 GiB>

    # Submit one task per input file
    print(f"Submitting {len(jobs)} file-level tasks")

    futures = []
    for file_string, sample_name in jobs:
        future = client.submit(process_one_file, file_string, sample_name)
        futures.append(future)

    # collect results from workers
    results = client.gather(futures)

    # Merge file-level results into sample-level histograms
    merged = merge_results(results)

    plot_path = plot_results(samples, merged, output_dir)

    print(f"Saved plot to {plot_path}")

    client.close()


if __name__ == "__main__":
    main()
