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

from config import BIN_CENTRES, BIN_EDGES, DEFS, RELEASE, SKIM, STEP_SIZE, XMAX, XMIN
from worker import process_one_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the distributed HZZ analysis with Dask.")
    parser.add_argument("--scheduler", default=None, help="Dask scheduler address, e.g. tcp://scheduler:8786")
    parser.add_argument("--limit-files", type=int, default=None, help="Only process the first N files per sample")
    parser.add_argument("--output-dir", default="results", help="Directory for plots and summary outputs")
    return parser.parse_args()


def build_jobs(limit_files: int | None = None) -> tuple[Dict[str, dict], List[tuple[str, str]]]:
    atom.set_release(RELEASE)
    samples = atom.build_dataset(DEFS, skim=SKIM, protocol="https", cache=True)
    jobs: List[tuple[str, str]] = []
    for sample_name, sample_info in samples.items():
        file_list = sample_info["list"][:limit_files] if limit_files else sample_info["list"]
        for file_string in file_list:
            jobs.append((file_string, sample_name))
    return samples, jobs


def merge_results(results: list[dict]) -> dict:
    merged: dict = {}
    for result in results:
        sample = result["sample"]
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



def plot_results(samples: dict, merged: dict, output_dir: Path) -> Path:
    if "Data" not in merged:
        raise ValueError("No Data histogram found in merged results")

    fig, ax = plt.subplots(figsize=(12, 8))

    # -------------------
    # Data
    # -------------------
    data_hist = merged["Data"]["hist"]
    data_err = np.sqrt(merged["Data"]["sumw2"])
    ax.errorbar(BIN_CENTRES, data_hist, yerr=data_err, fmt="ko", label="Data")

    # -------------------
    # Backgrounds
    # -------------------
    # Remove any LaTeX-style names and keep everything plain
    background_names = [s for s in samples if s not in ["Data", "Signal (mH = 125 GeV)"]]

    bg_hists = []
    bg_colors = []
    bg_labels = []

    for s in background_names:
        if s in merged:
            bg_hists.append(merged[s]["hist"])
            bg_colors.append(samples[s]["color"])
            bg_labels.append(s.replace("$", "").replace("\\", ""))  # sanitize just in case

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

    # -------------------
    # Background uncertainty band
    # -------------------
    bg_sumw2 = np.zeros_like(data_hist, dtype=float)
    for s in background_names:
        if s in merged:
            bg_sumw2 += merged[s]["sumw2"]

    bg_err = np.sqrt(bg_sumw2)

    ax.bar(
        BIN_CENTRES,
        2 * bg_err,
        bottom=cumulative - bg_err,
        width=STEP_SIZE,
        color="none",
        edgecolor="black",
        hatch="////",
        alpha=0.4,
        label="Stat. Unc.",
    )

    # -------------------
    # Signal
    # -------------------
    signal_name = "Signal (mH = 125 GeV)"

    if signal_name in merged:
        ax.step(
            BIN_EDGES[:-1],
            merged[signal_name]["hist"],
            where="post",
            color=samples[signal_name]["color"],
            linewidth=2,
            label=signal_name,
        )

    # -------------------
    # Axes formatting
    # -------------------
    ax.set_xlim(left=XMIN, right=XMAX)

    ymax = max(float(np.max(data_hist)), float(np.max(cumulative)))
    ax.set_ylim(bottom=0, top=max(1.0, ymax))


    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(which="both", direction="in", top=True, right=True)

    # Plain text labels ONLY (no LaTeX)
    ax.set_xlabel("4-lepton invariant mass m4l [GeV]", fontsize=13, x=1, ha="right")
    ax.set_ylabel(f"Events / {STEP_SIZE} GeV", y=1, ha="right")

    ax.legend(frameon=False)

    # -------------------
    # Save
    # -------------------
    plot_path = output_dir / "hzz_mass_plot.png"

    fig.savefig(plot_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    return plot_path

def write_summary(merged: dict, output_dir: Path) -> Path:
    summary = {}
    for sample, result in merged.items():
        summary[sample] = {
            "n_events": int(result["n_events"]),
            "n_selected": int(result["n_selected"]),
            "hist_sum": float(np.sum(result["hist"])),
        }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    return summary_path


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    samples, jobs = build_jobs(limit_files=args.limit_files)
    if not jobs:
        raise RuntimeError("No jobs were built from the dataset definitions.")

    client = Client(args.scheduler) if args.scheduler else Client()
    print(client)
    print(f"Submitting {len(jobs)} file-level tasks")

    futures = [client.submit(process_one_file, file_string, sample_name) for file_string, sample_name in jobs]
    results = client.gather(futures)
    merged = merge_results(results)

    plot_path = plot_results(samples, merged, output_dir)
    summary_path = write_summary(merged, output_dir)

    print(f"Saved plot to {plot_path}")
    print(f"Saved summary to {summary_path}")

    client.close()


if __name__ == "__main__":
    main()
