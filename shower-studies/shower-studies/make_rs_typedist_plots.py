import ROOT as r
from copy import deepcopy
import matplotlib.pyplot as plt
from mplhep import style
from matplotlib.colors import Normalize
import numpy as np

plt.style.use(style.CMS)
cmap = plt.get_cmap("viridis").copy()
cmap.set_under("none")
norm = Normalize(vmin=0.0001, vmax=1, clip=False)

r.gStyle.SetOptStat(0)

def main():
    folder = "."
    histo_name = "realshower_type_dist_MBX"

    f = r.TFile.Open( folder + "/histograms/histograms_test_thr6.root")
    pre, pos = histo_name.split("MB")
    histos = [ deepcopy( f.Get( f"{pre}MB{ist}{pos[1:]}")) for ist in range(1, 5) ]
    f.Close()
    conf_map = np.zeros((8, 10))
    totals = np.zeros((4, 5))

    for st, h in enumerate(histos):
        for iWheel in range(0, 5):
            tp1 = h.GetBinContent(iWheel + 1, 1)
            tp2 = h.GetBinContent(iWheel + 1, 2)
            tp3 = h.GetBinContent(iWheel + 1, 3)
            tp4 = h.GetBinContent(iWheel + 1, 4)
            total = tp1 + tp2 + tp3 + tp4
            conf_map[ st * 2, iWheel * 2] = tp1 / total
            conf_map[ (st * 2) + 1, iWheel * 2] = tp2 / total
            conf_map[ st * 2, (iWheel * 2) + 1] = tp3 / total
            conf_map[ (st * 2) + 1,(iWheel * 2) + 1] = tp4 / total
            totals[st, iWheel] = total

    fig, ax = plt.subplots()
    im = ax.imshow(conf_map, cmap=cmap, norm=norm)
    fig.suptitle("real showers type distributions")
    ax.set_xticks(np.arange(0.5, 10, 2))
    ax.set_xticklabels(range(-2, 3))
    ax.set_yticks(np.arange(0.5, 8, 2))
    ax.set_yticklabels([f"MB{st}" for st in range(1, 5)])
    ax.set_xlabel("Wheel")
    ax.set_ylabel("Station")
    for lh in np.arange(1.5, 8, 2):
        ax.axhline(lh, color="w", lw=2)
    for lv in np.arange(1.5, 10, 2):
        ax.axvline(lv, color="w", lw=2)

    labels = np.array([["1", "3"], ["2", "4"]])
    for x, y in np.ndindex(conf_map.shape):
        ax.text(y, x, f"{labels[x%2,y%2]}={conf_map[x, y]:.2f}", ha="center", va="center", color="w", fontsize=8)

    for st in range(4):
        for iWheel in range(5):
            ax.text(iWheel * 2 + 0.5, st * 2 + 0.5, f"{totals[st, iWheel]:.0f}", ha="center", va="center", color="w", fontsize=8,
                    bbox=dict(facecolor='black', alpha=0.5, edgecolor='none'), fontweight="bold")

    fig.colorbar(im, ax=ax, orientation="horizontal", pad=0.01, location="top")
    fig.tight_layout()
    fig.savefig(folder + "/conf_map_real_shower_type_distributions.pdf")

if __name__ == "__main__":
    main()