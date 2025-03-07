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
    folder = "./results_g4"
    histo_name = "shower_tpfptnfn_MB1"

    f = r.TFile.Open( folder + "/histograms/histograms_thr6.root")
    h = deepcopy(f.Get( histo_name))
    f.Close()
    conf_map = np.zeros((2, 2))
    tp = h.GetBinContent(1, 1)
    fp = h.GetBinContent(1, 2)
    tn = h.GetBinContent(1, 3)
    fn = h.GetBinContent(1, 4)
    total = tp + fp + tn + fn
    conf_map[ 0, 0] = tp / total
    conf_map[ 1, 0] = fp / total
    conf_map[ 0, 1] = tn / total
    conf_map[ 1, 1] = fn / total
    print(conf_map)
    fig, ax = plt.subplots()
    im = ax.imshow(conf_map, cmap=cmap, norm=norm)
    fig.suptitle("Confusion matrices from showers detection")
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    labels = np.array([["TP", "TN"], ["FP", "FN"]])
    for x, y in np.ndindex(conf_map.shape):
        ax.text(y, x, f"{labels[x%2,y%2]}={conf_map[x, y]:.2f}", ha="center", va="center", color="w", fontsize=15)

    ax.text( 0.5,  0.5, f"{total:.0f}", ha="center", va="center", color="w", fontsize=15,
                    bbox=dict(facecolor='black', alpha=0.5, edgecolor='none'), fontweight="bold")

    fig.colorbar(im, ax=ax, orientation="horizontal", pad=0.01, location="top")
    fig.tight_layout()
    fig.savefig(folder + "/conf_map_thr6.pdf")

if __name__ == "__main__":
    main()