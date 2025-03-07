from dtpr.base import NTuple
from mpldts.geometry import Station

inputfile = "../ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root"
tree_name = "/dtNtupleProducer/DTTREE"


def compute_showers_phi(ev):
    showers = ev.emushowers
    if showers:
        for shower in showers:
            print(shower)


def main():

    ntuple = NTuple(
        inputFolder=inputfile,
        tree_name=tree_name,
    )

    for ev in ntuple.events:
        compute_showers_phi(ev)

if __name__ == '__main__':
    main()