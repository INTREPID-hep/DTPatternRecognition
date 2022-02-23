'''
Code for doing DT Trigger
'''
from plotter.plotter import *
from objects.Muon import *
from geometry import *

# Python will start executing things here 
stations = {"MB1" : MB1, "MB2" : MB2, "MB3" : MB3, "MB4" : MB4}

MB1 = stations["MB1"]

# Global parameters
np.random.seed(4318937)
nTrueMuons = 4
nNoise     = 0

reMatchHits  = True
doLaterality = True
agingPercentage = 1
minHits     = 2

#Generate true muons and its resulting set of patterns
allHits = []
trueHits = []
trueMuons = []

for n in range(nTrueMuons):
    
    mm          = Muon(np.random.rand()*200,0., 1./((np.random.rand())))
    MB1.check_in(mm)
    trueHits += mm.getRecoPattern()
    trueMuons.append(mm)

#And now generate a couple of points with random noise
noiseHits = []
for n in range(nNoise):
    noiseHits.append([int(np.ceil(np.random.rand()*8)), int(np.floor(np.random.rand()*47))])

for t in trueHits:
    if np.random.rand() > agingPercentage: continue
    allHits.append(t)

for n in noiseHits:
    if n in trueHits: continue
    allHits.append(n)    


#Now do the magic
for m in trueMuons:
    for hit in m.get_hits():
        print (hit.parent.idy, hit.idx)
    print("slope: ", m.get_slope())
    print("-")

#First sort all hits according to the layer
allHits.sort(key= lambda x: x[0])

#First try to separate them:
print("START: ", allHits)


#Now try to match extra hits to existing chosen patterns (+/- 1 cell tolerance)
print("==========================================================")
print("==========================================================")
print("==========================================================")
print("==========================================================")

 
#Plot it

p = plotter(MB1)
p.plot_pattern(trueMuons)
p.save_canvas("prueba", "trueMuons")

