# A Custom DT simulator
This is a custom made DT simulator, mimicking a DT station of the CMS detector. Geometry is completely configurable and can be easily adapted. 

### How to configure: 
```
source /cvmfs/geant4.cern.ch/geant4/11.2/x86_64-el9-gcc11-optdeb/bin/geant4.sh
export G4LIB=/cvmfs/geant4.cern.ch/geant4/11.2/x86_64-el9-gcc11-optdeb/lib64/Geant4-11.2.0/

echo "Creating build directory"
mkdir DTSim_build
cd DTSim_build

echo "Configuring..."
cmake -DGeant4_DIR=$G4LIB ../

echo "Compiling..."
make -j4
```

### How to run: 
```
./exampleDTSim runShowers.mac
```
This will command will generat a muon gun with 1000 muons, two files will be generated:  DTSimHistos.root and DTSimNtuple.root.  The former contains few histograms showing the number of hits and the position of each hit. The later contains an Ntuple with the following format

## NTuple format:
EventNo | NumberOfHits | SuperLayer | Layer | Cell | Hit X position (cm) | Hit Y position (cm) | Hit Time (ns) | Hit PDG Id
--- | --- | --- | --- |--- |--- |--- |--- |--- 
EventNo | SLHit_NHits | SLHit_SL | SLHit_Layer | SLHit_Cell | SLHit_PosX | SLHit_PosY | SLHit_Time | SLHit_PDG
