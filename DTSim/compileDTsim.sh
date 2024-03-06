#!/bin/sh

echo "Loading Geant4"
source /cvmfs/geant4.cern.ch/geant4/11.2/x86_64-el9-gcc11-optdeb/bin/geant4.sh
export G4LIB=/cvmfs/geant4.cern.ch/geant4/11.2/x86_64-el9-gcc11-optdeb/lib64/Geant4-11.2.0/

echo "Creating build directory"
mkdir DTSim_build
cd DTSim_build

echo "Configuring..."
cmake -DGeant4_DIR=$G4LIB ../

echo "Compiling..."
make -j4

