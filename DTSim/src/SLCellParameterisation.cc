//
// ********************************************************************
// * License and Disclaimer                                           *
// *                                                                  *
// * The  Geant4 software  is  copyright of the Copyright Holders  of *
// * the Geant4 Collaboration.  It is provided  under  the terms  and *
// * conditions of the Geant4 Software License,  included in the file *
// * LICENSE and available at  http://cern.ch/geant4/license .  These *
// * include a list of copyright holders.                             *
// *                                                                  *
// * Neither the authors of this software system, nor their employing *
// * institutes,nor the agencies providing financial support for this *
// * work  make  any representation or  warranty, express or implied, *
// * regarding  this  software system or assume any liability for its *
// * use.  Please see the license in the file  LICENSE  and URL above *
// * for the full disclaimer and the limitation of liability.         *
// *                                                                  *
// * This  code  implementation is the result of  the  scientific and *
// * technical work of the GEANT4 collaboration.                      *
// * By using,  copying,  modifying or  distributing the software (or *
// * any work based  on the software)  you  agree  to acknowledge its *
// * use  in  resulting  scientific  publications,  and indicate your *
// * acceptance of all terms of the Geant4 Software license.          *
// ********************************************************************
//
//
/// \file DTSim/src/SLCellParameterisation.cc
/// \brief Implementation of the DTSim::SLCellParameterisation class

#include "SLCellParameterisation.hh"
#include "Constants.hh"

#include "G4VPhysicalVolume.hh"
#include "G4ThreeVector.hh"
#include "G4SystemOfUnits.hh"

namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

SLCellParameterisation::SLCellParameterisation()
{
   for (auto layer=0; layer<kNofLayers; layer++){
        G4double zlayer = (layer - kNofLayers/2)*kCellThickness;
        for (auto cell=0; cell<kNofCells; cell++){
            G4int cellNo = cell + layer*kNofCells;
            fXCell[cellNo] = (cell-kNofCells/2)*kCellWidth+(layer%2)*kCellWidth/2;
            fZCell[cellNo] = (layer - kNofLayers/2)*kCellThickness;
        }
    }
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void SLCellParameterisation::ComputeTransformation(
       const G4int copyNo, G4VPhysicalVolume *physVol) const
{
  physVol->SetTranslation(G4ThreeVector(fXCell[copyNo],0,fZCell[copyNo]));
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

}
