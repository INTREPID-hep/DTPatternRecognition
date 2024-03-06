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
/// \file DTSim/src/SuperLayerSD.cc
/// \brief Implementation of the DTSim::SuperLayerSD class

#include "SuperLayerHit.hh"
#include "SuperLayerSD.hh"
#include "Constants.hh"

#include "G4HCofThisEvent.hh"
#include "G4TouchableHistory.hh"
#include "G4Track.hh"
#include "G4Step.hh"
#include "G4SDManager.hh"
#include "G4ios.hh"

namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

SuperLayerSD::SuperLayerSD(G4String name)
: G4VSensitiveDetector(name)
{
  collectionName.insert("SuperLayerColl");
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void SuperLayerSD::Initialize(G4HCofThisEvent* hce)
{
  fHitsCollection
    = new SuperLayerHitsCollection(SensitiveDetectorName,collectionName[0]);
  if (fHCID<0) {
    fHCID = G4SDManager::GetSDMpointer()->GetCollectionID(fHitsCollection);
  }
  hce->AddHitsCollection(fHCID,fHitsCollection);
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

G4bool SuperLayerSD::ProcessHits(G4Step*step, G4TouchableHistory*)
{
  auto charge = step->GetTrack()->GetDefinition()->GetPDGCharge();
  auto pdgID  = step->GetTrack()->GetDefinition()->GetPDGEncoding();
  
  if (charge==0.) return true;

  auto preStepPoint = step->GetPreStepPoint();

  auto touchable = step->GetPreStepPoint()->GetTouchable();
  auto physical = touchable->GetVolume();
  auto copyNo = physical->GetCopyNo();
  auto layerID = GetLayerID(copyNo);
  auto cellID = GetCellID(copyNo);

  auto worldPos = preStepPoint->GetPosition();
  auto localPos = touchable->GetHistory()->GetTopTransform().TransformPoint(worldPos);

  auto hit = new SuperLayerHit(cellID, layerID);
  hit->SetLogV(physical->GetLogicalVolume());
  hit->SetWorldPos(worldPos);
  hit->SetLocalPos(localPos);
  hit->SetTime(preStepPoint->GetGlobalTime());
  hit->SetPDGID(pdgID);
  
  fHitsCollection->insert(hit);

  return true;
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......
G4int SuperLayerSD::GetCellID(G4int copyNo) const
{
  return copyNo % kNofCells;
}

G4int SuperLayerSD::GetLayerID(G4int copyNo) const
{
  return copyNo / kNofCells + 1;
}

}
