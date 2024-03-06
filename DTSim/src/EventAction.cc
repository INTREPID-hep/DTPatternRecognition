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
/// \file DTSim/src/EventAction.cc
/// \brief Implementation of the DTSim::EventAction class

#include "EventAction.hh"
#include "SuperLayerHit.hh"
#include "Constants.hh"

#include "G4Event.hh"
#include "G4RunManager.hh"
#include "G4EventManager.hh"
#include "G4HCofThisEvent.hh"
#include "G4VHitsCollection.hh"
#include "G4SDManager.hh"
#include "G4SystemOfUnits.hh"
#include "G4ios.hh"
#include "G4AnalysisManager.hh"

using std::array;
using std::vector;

namespace {

// Utility function which finds a hit collection with the given Id
// and print warnings if not found
G4VHitsCollection* GetHC(const G4Event* event, G4int collId) {
  auto hce = event->GetHCofThisEvent();
  if (!hce) {
      G4ExceptionDescription msg;
      msg << "No hits collection of this event found." << G4endl;
      G4Exception("EventAction::EndOfEventAction()",
                  "Code001", JustWarning, msg);
      return nullptr;
  }

  auto hc = hce->GetHC(collId);
  if ( ! hc) {
    G4ExceptionDescription msg;
    msg << "Hits collection " << collId << " of this event not found." << G4endl;
    G4Exception("EventAction::EndOfEventAction()",
                "Code001", JustWarning, msg);
  }
  return hc;
}

}

namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

EventAction::EventAction()
{
  // set printing per each event
  G4RunManager::GetRunManager()->SetPrintProgress(1);
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void EventAction::BeginOfEventAction(const G4Event*)
{
  // Find hit collections and histogram Ids by names (just once)
  // and save them in the data members of this class

  if (fSLHCID[0] == -1) {
    auto sdManager = G4SDManager::GetSDMpointer();
    auto analysisManager = G4AnalysisManager::Instance();

    // hits collections names
      array<G4String, kDim> sHCName
      = {{ "SuperLayer1/SuperLayerColl", "SuperLayer2/SuperLayerColl", "SuperLayer3/SuperLayerColl" }};

    // histograms names
    array<array<G4String, kDim>, kDim> histoName
      = {{ {{ "SL1", "SL2", "SL3" }}, {{ "SL1 XY", "SL2 XY", "SL3 XY" }} }};

    for (G4int iDet = 0; iDet < kDim; ++iDet) {
      // hit collections IDs
      fSLHCID[iDet] = sdManager->GetCollectionID(sHCName[iDet]);

      // histograms IDs
      fSLHistoID[kH1][iDet] = analysisManager->GetH1Id(histoName[kH1][iDet]);
      fSLHistoID[kH2][iDet] = analysisManager->GetH2Id(histoName[kH2][iDet]);
    }
  }
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void EventAction::EndOfEventAction(const G4Event* event)
{
  //
  // Fill histograms & ntuple
  //

  // Get analysis manager
  auto analysisManager = G4AnalysisManager::Instance();
  
  // Drift chambers hits
  for (G4int iDet = 0; iDet < kDim; ++iDet) {
    auto hc = GetHC(event, fSLHCID[iDet]);
    if ( ! hc ) return;

    auto nhit = hc->GetSize();
    analysisManager->FillH1(fSLHistoID[kH1][iDet], nhit);

    // columns 0, 1, 2
    for (unsigned long i = 0; i < nhit; ++i) {
      auto hit = static_cast<SuperLayerHit*>(hc->GetHit(i));
      auto localPos = hit->GetLocalPos();
      analysisManager->FillH2(fSLHistoID[kH2][iDet], localPos.x(), localPos.y());

      // Fill Ntuple Columns
      analysisManager->FillNtupleIColumn(0, event->GetEventID());
      analysisManager->FillNtupleIColumn(1, nhit);
      analysisManager->FillNtupleIColumn(2, iDet+1);
      
      analysisManager->FillNtupleIColumn(3, hit->GetLayerID());
      analysisManager->FillNtupleIColumn(4, hit->GetCellID());
      analysisManager->FillNtupleDColumn(5, localPos.x());
      analysisManager->FillNtupleDColumn(6, localPos.y());
      analysisManager->FillNtupleDColumn(7, hit->GetTime());
      analysisManager->FillNtupleIColumn(8, hit->GetPDGID());

      analysisManager->AddNtupleRow();
    }
  }

  //
  // Print diagnostics
  //
  auto printModulo = G4RunManager::GetRunManager()->GetPrintProgress();
  if ( printModulo == 0 || event->GetEventID() % printModulo != 0) return;

  auto primary = event->GetPrimaryVertex(0)->GetPrimary(0);
  G4cout
    << G4endl
    << ">>> Event " << event->GetEventID() << " >>> Simulation truth : "
    << primary->GetG4code()->GetParticleName()
    << " " << primary->GetMomentum() << G4endl;

  // Drift chambers
  for (G4int iDet = 0; iDet < kDim; ++iDet) {
    auto hc = GetHC(event, fSLHCID[iDet]);
    if ( ! hc ) return;
    G4cout << "Super Layer " << iDet + 1 << " has " <<  hc->GetSize()  << " hits." << G4endl;
    for (unsigned int i = 0; i < hc->GetSize(); i++) {
      auto hit = static_cast<SuperLayerHit*>(hc->GetHit(i));
      hit->Print();
    }
  }

}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

}
