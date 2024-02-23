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
/// \file B4/B4d/src/RunAction.cc
/// \brief Implementation of the B4::RunAction class

#include "RunAction.hh"
#include "EventAction.hh"

#include "G4AnalysisManager.hh"
#include "G4Run.hh"
#include "G4RunManager.hh"
#include "G4UnitsTable.hh"
#include "G4SystemOfUnits.hh"

namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

RunAction::RunAction(EventAction* eventAction)
 : fEventAction(eventAction)
{
  // set printing event number per each event
  G4RunManager::GetRunManager()->SetPrintProgress(1);

  // Create analysis manager
  // The choice of the output format is done via the specified
  // file extension.
  auto analysisManager = G4AnalysisManager::Instance();
  analysisManager->SetDefaultFileType("root");

  // Create directories
  //analysisManager->SetHistoDirectoryName("histograms");
  //analysisManager->SetNtupleDirectoryName("ntuple");
  analysisManager->SetVerboseLevel(1);
  analysisManager->SetNtupleMerging(true);
    // Note: merging ntuples is available only with Root output
  analysisManager->SetFileName("Dtsim");

  // Book histograms, ntuple
  //
  // Creating 1D histograms
  analysisManager
    ->CreateH1("SuperLayer1","SuperLayer 1 # Hits", 50, 0., 50); // h1 Id = 0
  analysisManager
    ->CreateH1("SuperLayer2","SuperLayer 2 # Hits", 50, 0., 50); // h1 Id = 1
  analysisManager
    ->CreateH1("SuperLayer3","SuperLayer 3 # Hits", 50, 0., 50); // h1 Id = 1

  // Creating 2D histograms
  analysisManager
    ->CreateH2("SuperLayer1 XY","SuperLayer 1 X vs Y",           // h2 Id = 0
               50, -1000., 1000, 50, -300., 300.);
  analysisManager
    ->CreateH2("SuperLayer2 XY","SuperLayer 2 X vs Y",           // h2 Id = 0
               50, -1000., 1000, 50, -300., 300.);
  analysisManager
    ->CreateH2("SuperLayer3 XY","SuperLayer 3 X vs Y",           // h2 Id = 0
               50, -1000., 1000, 50, -300., 300.);

  if ( fEventAction ) {
    analysisManager->CreateNtuple("DTSim", "Hits");
    analysisManager->CreateNtupleIColumn("SL1Hits");  // column Id = 0
    analysisManager->CreateNtupleIColumn("SL2Hits");  // column Id = 1
    analysisManager->CreateNtupleIColumn("SL3Hits");  // column Id = 2
    analysisManager->FinishNtuple();
  }


  // Set ntuple output file
  analysisManager->SetNtupleFileName(0, "DTsimNtuple");

}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void RunAction::BeginOfRunAction(const G4Run* /*run*/)
{
  //inform the runManager to save random number seed
  //G4RunManager::GetRunManager()->SetRandomNumberStore(true);

  // Get analysis manager
  auto analysisManager = G4AnalysisManager::Instance();
  analysisManager->Reset();

  // Open an output file
  //
  G4String fileName = "Dtsim.root";
  // Other supported output types:
  // G4String fileName = "B4.csv";
  // G4String fileName = "B4.hdf5";
  // G4String fileName = "B4.xml";
  analysisManager->OpenFile(fileName);
  G4cout << "Using " << analysisManager->GetType() << G4endl;
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void RunAction::EndOfRunAction(const G4Run* /*run*/)
{
  // print histogram statistics
  //
  auto analysisManager = G4AnalysisManager::Instance();
  if ( analysisManager->GetH1(1) ) {
    G4cout << G4endl << " ----> print histograms statistic ";
    if(isMaster) {
      G4cout << "for the entire run " << G4endl << G4endl;
    }
    else {
      G4cout << "for the local thread " << G4endl << G4endl;
    }

    G4cout << " SL1 hits : mean = "
       << analysisManager->GetH1(0)->mean()
       << " rms = "
       << analysisManager->GetH1(0)->rms() << G4endl;

    G4cout << " SL2 hits : mean = "
       << analysisManager->GetH1(1)->mean()
       << " rms = "
       << analysisManager->GetH1(1)->rms() << G4endl;

    G4cout << " SL3 hits : mean = "
       << analysisManager->GetH1(2)->mean()
       << " rms = "
       << analysisManager->GetH1(2)->rms() << G4endl;
  }

  // save histograms & ntuple
  //
  analysisManager->Write();
  analysisManager->CloseFile(false);
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

}
