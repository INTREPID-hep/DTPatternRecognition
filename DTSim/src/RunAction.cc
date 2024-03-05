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
/// \file DTSim/src/RunAction.cc
/// \brief Implementation of the DTSim::RunAction class

#include "RunAction.hh"
#include "EventAction.hh"

#include "G4Run.hh"
#include "G4UnitsTable.hh"
#include "G4SystemOfUnits.hh"
#include "G4AnalysisManager.hh"

namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

RunAction::RunAction(EventAction* eventAction)
 : fEventAction(eventAction)
{
  // Create the generic analysis manager
  auto analysisManager = G4AnalysisManager::Instance();
  analysisManager->SetDefaultFileType("root");
     // If the filename extension is not provided, the default file type (root)
     // will be used for all files specified without extension.
  analysisManager->SetVerboseLevel(1);

  // Default settings
  analysisManager->SetNtupleMerging(true);
     // Note: merging ntuples is available only with Root output
  analysisManager->SetFileName("DTSim");

  // Book histograms, ntuple

  // Creating 1D histograms
  analysisManager
    ->CreateH1("SL1","Super Layer 1 # Hits", 50, 0., 50); // h1 Id = 0
  analysisManager
    ->CreateH1("SL2","Super Layer 2 # Hits", 50, 0., 50); // h1 Id = 1
analysisManager
    ->CreateH1("SL3","Super Layer 3 # Hits", 50, 0., 50); // h1 Id = 2
    
  // Creating 2D histograms
  analysisManager
    ->CreateH2("SL1 XY","Super Layer 1 X vs Y",           // h2 Id = 0
               50, -1000., 1000, 50, -300., 300.);
  analysisManager
    ->CreateH2("SL2 XY","Super Layer 2 X vs Y",           // h2 Id = 1
               50, -1500., 1500, 50, -300., 300.);
   analysisManager
    ->CreateH2("SL3 XY","Super Layer 3 X vs Y",           // h2 Id = 2
               50, -1500., 1500, 50, -300., 300.);


  // Creating ntuple
  if ( fEventAction ) {
    analysisManager->CreateNtuple("DTSim", "Hits");
    analysisManager->CreateNtupleIColumn("SL1Hits");    // column Id = 0
    analysisManager->CreateNtupleIColumn("SL1LayerNo"); // column Id = 1
    analysisManager->CreateNtupleIColumn("SL1CellNo");  // column Id = 2
    analysisManager->CreateNtupleIColumn("SL2Hits");    // column Id = 3
    analysisManager->CreateNtupleIColumn("SL2LayerNo"); // column Id = 4
    analysisManager->CreateNtupleIColumn("SL2CellNo");  // column Id = 5
    analysisManager->CreateNtupleIColumn("SL3Hits");    // column Id = 6
    analysisManager->CreateNtupleIColumn("SL3LayerNo"); // column Id = 7
    analysisManager->CreateNtupleIColumn("SL3CellNo");  // column Id = 8
    analysisManager->FinishNtuple();
  }

  // Set ntuple output file
  analysisManager->SetNtupleFileName(0, "DTSimntuple");
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void RunAction::BeginOfRunAction(const G4Run* /*run*/)
{
  //inform the runManager to save random number seed
  //G4RunManager::GetRunManager()->SetRandomNumberStore(true);

  // Get analysis manager
  auto analysisManager = G4AnalysisManager::Instance();

  // Reset histograms from previous run
  analysisManager->Reset();

  // Open an output file
  // The default file name is set in RunAction::RunAction(),
  // it can be overwritten in a macro
  analysisManager->OpenFile();
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void RunAction::EndOfRunAction(const G4Run* /*run*/)
{
  // save histograms & ntuple
  //
  auto analysisManager = G4AnalysisManager::Instance();
  analysisManager->Write();
  analysisManager->CloseFile(false);
    // Keep content of histos so that they are plotted.
    // The content will be reset at start of the next run.
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

}
