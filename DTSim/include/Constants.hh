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
/// \file DTSim/include/Constants.hh
/// \brief Definition of DTSim example constants.

#ifndef DTSimConstants_h
#define DTSimConstants_h 1

#include "globals.hh"
#include "G4SystemOfUnits.hh"

namespace DTSim
{
  constexpr G4int     kNofLayers = 4;
  constexpr G4int     kNofCells  = 50;
  constexpr G4int     kNoOfCellsInSL  = kNofLayers * kNofCells;
  constexpr G4int     kNofSuperLayers = 3; 
  constexpr G4double  kCellThickness  = 13.*mm;
  constexpr G4double  kCellWidth      = 42.*mm;
  constexpr G4double  kGapThickness   = 23.5*cm;
  constexpr G4double  kYokeThickness  = 29*cm;  // The other flavour is 63cm

  constexpr G4int kNofColumns = 6;
}

#endif
