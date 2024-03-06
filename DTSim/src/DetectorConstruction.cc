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
/// \file DTSim/src/DetectorConstruction.cc
/// \brief Implementation of the DTSim::DetectorConstruction class

#include "DetectorConstruction.hh"
#include "MagneticField.hh"
#include "SLCellParameterisation.hh"
#include "SuperLayerSD.hh"
#include "Constants.hh"

#include "G4FieldManager.hh"
#include "G4TransportationManager.hh"
#include "G4Mag_UsualEqRhs.hh"

#include "G4Material.hh"
#include "G4Element.hh"
#include "G4MaterialTable.hh"
#include "G4NistManager.hh"

#include "G4VSolid.hh"
#include "G4Box.hh"
#include "G4Tubs.hh"
#include "G4LogicalVolume.hh"
#include "G4VPhysicalVolume.hh"
#include "G4PVPlacement.hh"
#include "G4PVParameterised.hh"
#include "G4PVReplica.hh"
#include "G4UserLimits.hh"

#include "G4SDManager.hh"
#include "G4VSensitiveDetector.hh"
#include "G4RunManager.hh"
#include "G4GenericMessenger.hh"

#include "G4VisAttributes.hh"
#include "G4Colour.hh"

#include "G4ios.hh"
#include "G4SystemOfUnits.hh"



namespace DTSim
{

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

G4ThreadLocal MagneticField* DetectorConstruction::fMagneticField;
G4ThreadLocal G4FieldManager* DetectorConstruction::fFieldMgr;

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

DetectorConstruction::DetectorConstruction()
{

}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

DetectorConstruction::~DetectorConstruction()
{
  delete fMessenger;
  delete fSuperLayer1Logical;
  delete fSuperLayer2Logical;
  delete fSuperLayer3Logical;
  delete fMagneticLogical;

}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

G4VPhysicalVolume* DetectorConstruction::Construct()
{
  // Construct materials
  ConstructMaterials();

  // Get materials
  auto defaultMaterial = G4Material::GetMaterial("G4_Galactic");
  auto yokeMaterial = G4Material::GetMaterial("G4_Fe");
  auto cellMaterial = G4Material::GetMaterial("GasMixture");
  auto gapMaterial = G4Material::GetMaterial("G4_Al");
  

  if ( ! defaultMaterial || ! yokeMaterial || ! cellMaterial || !gapMaterial) {
    G4ExceptionDescription msg;
    msg << "Cannot retrieve materials already defined."; 
    G4Exception("DTGeometry::DefineVolumes()",
      "MyCode0001", FatalException, msg);
  }  



  // Option to switch on/off checking of volumes overlaps
  //
  G4bool checkOverlaps = true;

  // geometries --------------------------------------------------------------
  auto  layerThickness = kCellThickness;
  auto  superlayerThickness = kNofLayers * kCellThickness;
  auto  dtThickness = superlayerThickness * kNofSuperLayers + kGapThickness + kYokeThickness;
  auto  dtWidth = kCellWidth * kNofCells;

  auto  worldSizeXY = 1.2 * dtWidth;
  auto  worldSizeZ  = 1.2 * dtThickness; 

  // POSITIONS OF DIFFERENT SUBDETECTORS...
  auto zpos_yoke = -dtThickness/2+kYokeThickness/2;
  auto zpos_superlayer1 = -dtThickness/2+kYokeThickness+superlayerThickness/2;
  auto zpos_honeycomb = -dtThickness/2+kYokeThickness+superlayerThickness+kGapThickness/2;
  auto zpos_superlayer2 = -dtThickness/2+kYokeThickness+superlayerThickness+kGapThickness+superlayerThickness/2;
  auto zpos_superlayer3 = -dtThickness/2+kYokeThickness+superlayerThickness+kGapThickness+superlayerThickness+superlayerThickness/2;


  // experimental hall (world volume)
  auto worldSolid
    = new G4Box("worldBox",worldSizeXY/2, worldSizeXY/2, worldSizeZ/2);
  auto worldLogical
    = new G4LogicalVolume(worldSolid,defaultMaterial,"worldLogical");
  auto worldPhysical = new G4PVPlacement(
    nullptr, G4ThreeVector(), worldLogical, "worldPhysical", nullptr, false, 0, checkOverlaps);

  // dt chamber
  auto dtChamberSolid
    = new G4Box("dtChamberSolid",dtWidth/2, dtWidth/2,dtThickness/2);
  auto dtChamberLogical
    = new G4LogicalVolume(dtChamberSolid,defaultMaterial,"dtChamberLogical");
  new G4PVPlacement(nullptr, G4ThreeVector(0., 0., 0.), dtChamberLogical, "dtChamberPhysical",
    worldLogical, false, 0, checkOverlaps);


  // Yoke with Magnetic field
  auto magneticSolid
    = new G4Box("magneticBox", dtWidth/2, dtWidth/2, kYokeThickness/2);
  fMagneticLogical = new G4LogicalVolume(magneticSolid, yokeMaterial, "magneticLogical");
  new G4PVPlacement(nullptr,G4ThreeVector(0., 0., zpos_yoke),fMagneticLogical,"magneticPhysical",dtChamberLogical,false,0,checkOverlaps);

  // set step limit in tube with magnetic field
  auto userLimits = new G4UserLimits(1 * mm);
  fMagneticLogical->SetUserLimits(userLimits);

  // honeycomb
  auto gapS = new G4Box("HoneycombBox", dtWidth/2, dtWidth/2, kGapThickness/2);
  auto gapLV = new G4LogicalVolume(gapS, gapMaterial, "HoneycombLogical");
  new G4PVPlacement(0, G4ThreeVector(0., 0., zpos_honeycomb), gapLV, "HoneycombPhysical", dtChamberLogical, false, 0, checkOverlaps);

  // SuperLayer 1
  auto superLayer1Solid = new G4Box("superLayer1Box", dtWidth/2, dtWidth/2, superlayerThickness/2);
  auto superLayer1Logical
    = new G4LogicalVolume(superLayer1Solid,cellMaterial,"SL1Logical");
  new G4PVPlacement(nullptr, G4ThreeVector(0., 0., zpos_superlayer1), superLayer1Logical,
    "SL1Physical", dtChamberLogical, false, 0, checkOverlaps);

  // SL1 cells
  auto SL1CellSolid = new G4Box("SL1CellBox", kCellWidth/2, dtWidth/2, layerThickness/2); 
  fSuperLayer1Logical = new G4LogicalVolume(SL1CellSolid,cellMaterial,"SuperLayer1Logical");
  G4VPVParameterisation* cellParam = new SLCellParameterisation();
  new G4PVParameterised("SuperLayer1Physical",fSuperLayer1Logical,superLayer1Logical,
                        kXAxis,kNoOfCellsInSL,cellParam);

  // SuperLayer 2
  auto superLayer2Solid = new G4Box("superLayer2Box", dtWidth/2, dtWidth/2, superlayerThickness/2);
  auto superLayer2Logical
    = new G4LogicalVolume(superLayer2Solid,cellMaterial,"SL2Logical");

  G4RotationMatrix* sl2_rot = new G4RotationMatrix;
  sl2_rot->rotateZ(90.*deg);

  new G4PVPlacement(sl2_rot, G4ThreeVector(0., 0., zpos_superlayer2), superLayer2Logical,
    "SL2Physical", dtChamberLogical, false, 0, checkOverlaps);

  // SL2 cells
  auto SL2CellSolid = new G4Box("SL2CellBox", kCellWidth/2, dtWidth/2, layerThickness/2); 
  fSuperLayer2Logical = new G4LogicalVolume(SL1CellSolid,cellMaterial,"SuperLayer1Logical");
  new G4PVParameterised("SuperLayer2Physical",fSuperLayer2Logical,superLayer2Logical,
                        kXAxis,kNoOfCellsInSL,cellParam);


  // SuperLayer 3
  auto superLayer3Solid = new G4Box("superLayer1Box",dtWidth/2, dtWidth/2, superlayerThickness/2);
  auto superLayer3Logical
    = new G4LogicalVolume(superLayer3Solid,cellMaterial,"SL3Logical");
  new G4PVPlacement(nullptr, G4ThreeVector(0., 0., zpos_superlayer3), superLayer3Logical,
    "SL3Physical", dtChamberLogical, false, 0, checkOverlaps);

  // SL3 cells
  auto SL3CellSolid = new G4Box("SL1CellBox", kCellWidth/2, dtWidth/2, layerThickness/2); 
  fSuperLayer3Logical = new G4LogicalVolume(SL3CellSolid,cellMaterial,"SuperLayer3Logical");
  new G4PVParameterised("SuperLayer3Physical",fSuperLayer3Logical,superLayer3Logical,
                        kXAxis,kNoOfCellsInSL,cellParam);


  // visualization attributes ------------------------------------------------

  G4VisAttributes invisible(G4VisAttributes::GetInvisible());
  G4VisAttributes invisibleBlue(false, G4Colour::Blue());
  G4VisAttributes invisibleGreen(false, G4Colour::Green());
  G4VisAttributes invisibleYellow(false, G4Colour::Yellow());
  G4VisAttributes blue(G4Colour::Blue());
  G4VisAttributes cgray(G4Colour::Gray());
  G4VisAttributes green(G4Colour::Green());
  G4VisAttributes red(G4Colour::Red());
  G4VisAttributes yellow(G4Colour::Yellow());

  worldLogical->SetVisAttributes(invisible);
  dtChamberLogical->SetVisAttributes(invisible);

  fMagneticLogical->SetVisAttributes(red);
  gapLV->SetVisAttributes(green);

  fSuperLayer1Logical->SetVisAttributes(blue);
  fSuperLayer2Logical->SetVisAttributes(blue);
  fSuperLayer3Logical->SetVisAttributes(blue);

  // return the world physical volume ----------------------------------------

  return worldPhysical;
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void DetectorConstruction::ConstructSDandField()
{
  // sensitive detectors -----------------------------------------------------
  auto sdManager = G4SDManager::GetSDMpointer();
  G4String SDname;

  auto superLayer1 = new SuperLayerSD(SDname="/SuperLayer1");
  sdManager->AddNewDetector(superLayer1);
  fSuperLayer1Logical->SetSensitiveDetector(superLayer1);

  auto superLayer2 = new SuperLayerSD(SDname="/SuperLayer2");
  sdManager->AddNewDetector(superLayer2);
  fSuperLayer2Logical->SetSensitiveDetector(superLayer2);

  auto superLayer3 = new SuperLayerSD(SDname="/SuperLayer3");
  sdManager->AddNewDetector(superLayer3);
  fSuperLayer3Logical->SetSensitiveDetector(superLayer3);

  // magnetic field ----------------------------------------------------------
  fMagneticField = new MagneticField();
  fFieldMgr = new G4FieldManager();
  fFieldMgr->SetDetectorField(fMagneticField);
  fFieldMgr->CreateChordFinder(fMagneticField);
  G4bool forceToAllDaughters = true;
  fMagneticLogical->SetFieldManager(fFieldMgr, forceToAllDaughters);
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

void DetectorConstruction::ConstructMaterials()
{
  auto nistManager = G4NistManager::Instance();

  // Air 
  auto air = nistManager->FindOrBuildMaterial("G4_AIR");
  G4double air_density = air->GetDensity();

  // Gas mixture
  auto gas_mixture = new G4Material("GasMixture", air_density, 2);
  gas_mixture->AddMaterial(nistManager->FindOrBuildMaterial("G4_Ar"), 85*perCent);
  gas_mixture->AddMaterial(nistManager->FindOrBuildMaterial("G4_CARBON_DIOXIDE"), 15*perCent);  
  // Aluminium - Honeycomb (GAP)
  nistManager->FindOrBuildMaterial("G4_Al");
  
  // iron Yoke
  nistManager->FindOrBuildMaterial("G4_Fe");
  
  // Vacuum "Galactic"
  nistManager->FindOrBuildMaterial("G4_Galactic");

  // Vacuum "Air with low density"
  // auto air = G4Material::GetMaterial("G4_AIR");
  // G4double density = 1.0e-5*air->GetDensity();
  // nistManager
  //   ->BuildMaterialWithNewDensity("Air_lowDensity", "G4_AIR", density);

  G4cout << G4endl << "The materials defined are : " << G4endl << G4endl;
  G4cout << *(G4Material::GetMaterialTable()) << G4endl;
}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......


//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......



//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

}
