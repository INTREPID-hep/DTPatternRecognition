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
/// \file DTSim/include/SuperLayerHit.hh
/// \brief Definition of the DTSim::SuperLayerHit class

#ifndef DTSimSuperLayerHit_h
#define DTSimSuperLayerHit_h 1

#include "G4VHit.hh"
#include "G4THitsCollection.hh"
#include "G4Allocator.hh"
#include "G4ThreeVector.hh"
#include "G4LogicalVolume.hh"
#include "G4Transform3D.hh"
#include "G4RotationMatrix.hh"

class G4AttDef;
class G4AttValue;

namespace DTSim
{

/// EM Calorimeter hit
///
/// It records:
/// - the cell ID
/// - the energy deposit
/// - the cell logical volume, its position and rotation

class SuperLayerHit : public G4VHit
{
  public:
    SuperLayerHit() = default;
    SuperLayerHit(G4int cellID, G4int layerID);
    SuperLayerHit(const SuperLayerHit &right) = default;
    ~SuperLayerHit() override = default;

    SuperLayerHit& operator=(const SuperLayerHit &right) = default;
    G4bool operator==(const SuperLayerHit &right) const;

    inline void *operator new(size_t);
    inline void operator delete(void *aHit);

    void Draw() override;
    const std::map<G4String,G4AttDef>* GetAttDefs() const override;
    std::vector<G4AttValue>* CreateAttValues() const override;
    void Print() override;

    void SetCellID(G4int z) { fCellID = z; }
    G4int GetCellID() const { return fCellID; }

    void SetLayerID(G4int z) { fLayerID = z; }
    G4int GetLayerID() const { return fLayerID; }

    void SetPDGID(G4int id) { fPDGID = id; }
    G4int GetPDGID() { return fPDGID; }
  
    void SetTime(G4double t) { fTime = t; }
    G4double GetTime() const { return fTime; }

    void SetLocalPos(G4ThreeVector xyz) { fLocalPos = xyz; }
    G4ThreeVector GetLocalPos() const { return fLocalPos; }

    void SetWorldPos(G4ThreeVector xyz) { fWorldPos = xyz; }
    G4ThreeVector GetWorldPos() const { return fWorldPos; }

    void SetLogV(G4LogicalVolume* val) { fPLogV = val; }
    const G4LogicalVolume* GetLogV() const { return fPLogV; }

  private:

    G4int fCellID = -1;
    G4int fLayerID = -1;
    G4int fPDGID = -1;
    G4double fTime = 0.;
    G4ThreeVector fLocalPos;
    G4ThreeVector fWorldPos;
    const G4LogicalVolume* fPLogV = nullptr;
};

using SuperLayerHitsCollection = G4THitsCollection<SuperLayerHit>;

extern G4ThreadLocal G4Allocator<SuperLayerHit>* SuperLayerHitAllocator;

inline void* SuperLayerHit::operator new(size_t)
{
  if (!SuperLayerHitAllocator) {
       SuperLayerHitAllocator = new G4Allocator<SuperLayerHit>;
  }
  return (void*)SuperLayerHitAllocator->MallocSingle();
}

inline void SuperLayerHit::operator delete(void* aHit)
{
  SuperLayerHitAllocator->FreeSingle((SuperLayerHit*) aHit);
}

}

//....oooOO0OOooo........oooOO0OOooo........oooOO0OOooo........oooOO0OOooo......

#endif
