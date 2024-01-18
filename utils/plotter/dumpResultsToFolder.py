import ROOT
import os

from setTDRStyle import setTDRStyle

ROOT.gROOT.SetBatch(True)
setTDRStyle()
# Define the ROOT file path and the output directory path
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--root-file-path', required=True, type=str, help='Path to the ROOT file')
    parser.add_argument('-o','--output-dir-path', required=True, type=str, help='Path to the output directory')
    parser.add_argument('-eff','--efficiencies', required=False,  action='store_true', help='Draw efficiencies')
    args = parser.parse_args()
    root_file_path = args.root_file_path
    output_dir_path = args.output_dir_path

    if args.efficiencies:
        from efficiencies import efficiencies
    
    # Check if the output directory exists, if not, create it
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
        os.system("git clone https://github.com/musella/php-plots.git "+output_dir_path)


    # Open the ROOT file with ROOT.TFile
    file = ROOT.TFile(root_file_path)


    # Create a ROOT.TCanvas
    canvas = ROOT.TCanvas()

   # Check if efficiencies need to be drawn
    if args.efficiencies:
        from efficiencies import efficiencies

    # Iterate over all keys in the ROOT file
    for key in file.GetListOfKeys():
        # If the key is a histogram, draw it on the canvas and save the canvas as PNG and PDF
        if key.GetClassName().startswith('TH'):
            histogram = key.ReadObj()
            histogram.Draw()
            canvas.SaveAs(os.path.join(output_dir_path, key.GetName() + '.png'))
            canvas.SaveAs(os.path.join(output_dir_path, key.GetName() + '.pdf'))
            canvas.SaveAs(os.path.join(output_dir_path, key.GetName() + '.root'))

    # Iterate over all efficiencies
    for eff in efficiencies:
        # Get the numerator and denominator histograms
        num = file.Get(efficiencies[eff]["num"])
        den = file.Get(efficiencies[eff]["denom"])
        print(num, den)
        # Create the efficiency histogram
        if efficiencies[eff]["type"] == "efficiency":
            eff_histo = ROOT.TEfficiency(num, den)
        elif efficiencies[eff]["type"] == "ratio":
            eff_histo = ROOT.TH1D(num.Clone())
            eff_histo.Divide(den)
        
        eff_histo.SetName(efficiencies[eff]["histo"]["name"])
        eff_histo.SetTitle(efficiencies[eff]["histo"]["title"])
        # Draw the efficiency histogram
        eff_histo.Draw()
        # Save the canvas as PNG and PDF
        canvas.SaveAs(os.path.join(output_dir_path, eff + '.png'))
        canvas.SaveAs(os.path.join(output_dir_path, eff + '.pdf'))
        canvas.SaveAs(os.path.join(output_dir_path, eff + '.root'))
            
    # Close the ROOT file
    file.Close()