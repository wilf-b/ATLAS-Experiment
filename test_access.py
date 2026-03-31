import uproot
import atlasopenmagic as atom

atom.set_release('2025e-13tev-beta')

skim = "exactly4lep"

defs = {
    'Data': {'dids': ['data']}
}

samples = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)

file_string = samples['Data']['list'][0]
print(file_string)

tree = uproot.open(file_string + ":analysis")
print(tree.num_entries)
print(tree.keys())