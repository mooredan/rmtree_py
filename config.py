import os

rmtree_path = os.path.expanduser("~/Genealogy/ZebMoore_Ancestry.rmtree")
extension_path = os.path.expanduser("~/Genealogy/sqlite/unifuzz.so")

UNIQUE_FACT_TYPES = {
    1: "Birth",
    2: "Death",
    4: "Burial",
    6: "Adoption",
    15: "Naturalization",
    16: "Emigration",
    17: "Immigration",
}

