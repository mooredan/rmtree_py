import sqlite3
import re
import inspect

from config import (
#    rmtree_path,
#    extension_path,
    US_COUNTIES,
    STATE_ABBREVIATIONS,
    OLD_STYLE_ABBR,
    STATE_NAMES,
    US_PLACES,
    FOREIGN_COUNTRIES,
    COMMON_PLACE_MAPPINGS,
    SPECIAL_PLACE_MAPPINGS,
    MEXICAN_STATES,
    CANADIAN_PROVINCES,
    HISTORICAL_US_TERRITORIES,
)


def fix_address(address):
    m = re.match(r".*\b[NS]\. *[EW]\.*$", address, flags=re.IGNORECASE)
    if m:
        # print(f"match: {m}")
        address = re.sub(r"\.$", r'', address, flags=re.IGNORECASE)
        # print(f"address: {address}")
        address = re.sub(r"([NSEW])\. *([NSEW])$", r'\1\2', address, flags=re.IGNORECASE)
        # print(f"address: {address}")
        # address = re.sub(r"([NSEW][NSEW])$", r'\1\2', address, re.IGNORECASE)
        pattern = "[NSEW][NSEW]$"
        address = re.sub(pattern, lambda match: match.group(0).upper(), address, flags=re.IGNORECASE)
        # print(f"address: {address}")
        return address      
    return address      


def pp_for_strip_address(name):


    # strip_address picks up on digits in the name
    # we should take care of these ourselves first
    name = re.sub(r"\#", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\bWard\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMagisterial\s+District\s+No\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMagisterial\s+District\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMagisterial\s+Dist\s+\#*[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bElection\s+District\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bElection\s+Precinct\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bElec\s+Prec\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bSchool\s+District\s+No\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bSchool\s+District\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+Dist\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+Dist\s+\#[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+D\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+D[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+Dist\s+No\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+Dist\s+No\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bMag\s+Dist\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bCivil\s+District\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bAssembly\s+District\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bDistrict\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bSubdivision\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bDist-[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bDis-[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bDist\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bDis\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bBeat\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bRegiment\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bJustice\s+Precinct\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bJustice\s+Precint\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bJ\s+P\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bA\s+D\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bG\s+H\s+No\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bJustice\s+Precinct", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bPrecinct\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bPrecint\s+[0-9]+", " ", name, flags=re.IGNORECASE)
    # name = re.sub(r" Ward [0-9][0-9]", " ", name)

    name = re.sub(r'\ \.', '.', name, flags=re.DOTALL)
    name = re.sub(r'\.\.', '.', name, flags=re.DOTALL)
    name = re.sub(r'\.  \,', '.,', name, flags=re.DOTALL)

    # Remove leading symbols before first digit (e.g., "-410 N. Euclid")
    name = re.sub(r"^[^\w\d]*", "", name)

    # If there is an apartment number in the address, we are dropping it
    # get the first field, leave the rest as is
    parts = [p.strip() for p in name.split(",")]
    first_part = parts[0]
    m = re.match(r".*\bApt\.*\s+No\.*\s+\d+\.*", parts[0], flags=re.IGNORECASE)
    if m:
        # print(f"match: {m}")
        parts[0] = re.sub(r"\s*Apt\.*\s+No\.*\s+\d+\.*", r'', parts[0], flags=re.IGNORECASE)
        # print(f"parts[0]: {parts[0]}")
        name = ", ".join(parts)
        # print(f"{name}")


    # print(f"{name}")
    m = re.match(r".*\b[NS]\. *[EW]\.*$", name, flags=re.IGNORECASE)
    if m:
        # print(f"match: {m}")
        name = re.sub(r"\.$", r'', name, flags=re.IGNORECASE)
        # print(f"name: {name}")
        name = re.sub(r"([NSEW])\. *([NSEW])$", r'\1\2', name, flags=re.IGNORECASE)
        # print(f"name: {name}")
        # name = re.sub(r"([NSEW][NSEW])$", r'\1\2', name, re.IGNORECASE)
        pattern = "[NSEW][NSEW]$"
        name = re.sub(pattern, lambda match: match.group(0).upper(), name, flags=re.IGNORECASE)
        # print(f"name: {name}")


    m = re.match(r".*\bWashington\,*\b.*\bD\.* *c\.$", name, flags=re.IGNORECASE) 
    if m:
        # print(f"match: {m}")
        name = re.sub(r"\s+Washington\,*\s+.*\bD\.* *c\.$", r', Washington, District of Columbia, USA', name, flags=re.IGNORECASE)
        # print(f"name: {name}")



    # Transform names that obviously begin with a township designation
    m = re.match(r'^\d+\s+[NS]\s+\d+\s+[EW]', name, flags=re.IGNORECASE)
    if m:
        name = re.sub(r'^', r'Township ', name)            
        name = re.sub(r'(\d+\s+[EW])', r'Range \1', name, flags=re.IGNORECASE)            

    m = re.match(r'^\d+\s+[NS]\s+R\s+\d+\s+[EW]', name, flags=re.IGNORECASE)
    if m:
        name = re.sub(r'^', r'Township ', name)            
        name = re.sub(r'([NS])\s+R\s+', r'\1 Range ', name)            


    if len(parts) == 1:
        if parts[0] == "Ev":
            name = ""
        if parts[0] == "Sh":
            name = ""
        if parts[0] == "Sp":
            name = ""
        if parts[0] == "Fw":
            name = ""
        if parts[0] == "Rural":
            name = ""
        if parts[0] == "Suburban":
            name = ""
        if parts[0] == "This City":
            name = ""
        if parts[0] == "Railroad Board":
            name = ""
        if parts[0] == "North Main street":
            name = ""
        if parts[0] == "Salt/Lake-City":
            name = "Salt Lake City"


    return name



def strip_address_if_present(name: str, pid: int = 0) -> tuple[str, str | None]:
    """
    Attempts to strip a street address from the beginning of a place name.
    Returns the updated name and the stripped address if found.
    Leading punctuation is ignored. Matching is case- and punctuation-insensitive.
    """
    # print(f"[{inspect.currentframe().f_code.co_name}] pid: {pid}, name: \"{name}\"")

    if not name or not isinstance(name, str):
        # print(f"strip_address: returning early")
        return name, None

    # Skip Townships
    m = re.match(r"\bTownship\s+[0-9]+", name, flags=re.IGNORECASE)
    if m:
        return name, None

    m = re.match(r"\bRange\s+[0-9]+", name, flags=re.IGNORECASE)
    if m:
        return name, None


    original = name
    working = name.strip()

    # Remove leading symbols before first digit (e.g., "-410 N. Euclid")
    working = re.sub(r"^[^\w\d]*", "", working)

    # print(f"Try Fallback 1")
    # === Fallback 1: check if comma present and only evaluate portion before comma ===
    if "," in working:
        left, right = working.split(",", 1)
        prefix = re.sub(r"^[^\d]*", "", left.strip())  # remove symbols before number
        m = re.match(r"^\d{1,6}(?:\s+\S+){0,5}$", prefix)
        if m:
            address = left.strip()
            # print(f"address: {address}")
            remainder = right.strip()
            new_name = remainder if remainder else "NOPLACENAME"
            address = fix_address(address)
            # print(f"📍 PlaceID {pid} matched fallback1 with comma split: address = \"{address}\", name = \"{new_name}\"")
            return new_name, address

    # print(f"Try Primary match")
    # === Primary match: full address up to known suffix ===
    STREET_SUFFIXES = [
        "Street", "St", "Avenue", "Ave", "Road", "Rd", "Drive", "Dr", "Lane", "Ln",
        "Boulevard", "Blvd", "Court", "Ct", "Terrace", "Place", "Way", "Loop", "Trail",
        "Highway", "Hwy", "Parkway", "Pkwy", "Circle", "Plaza"
    ]
    suffixes_sorted = sorted(STREET_SUFFIXES, key=lambda s: -len(s))
    suffix_pattern = r'\b(?:' + '|'.join(re.escape(s) for s in suffixes_sorted) + r')\b'

    GEO_SUFFIXES = ["SW", "SE", "NE", "NW", "N", "S", "E", "W"]
    geo_suffixes_sorted = sorted(GEO_SUFFIXES, key=lambda s: -len(s))
    geo_suffix_pattern = r'\b(?:' + '|'.join(re.escape(s) for s in geo_suffixes_sorted) + r')\b'

    pattern = re.compile(
        r'^\s*'
        r'(?P<addr>\d{1,6}(?:\s+\S+){0,5})\s+' + suffix_pattern + geo_suffix_pattern +
        r'(?:\s*\.\s*|\s*,\s*|\s+)?'
        r'(?P<tail>.*)?$',
        flags=re.IGNORECASE
    )
    match = pattern.match(working)
    if match:
        address = working[:match.end()].strip()
        # print(f"address: {address}")
        tail = match.group('tail') or ''
        new_name = tail.strip() if tail.strip() else "NOPLACENAME"
        address = fix_address(address)
        # print(f"📍 PlaceID {pid} matched primary: address = \"{address}\", name = \"{new_name}\"")
        return new_name, address

    # print(f"Try Fallback 2")
    # === Fallback 2: any address-looking prefix without requiring suffix ===
    fallback2 = re.match(r"^\d{1,6}(?:\s+\S+){0,4}$", working)
    if fallback2:
        address = working.strip()
        # print(f"address: {address}")
        new_name = "NOPLACENAME"
        address = fix_address(address)
        # print(f"📍 PlaceID {pid} matched fallback2: address = \"{address}\", name = \"{new_name}\"")
        return new_name, address

#     print(f"Try Fallback 4")
#     # === Fallback 4: detect street-only part before comma
#     fb4 = fallback_street_before_comma(name)
#     if fb4:
#         new_name, address = fb4
#         # print(f"address: {address}")
#         address = fix_address(address)
#         # print(f"📍 fallback4 matched: addr: \"{address}\", new name: \"{new_name}\"")
#         return new_name, address


    # print(f"Try Fallback 5")
    # === Fallback 5: detect street-only, assumes only one field
    # ensure only one field
    if ',' in name:
       return name, None
    
    # strip any special character at the end
    name = re.sub(r"\W+$", "", name)
    name = re.sub(r"\.+$", "", name)
    name = re.sub(r"\,+$", "", name)
    # print(f"name: {name}")

    # If we end in a known contry name, skip it
    for country in FOREIGN_COUNTRIES:
        if name.endswith(country):
            # print(f"country found")
            return name, None

    # If we end in a known state or province name, skip it
    for state in STATE_NAMES:
        if name.endswith(state):
            # print(f"state found")
            return name, None

    for state in MEXICAN_STATES:
        if name.endswith(state):
            # print(f"mexican state found")
            return name, None

    for province in CANADIAN_PROVINCES:
        if name.endswith(province):
            # print(f"province found")
            return name, None

    for suffix in STREET_SUFFIXES:
        if name.endswith(suffix):
            # print(f"suffix found")
            address = name
            address = fix_address(address)
            # print(f"address: {address}")
            return "NOPLACENAME", address

    # === No more fallbacks

    return name, None





def normalize_once(pid, name, brief=True):
    # print(f"        [{inspect.currentframe().f_code.co_name}] pid: {pid} name: \"{name}\"")
    # print(f"[{inspect.currentframe().f_code.co_name}] {pid} {name}")
    
    # Check for exact match in COMMON_PLACE_MAPPINGS
    key = name.strip()
    for pattern, replacement in COMMON_PLACE_MAPPINGS.items():
        if key == pattern:
            name = replacement
            break  # Exact match found, skip further mapping


    # Check for exact match in SPECIAL_PLACE_MAPPINGS
    key = name.strip()
    for pattern, replacement in SPECIAL_PLACE_MAPPINGS.items():
        if key == pattern:
            name = replacement
            break  # Exact match found, skip further mapping


    # If 4 fields and ends with USA, remove ' County' from second field
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        if " County" in parts[1] and not is_legitimate_us_place_name(parts):
            parts[1] = parts[1].replace(" County", "")
            name = ", ".join(parts)


    # If 4 fields and ends with USA, remove ' County' from second field
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        if " County" in parts[1] and is_legitimate_us_place_name(parts):
            town = parts[0].lower()
            no_county = parts[1].replace(" County", "").lower()
            if not (town == no_county):
                state = parts[2].lower()
                if not (state == no_county):
                    no_county = parts[1].replace(" County", "")
                    parts[1] = no_county
                    name = ", ".join(parts)


    name = correct_misordered_county_name(name, brief=brief)


    # If 5 fields and ends with USA, and the first and second fields are indentical
    # remove the first field 
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 5 and parts[-1].upper() == "USA":
        if parts[0] == parts[1]:
            name = ", ".join(parts[1:-1])


    # Obvious replacements to take care of up front before they get managled
    name = re.sub(r'^No Township Listed,*\s+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^Rio Township, Rio,', r'Rio Township,', name, flags=re.IGNORECASE)
    name = re.sub(r'Floyd Knox, Floyd,',  r', Floyds Knobs, Floyd,', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+Shenandoah, Iowa', r', Shenandoah, Iowa', name, flags=re.IGNORECASE)
    name = re.sub(r'^Ohio, Preble Co$', r'Preble County, Ohio, USA', name, flags=re.IGNORECASE)
    name = re.sub(r'\(original\)', r'', name, flags=re.IGNORECASE)
    name = re.sub(r'\(new\)', r'', name, flags=re.IGNORECASE)
    name = re.sub(r'\(Issued Through\)', r'', name, flags=re.IGNORECASE)
    name = re.sub(r'^Route \d+$', r'', name, flags=re.IGNORECASE)

    
    # # get rid of addresses right away
    # # Strip an address if present
    # # this needs to be done *before*
    # # checking if an address in the "NOPLACENAME" routines below 
    # name = pp_for_strip_address(name)
    # name, addr = strip_address_if_present(name, pid)
    # if addr:
    #     # Optionally: save the stripped address somewhere (log, dict, etc.)
    #     # handling this is future work
    #     print(f"📍 PlaceID {pid} had its address: \"{addr}\" stripped, new name: \"{name}\"")


    # Capitalize first character of each field
    parts = [part.strip() for part in name.split(",")]
    normalized_parts = [p[0].upper() + p[1:] if p else "" for p in parts]
    name = ", ".join(normalized_parts)

    name = name.strip()

    normalized = name.strip()

    # Fix accidental splits in two-word U.S. state names
    us_two_word_states = {
        "West Virginia", "South Dakota", "North Dakota",
        "New York", "New Jersey", "New Mexico",
        "Rhode Island", "New Hampshire", "North Carolina", "South Carolina"
    }
    
    for state in us_two_word_states:
        parts = state.split()
        broken = f"{parts[0]}, {parts[1]}"
        fixed = state
        if broken in normalized:
            normalized = normalized.replace(broken, fixed)
            name = normalized
            break 


    # Strip an address if present
    # this needs to be done *before*
    # checking if an address in the "NOPLACENAME" routines below 
    name = pp_for_strip_address(name)
    if not name: 
        return "NOPLACENAME"


    # print(f"Before strip_address: name: \"{name}\"")
    name, addr = strip_address_if_present(name, pid)
    # print(f"After strip_address: name: \"{name}\"    addr: \"{addr}\"")
    if addr:
        # Optionally: save the stripped address somewhere (log, dict, etc.)
        # handling this is future work
        if not brief:
            print(f"📍 PlaceID {pid} had its address: \"{addr}\" stripped, new name: \"{name}\"")



    # when a place name is just a single character, remove it
    # making it a null string, another routine will delete the record
    # and update referencing records
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 1:
        if len(parts[0]) < 2:
           return "NOPLACENAME"

    original = name.strip()
    
    if len(original) <= 1:
        return "NOPLACENAME"
    
    # 1. Starts with number followed by common address pattern
    if re.match(r'^\d+\s+\w+', original):
        if re.search(r'\b(St|Ave|Blvd|Rd|Ln|Dr|Ct|Way|Circle|Pl|Terrace)\b', original, re.IGNORECASE):
            print(f"📍 PlaceID {pid} contains an address: \"{original}\", it will be marked for deletion")
            # maybe handle this later to prevent data bus
            return "NOPLACENAME"

    # 2. High digit ratio
    digit_ratio = sum(c.isdigit() for c in original) / max(len(original), 1)
    if digit_ratio > 0.5:
        return "NOPLACENAME"

    # 3. Obvious placeholders
    if original.lower() in {"unknown", "n/a", "-", ".", "..."}:
        return "NOPLACENAME"

    # 4. No alphabetic characters
    if not any(c.isalpha() for c in original):
        return "NOPLACENAME"



    # Normalize PLSS-like entries
    name = re.sub(
        r'\bT(?:wp)?\s*(\d+)([NS])\s*R\s*(\d+)([EW])\b',
        r'Township \1\2 Range \3\4',
        name
    )


    # Fix names that have no separator after the Township when the township is named
    m = re.match(r'^.*[a-z]\s+Township\s+[a-z].*', name, re.IGNORECASE)
    if m:
        name = re.sub(r'^(.*?\s+Township)', r'\1,', name, re.IGNORECASE)




    # Add missing comma before known state names (e.g., 'Twin Falls Idaho' → 'Twin Falls, Idaho')
    for state in STATE_NAMES:
        if name.endswith(" " + state) and not name.endswith(", " + state):
            name = re.sub(rf" (?!.*, ){state}$", f", {state}", name)
            break  # Only apply to one state match

    # Insert comma before state abbreviation if missing
    for abbr in STATE_ABBREVIATIONS:
        pattern = rf"\b(.+?)\s+{abbr}$"
        if re.search(pattern, name):
            name = re.sub(pattern, rf"\1, {abbr}", name)
            break  # only fix once

    # Ensure comma before historical U.S. territory names
    for territory in HISTORICAL_US_TERRITORIES:
        pattern = rf"\b(.+?)\s+{re.escape(territory)}$"
        if re.search(pattern, name):
            name = re.sub(pattern, rf"\1, {territory}", name)
            # remove double comma
            name = re.sub(r',,', r',', name)
            # fix doubles in the end
            parts = [p.strip() for p in name.split(",")]
            if len(parts) >= 2 and parts[-1] == parts[-2]:
                name = ", ".join(parts[0:-1])
            break  # only one match expected


    # if there is only one field and it is the abbreviation of a country
    # or shortened name, correct to the full name
    # other substitutions for single field names
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 1:
        if name == "USA" or name == "United States":
            name = "United States of America"
        if name == "Deutschland":
            name = "Germany"
        if name == "Columbia":
            name = "Colombia"
        



    name = fix_missing_commas_in_county_state(name)


    # Add comma before 'Mexico' unless it's part of 'New Mexico'
    if " Mexico" in name and "New Mexico" not in name:
        name = re.sub(r"(?<!New) Mexico", r", Mexico", name)

    # Remove 3–5 letter uppercase prefixes (e.g., "KYRO - ", "NYCA - ")
    name = re.sub(r"^[A-Z]{4,5} - ", "", name)
    name = re.sub(r"^[A-Z]{4}[0-9] - ", "", name)
    name = re.sub(r"^[A-Z]{3} - ", "", name)

    # Remove lone trailing period
    name = re.sub(r"\.\s*$", "", name)

    # Insert comma before known country names if missing
    for country in FOREIGN_COUNTRIES:
        if name.endswith(" " + country):
            name = name[: -len(country) - 1].rstrip(", ") + ", " + country





    # Fix names like 'highpoint IA' → 'Highpoint, Iowa, USA'
    tokens = name.split()
    if len(tokens) == 2 and tokens[1] in STATE_ABBREVIATIONS:
        name = f"{tokens[0]}, {STATE_ABBREVIATIONS[tokens[1]]}, USA"

    # Fix state-only names like 'KY' → 'Kentucky, USA'
    if name.strip() in STATE_ABBREVIATIONS:
        name = f"{STATE_ABBREVIATIONS[name.strip()]}, USA"


    # Fix state-only names like 'Virginia' → 'Virginia, USA'
    if name.strip() in STATE_NAMES:
        name += ", USA"





    # Ensure a comma precedes valid Mexican state names (excluding 'New Mexico')
    if not name.endswith("New Mexico, USA"):  # exclude legitimate U.S. state
        for state in MEXICAN_STATES:
            if (
                state == "México"
            ):  # special case to avoid conflict with 'Mexico' country
                continue
            pattern = rf"(?<!,),\s*{state}, Mexico$"
            fixed_pattern = rf", {state}, Mexico"
            if name.endswith(f" {state}, Mexico"):
                name = re.sub(rf" {state}, Mexico$", fixed_pattern, name)
                break

    # Ensure a comma precedes Canadian province names if missing
    if name.endswith(", Canada"):
        for province in CANADIAN_PROVINCES:
            if name.endswith(f" {province}, Canada"):
                name = re.sub(rf" {province}, Canada$", rf", {province}, Canada", name)
                break


    # ───────────────────────────────────────────────
    # 🧹 Strip parenthetical text like (Independent City), (new), (7 yrs), etc.
    # Apply only once, before other cleanup
    name = re.sub(r"\s*\([^()]*\)", "", name)  # remove parentheses and enclosed text
    name = re.sub(r"\s{2,}", " ", name).strip(",. ")  # clean excess spaces and trailing punctuation

    # remove leading hyphen '-' in name
    name = re.sub(r"^-", "", name)


    name = re.sub(r", United States of America$", ", USA", name)
    name = re.sub(r", U\.S\.A$", ", USA", name)
    name = re.sub(r", U\.S\.A\.$", ", USA", name)
    name = re.sub(r", U\.S\.$", ", USA", name)
    name = re.sub(r", United States$", ", USA", name)
    name = re.sub(r"^,\s*", "", name)
    name = re.sub(r",+$", "", name)
    name = re.sub(r",\s*,", ",", name)
    name = re.sub(r",\s*", ", ", name)
    name = re.sub(r" \d{5},", "", name)
    name = re.sub(r" Co\.", " County", name)
    name = re.sub(r"Co ", "County ", name)
    name = re.sub(r" Co,", " County,", name)
    name = re.sub(r" Co$", " County", name)
    name = re.sub(r" Coun,", " County,", name)
    name = re.sub(r"^County, ", "County ", name)
    name = re.sub(r"([A-Z,a-z,0-9])&([A-Z,a-z,0-9])", r"\1 & \2", name)
    name = re.sub(r"^Rural, ", "", name)
    name = re.sub(r"\(Chicago\)", "", name)
    name = re.sub(r" Ward [0-9],", ",", name)
    name = re.sub(r" Ward [0-9][0-9],", ",", name)
    name = re.sub(r"^District [0-9], ", "", name)
    name = re.sub(r"^District [0-9][0-9], ", "", name)
    name = re.sub(r" Twp,", ",", name)
    name = re.sub(r" Twp.,", ",", name)
    name = re.sub(r"^Magisterial ", "", name)
    name = re.sub(r" Assembly District [0-9],", ",", name)
    name = re.sub(r" Assembly District [0-9][0-9],", ",", name)
    name = re.sub(r"^District No [0-9], ", "", name)
    name = re.sub(r"^District No [0-9][0-9], ", "", name)
    # name = re.sub(r"^Township [0-9], ", "", name)
    # name = re.sub(r"^Township [0-9][0-9], ", "", name)
    name = re.sub(r"^Precinct [0-9], ", "", name)
    name = re.sub(r"^Precinct [0-9][0-9], ", "", name)
    name = re.sub(r" Irland$", " Ireland", name)
    name = re.sub(r"^Mag District No [0-9], ", "", name)
    name = re.sub(r"^Mag District No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist No [0-9], ", "", name)
    name = re.sub(r"^Mag Dist No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D No [0-9], ", "", name)
    name = re.sub(r"^Mag D No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District [0-9], ", "", name)
    name = re.sub(r"^Mag District [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist [0-9], ", "", name)
    name = re.sub(r"^Mag Dist [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D [0-9], ", "", name)
    name = re.sub(r"^Mag D [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District \# [0-9], ", "", name)
    name = re.sub(r"^Mag District \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist \# [0-9], ", "", name)
    name = re.sub(r"^Mag Dist \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D \# [0-9], ", "", name)
    name = re.sub(r"^Mag D \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District \#[0-9], ", "", name)
    name = re.sub(r"^Mag District \#[0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist \#[0-9], ", "", name)
    name = re.sub(r"^Mag Dist \#[0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D \#[0-9], ", "", name)
    name = re.sub(r"^Mag D \#[0-9][0-9], ", "", name)
    name = re.sub(r"Fraanklin, ", "Franklin, ", name)
    name = re.sub(r"Bethlehm, ", "Bethlehem, ", name)
    name = re.sub(r"Abingon, ", "Abingdon, ", name)
    name = re.sub(r"Grrenv", "Grenv", name)
    name = re.sub(r"Los Angles", "Los Angeles", name)
    name = re.sub(r"Indianapoli,", "Indianapolis,", name)
    name = re.sub(r"St Louis", "St. Louis", name)
    name = re.sub(r'\bSaint\s+(?=\w)', 'St. ', name)
    name = re.sub(r'\bSt\s+(?=\w)', 'St. ', name)
    name = re.sub(r'\bPrince Georges\b', "Prince George's", name)
    name = re.sub(r"Fond Du Lac", "Fond Du Lac", name)



    # Remove parenthetical prefixes like "City (Districts 1234-5678), ..."
    name = re.sub(r'^[^,]*\([^)]*\),\s*', '', name)

    # Remove leading parenthetical if followed by duplicate city
    name = re.sub(
        r'^\s*\((.*?)\),\s*(\w[\w\s.-]+?),\s*(.+)$',
        lambda m: f"{m.group(2)}, {m.group(3)}" if m.group(2).lower() in m.group(3).lower() else m.group(0),
        name
    )


    # Get County in the middle fixed
    name = re.sub(r" County ", " County, ", name)

    # Remove lone periods (again)
    name = re.sub(r"\s+\.\s+", " ", name)

    # Remove double commas and extra spaces between them
    name = re.sub(r",\s*,", ", ", name)

    # Remove trailing lone periods (e.g., "Oklahoma.")
    name = re.sub(r"\.\s*$", "", name)

    # Remove trailing commas (if still remaining)
    name = re.sub(r",\s*$", "", name)

    # Collapse repeated whitespace
    name = re.sub(r"\s{2,}", " ", name)

    for abbr, full in OLD_STYLE_ABBR.items():
        name = re.sub(rf"\b{re.escape(abbr)}\b", full, name)

    for abbr, full in STATE_ABBREVIATIONS.items():
        name = re.sub(rf",\s*{abbr}(,|$)", rf", {full}\1", name)



    if any(name.endswith(", " + state) for state in STATE_NAMES) and not name.endswith(
        ", USA"
    ):
        name += ", USA"


    # # If name ends with a historical US territory and does not already end in ', USA', append ', USA'
    # if any(name.endswith(", " + territory) for territory in HISTORICAL_US_TERRITORIES):
    #     if not name.endswith(", USA"):
    #         name += ", USA"

    # Fix Canadian places missing a comma before the province
    for province in CANADIAN_PROVINCES:
        pattern = rf"\b({province})$"
        if re.search(pattern, name, re.IGNORECASE):
            if f", {province}" not in name:
                # Insert comma before the province
                name = re.sub(rf"\s+{province}$", rf", {province}", name)

    # If ends with a known Canadian province but not ", Canada", append it
    parts = [p.strip() for p in name.split(",")]
    if parts[-1] in CANADIAN_PROVINCES and not name.endswith(", Canada"):
        name += ", Canada"

    # If ends with a known Mexican state but not ", Mexico", append it
    parts = [p.strip() for p in name.split(",")]
    if parts[-1] in MEXICAN_STATES and not name.endswith(", Mexiso"):
        name += ", Mexiso"


    # If 4 fields and ends with USA, remove ' County' from second field
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        if " County" in parts[1] and not is_legitimate_us_place_name(parts):
            parts[1] = parts[1].replace(" County", "")
            name = ", ".join(parts)


    # Fix repeated state name before 'USA'
    parts = [p.strip() for p in name.split(",")]
    if len(parts) >= 4 and parts[-1] == "USA":
        # Check if the second-to-last and third-to-last parts are the same
        if parts[-2].lower() == parts[-3].lower():
            # Remove the redundant part
            # are only going to do this when the state doesn't have a county
            # with the same name
            # 
            # but take this opportunity to add the word County to the 
            # County name to make it clear
            if not (parts[-2].lower() == "arkansas" or
                    parts[-2].lower() == "idaho" or
                    parts[-2].lower() == "oklahoma" or
                    parts[-2].lower() == "iowa" or
                    parts[-2].lower() == "utah" or
                    parts[-2].lower() == "hawaii" or
                    parts[-2].lower() == "new york"):
                del parts[-3]
                name = ", ".join(parts)
            else:
                county = parts[-3]
                county += " County"
                parts[-3] = county
                name = ", ".join(parts)

    # # Standarize when only the county is known to a list
    # # of known USA counties
    # name = standardize_us_county_name(name, COUNTY_DB, STATE_NAMES)

    return name


def normalize_place_iteratively(pid, name, brief=True):
    # print(f"    [{inspect.currentframe().f_code.co_name}] pid: {pid} name: \"{name}\"")
    # print(f"[{inspect.currentframe().f_back.f_code.co_name}] {pid} {name}")
    previous = name
    count = 0
    while True:
        count = count + 1
        # print(f"    [{inspect.currentframe().f_code.co_name}] Calling normalize_once, count: {count}")
        current = normalize_once(pid, previous, brief=brief)
        # print(f"    [{inspect.currentframe().f_code.co_name}] normalize_once returned with \"{current}\"")
        if current == previous:
            break
        previous = current
    return current if current != name else None


def normalize_place_names(conn: sqlite3.Connection, dry_run=True, brief=True):
    from rmutils import delete_place_id, current_utcmoddate
    cursor = conn.execute("SELECT PlaceID, Name FROM PlaceTable WHERE PlaceType != 1")
    updates = []

    for row in cursor.fetchall():
        place_id, old_name = row["PlaceID"], row["Name"]
        new_name = normalize_place_iteratively(place_id, old_name, brief=brief)
        if new_name:
            if new_name == "NOPLACENAME":
                if not brief:
                    print(f"🧹 PlaceID {place_id} had an old name of \"{old_name}\" and will be deleted")
                ret = delete_place_id(conn, place_id, dry_run, brief=brief)
            else:
                if not brief:
                    print(f"🧹 PlaceID {place_id} had an old name of \"{old_name}\" and will be updated to \"{new_name}\"")
                updates.append((place_id, old_name, new_name))
          

    if not updates:
        print("✅ No changes needed.")
        return

    print(f"✏️  Found {len(updates)} places to normalize.")

    # # Write log
    # with open(args.logfile, "w", encoding="utf-8") as log:
    #     for pid, old, new in updates:
    #         print(f"[{pid:5}] {old:<80} → {new}")
    #         log.write(f"[{pid}] {old} → {new}\n")

    # Optional database commit
    if not dry_run:
        for place_id, _, new_name in updates:
            # Update Reverse and UTCModDate when modifying the place name
            reverse = reverse_place_name(new_name)
            utcmoddate = current_utcmoddate()

            cursor.execute(
                """
                UPDATE PlaceTable
                SET Name = ?, Reverse = ?, UTCModDate = ?
                WHERE PlaceID = ?
                """,
                (new_name, reverse, utcmoddate, place_id),
            )
        conn.commit()
        print("✅ Changes committed to the database.")
    else:
        print("ℹ️  Dry run only. No changes made. Use dry_run=False to apply.")


def is_legitimate_us_place_name(parts: list[str]) -> bool:
    """
    Returns True if parts[1] includes 'County', parts[2] is a valid US state name,
    and (parts[1] without ' County', parts[2]) is a valid (county, state) pair.
    Intended for protecting legitimate US county names in normalization.
    """
    if len(parts) != 4 or parts[-1].strip().upper() != "USA":
        return False

    county_field = parts[1].strip()
    state = parts[2].strip()

    if "County" not in county_field or state not in STATE_NAMES:
        return False

    county_name = county_field.replace(" County", "").strip()
    return (county_name, state) in US_COUNTIES


def is_nonsensical_place_name(name):
    if name is None:
        return True
    name = name.strip()
    if not name:
        return True
    if len(name) <= 1:
        return True
    if name.upper() in {"UNKNOWN", "UNK", "NONE", "NA"}:
        return True
    if all(c in "?.," for c in name):
        return True
    return False




def correct_misordered_county_name(name: str, brief: bool=False) -> str:
    """
    Fix place names like 'Clay County, Clay, Indiana, USA' → 'Clay, Clay County, Indiana, USA'
    Only apply fix if county-state pair is in US_COUNTIES.
    """
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        state = parts[2]
        if state in STATE_NAMES and parts[0].endswith(" County"):
            county_name = parts[0].replace(" County", "")
            if (county_name, state) in US_COUNTIES:
                return f"{parts[1]}, {parts[0]}, {state}, USA"
    return name  # no change



def fix_missing_commas_in_county_state(name: str) -> str:
    """
    Fix entries like "Edinburg Shenandoah, Virginia, USA" to "Edinburg, Shenandoah, Virginia, USA"
    by inserting a missing comma between city and county.
    """
    parts = name.split(",")
    if len(parts) < 2:
        return name  # Nothing to fix

    head = parts[0].strip()
    tail = ",".join(p.strip() for p in parts[1:])

    for county, state in US_COUNTIES:
        test_suffix = f"{county}, {state}"
        if tail.startswith(test_suffix):
            if head.endswith(county):
                # Already something like "Edinburg Shenandoah"
                fixed_head = head.replace(f" {county}", f", {county}")
                return f"{fixed_head}, {tail}"
    return name


def reverse_place_name(name: str) -> str:
    """Reverse the order of comma-separated fields in a place name."""
    parts = [part.strip() for part in name.split(",")]
    return ", ".join(reversed(parts))


def standardize_us_county_name(name, counties_db, state_list):
    """
    Normalize U.S. county-style place names to the form:
    <county> County, <state>, USA
    """
    parts = [p.strip() for p in name.split(",")]
    if len(parts) != 3:
        return name  # Not a candidate

    county_candidate, state, country = parts
    if country.upper() != "USA":
        return name
    if state not in state_list:
        return name

    lookup = (f"{county_candidate} county".lower(), state.lower())
    for county, county_state in counties_db:
        if (county, county_state) == lookup:
            # Return properly capitalized replacement
            return f"{county_candidate} County, {state}, USA"

    return name  # No match found



def suggest_us_place_correction(place: str) -> str:
    """
    Normalize place names of the form 'City, City, State, USA' using US_PLACES,
    but only if the derived county and state pair is also found in US_COUNTIES.

    If the derived county is not a real county, emit a warning and return
    the original name.
    """
    parts = [p.strip() for p in place.split(",")]
    if len(parts) != 4 or parts[-1].upper() != "USA":
        return place  # Must be 4-part ending in USA

    city, county_candidate, state, country = parts

    if city.lower() != county_candidate.lower():
        return place  # First two fields must match

    if state not in STATE_NAMES:
        return place  # Not a known U.S. state

    county_name = f"{county_candidate} County"
    candidate_tuple = (city, county_name, state)

    if candidate_tuple in US_PLACES:
        # Validate that the county is legitimate
        if (county_candidate, state) in US_COUNTIES:
            return f"{city}, {county_name}, {state}, {country}"
        else:
            print(f"⚠️ Warning: '{county_name}, {state}' not in US_COUNTIES. Skipping normalization of '{place}'")
            return place

    return place


def assign_county_if_known_place(place: str) -> str:
    """
    To a three part place, ending in USA, and if
    it is a actual state
    If the derived county is not a real county, emit a warning and return
    the original name.
    """
    parts = [p.strip() for p in place.split(",")]
    if len(parts) != 3 or parts[-1].upper() != "USA":
        return place  # Must be 4-part ending in USA

    city, state, country = parts

    if state not in STATE_NAMES:
        return place  # Not a known U.S. state

    for entry in US_PLACES:
        if len(entry) != 3:
            continue
        entry_city, entry_county, entry_state = entry

        if city == entry_city and state == entry_state:
            county_clean = entry_county.replace(" County", "").strip()
            return f"{city}, {county_clean}, {state}, {country}"

    return place


def known_county_inserted(place: str) -> tuple[str, bool]:
    """
    Return a tuple of (normalized_place, changed_flag)
    """
    normalized = assign_county_if_known_place(place)
    return normalized, (normalized != place)



def normalize_if_matched(place: str) -> tuple[str, bool]:
    """
    Return a tuple of (normalized_place, changed_flag)
    """
    normalized = suggest_us_place_correction(place)
    return normalized, (normalized != place)



