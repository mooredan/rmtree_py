import sqlite3
from collections import defaultdict
import inspect
import csv
from rmutils import (
    get_connection,
    find_duplicate_place_names,
    delete_blank_place_records,
    report_non_normalized_places,
    merge_places,
    get_place_name_from_id,
    dump_place_usage,
    is_place_referenced,
    get_all_place_ids,
    delete_place_id,
    find_matches_against_known_segments,
    get_single_field_places,
    is_foreign_country,
    is_us_territory,
    print_event_references_for_place_ids,
    infer_and_insert_missing_county,
    update_place_name,
)

from normalizer import (
    strip_address_if_present,
    normalize_place_names,
    normalize_place_iteratively,
    normalize_if_matched,
    assign_county_if_known_place,
    known_county_inserted,
)



def sample_normalize(conn: sqlite3.Connection, place_id):
    orig_name = get_place_name_from_id(conn, place_id)
    # print(f"    [{inspect.currentframe().f_code.co_name}] pid: {pid} name: \"{name}\"")
    print(f"  [{inspect.currentframe().f_code.co_name}] place_id: {place_id}  Name: \"{orig_name}\"")
    new_name = normalize_place_iteratively(place_id, orig_name)
    print(f"  [{inspect.currentframe().f_code.co_name}] new_name: \"{new_name}\"")
    if new_name:
        if new_name == "NOPLACENAME":
            print(f"  [{inspect.currentframe().f_code.co_name}] place_id: {place_id} had an original name of \"{orig_name}\" and will be deleted ...")
            return new_name
        else:
            print(f"  [{inspect.currentframe().f_code.co_name}] place_id: {place_id}  Name will be updated to \"{new_name}\"")
            return new_name
    else:
        print(f"  [{inspect.currentframe().f_code.co_name}] place_id: {place_id} Original name \"{orig_name}\" will not be updated")
        return orig_name


def test_pid(conn: sqlite3.Connection, place_id):
    orig_name = get_place_name_from_id(conn, place_id)
    print(f"\nPlaceID: {place_id}    Original Name: \"{orig_name}\"")
    normalized_name = sample_normalize(conn, place_id)
    print(f"PlaceID: {place_id}  Normalized Name: \"{normalized_name}\"")


def test_strip_address_by_pid(conn: sqlite3.Connection, place_id):
    print(f"\n[{inspect.currentframe().f_code.co_name}] place_id: {place_id}")
    orig_name = get_place_name_from_id(conn, place_id)
    print(f"[{inspect.currentframe().f_code.co_name}] orig_name: {orig_name}")
    new_name, addr = strip_address_if_present(orig_name, place_id)
    print(f"[{inspect.currentframe().f_code.co_name}] new_name: \"{new_name}\", addr: \"{addr}\" ")

def test_strip_address_by_name(conn: sqlite3.Connection, name):
    print(f"\n[{inspect.currentframe().f_code.co_name}] name: \"{name}\"")
    new_name, addr = strip_address_if_present(name, 9999)
    print(f"[{inspect.currentframe().f_code.co_name}] new_name: \"{new_name}\", addr: \"{addr}\" ")


def test_addresses():
    #read the entire line 
    # df = pd.read_csv('address_test.csv', header=None, sep='|')
    pass_count = 0
    fail_count = 0

    with open('address_test.csv', mode='r', newline='') as file:
        reader = csv.reader(file, delimiter='|')
        for row in reader:
            place_id = row[0]
            orig_name = row[1]
            expected_addr = row[2]
            expected_name = row[3]

            print(f"\n🧭 Test: {place_id} : \"{orig_name}\"")
            new_name, addr = strip_address_if_present(orig_name, place_id)
            error_flag = False 

            if addr:
                if addr != expected_addr:
                    print(f"   🚫 No match (addr): {place_id} : \"{addr}\" vs. expected: \"{expected_addr}\"")
                    error_flag = True 
            else:
               if expected_addr != "None":
                    print(f"   🚫 No match (addr): {place_id} : \"{addr}\" vs. expected: \"{expected_addr}\"")
                    error_flag = True 
   
 
            if new_name != expected_name:
                print(f"   🚫 No match (name): {place_id} : \"{new_name}\" vs. expected: \"{expected_name}\"")
                error_flag = True 


            if error_flag:
               print(f"🚫 FAIL: {place_id}")
               fail_count += 1
            else:
               print(f"✅ PASS: {place_id}")
               pass_count += 1

        if fail_count > 0:
           print(f"\n🚫 Pass: {pass_count}  Fail: {fail_count}")
           return False
        else:
           print(f"\n✅ Pass: {pass_count}  Fail: {fail_count}")
           return True 




def delete_unused_places(conn: sqlite3.Connection, dry_run=True, brief=False):
    place_ids = get_all_place_ids(conn)
    unused_count = 0
    for pid in place_ids:
        # Do something with each PlaceID
        if not is_place_referenced(conn, pid, quiet=brief):
            unused_count += 1
            name = get_place_name_from_id(conn, pid)
            print(f"This PlaceID {pid} is not referenced: name: \"{name}\"")
            # dump_place_usage(conn, pid)
            ret = delete_place_id(conn, pid, dry_run=dry_run, brief=brief)
            if not ret:
                print(f"🚫 delete_place_id returned False for pid: {pid}")
    if unused_count > 0:
        print(f"{unused_count} PlaceIDs were not used and deleted\n")



def do_merge_places(conn: sqlite3.Connection, dry_run=True, brief=False):
    dupes = find_duplicate_place_names(conn, brief=brief)
    num_dupes = len(dupes)
    print(f"Number of duplicates found: {num_dupes}\n")

    # let's merge those, if there are duplicates
    if num_dupes > 0:
        merge_places(conn, dupes, dry_run=dry_run, brief=brief)





def fix_places(conn: sqlite3.Connection, dry_run=True, brief=False):
    ##################################
    # Delete unused places
    ##################################
    delete_unused_places(conn, dry_run=dry_run, brief=brief)

    #######################################################
    # Find PlaceIDs where the place name is identical
    # Find duplicates and merge
    #######################################################
    do_merge_places(conn, dry_run=dry_run, brief=brief)


    ##################################################
    # do our best at renaming PlaceTable names
    ##################################################
    normalize_place_names(conn, dry_run=dry_run, brief=brief)


    ####################################################
    # Fix up missing county
    ####################################################
    infer_and_insert_missing_county(conn, dry_run=dry_run)


    #######################################################
    # Find PlaceIDs where the place name is identical
    # Find duplicates and merge
    #######################################################
    do_merge_places(conn, dry_run=dry_run, brief=brief)



    #################################################
    # fix up quadruples that have an obvious missing
    # county name
    #################################################
    place_ids = get_all_place_ids(conn)
    for pid in place_ids:
        place = get_place_name_from_id(conn, pid)
        normalized_place, was_changed = normalize_if_matched(place)
        if was_changed:
            print(f"✔ Normalized: {place} → {normalized_place}")
            print(f"📝 Updating PlaceID: {pid} name to {normalized_place}'")
            update_place_name(conn, pid, normalized_place)



    #################################################
    # fix triples in the USA that are missing the
    # county, but state is known
    # e.g. Wadsworth, Illinois, USA should become
    #      Wadsworth, Lake, Illinois, USA
    #################################################
    place_ids = get_all_place_ids(conn)
    for pid in place_ids:
        place = get_place_name_from_id(conn, pid)
        normalized_place, was_changed = known_county_inserted(place)
        if was_changed:
            print(f"✔ County added: {place} → {normalized_place}")
            print(f"📝 Updating PlaceID: {pid} name to {normalized_place}'")
            update_place_name(conn, pid, normalized_place)



    #######################################################
    # Find PlaceIDs where the place name is identical
    # Find duplicates and merge
    #######################################################
    do_merge_places(conn, dry_run=dry_run, brief=brief)


    ##################################
    # Delete unused places
    ##################################
    delete_unused_places(conn, dry_run=dry_run, brief=brief)



def funny_place_report (conn: sqlite3.Connection, brief: bool = False):
    ##########################################################
    # reporting and analysis
    ##########################################################
    # report singles.....
    name_list = []
    pid_list = []
    single_field_places = get_single_field_places(conn)
    for pid, name in single_field_places:
        # print(f"{pid} {name}")
        if (is_foreign_country(name)):
            continue
        if (is_us_territory(name)):
            continue
        if name == "Mexico":
            continue
        # print(f"{pid} {name}")
        name_list.append(name)
        pid_list.append(pid)


    # with open("place_names.txt", "w", encoding="utf-8") as f:
    #     for item in name_list:
    #         f.write(item + "\n")

    num_leftovers = len(name_list) 
    if num_leftovers > 0:
        print(f"leftovers: {num_leftovers}\n")
        print_event_references_for_place_ids(conn, pid_list)
        print(f"\n")

    find_matches_against_known_segments(conn)

    # what is left over?
    report_non_normalized_places(conn)




def devel():
    # open the connection to the database
    conn = get_connection()

    dry_run = False
    brief = False

    fix_places(conn, dry_run=dry_run, brief=brief)

    funny_place_report(conn, brief=False)

    conn.close()

if __name__ == "__main__":
    devel()
