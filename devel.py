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
)

from normalizer import (
    strip_address_if_present,
    normalize_place_names,
    normalize_place_iteratively,
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


def devel():
    # open the connection to the database

    conn = get_connection()

    # # test_strip_address_by_pid(conn, 4906)
    # test_strip_address_by_name(conn, "511 E. North St.")
    # return

    # rtn = test_addresses()
    # return

    # for place_id in {5837, 4582, 6034, 4906, 4354, 4975, 5185, 476, 2678}:
    #    test_pid(conn, place_id)
    # return


    place_ids = get_all_place_ids(conn)
    unused_count = 0
    for pid in place_ids:
        # Do something with each PlaceID
        if not is_place_referenced(conn, pid, quiet=True):
            unused_count += 1
            name = get_place_name_from_id(conn, pid)
            print(f"This PlaceID {pid} is not referenced: name: \"{name}\"")
            # dump_place_usage(conn, pid)
            ret = delete_place_id(conn, pid, dry_run=False, brief=True)
            if not ret:
                print(f"🚫 delete_place_id returned False for pid: {pid}")

    print(f"{unused_count} PlaceIDs were not used and deleted")
    # return

    # do our best at renaming PlaceTable names
    normalize_place_names(conn, dry_run=False)

    #  delete_blank_place_records(conn, dry_run=True)

    # Find PlaceIDs where the place name is identical
    dupes = find_duplicate_place_names(conn)
    num_dupes = len(dupes)
    print(f"Number of duplicates found: {num_dupes}\n")

    # let's merge those
    merge_places(conn, dupes, dry_run=False, brief=True)

    # what is left over?
    report_non_normalized_places(conn)

    conn.close()


if __name__ == "__main__":
    devel()
