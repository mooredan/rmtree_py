from normalizer import strip_address_if_present, pp_for_strip_address
import csv

def test_addresses():
    pass_count = 0
    fail_count = 0

    try:
        with open('address_test.csv', mode='r', newline='') as file:
            reader = csv.reader(file, delimiter='|')
            for row in reader:
                place_id = row[0]
                orig_name = row[1]
                expected_addr = row[2]
                expected_name = row[3]

                orig_name = pp_for_strip_address(orig_name)
    
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
    
    except FileNotFoundError:
        print("Error: The file was not found.")
    except PermissionError:
        print("Error: You do not have permission to access this file.")
    except IOError as e:
        print(f"An I/O error occurred: {e}")

if __name__ == '__main__':
    rtn = test_addresses()
