import sys
import yaml

main_yaml_file, columns_file = sys.argv[1:]

with open(main_yaml_file, 'r') as file:
    place_map = yaml.safe_load(file)

place_reverse_map = {}
for label in place_map:
    place_reverse_map['.'.join(place_map[label])] = label

cons_map = {}
with open(columns_file, 'r') as file:
    for line in file:
        cons_label, ntp_label, type = line.replace('"','').strip().split('\t')
        cons_map[cons_label] = ntp_label

final_map = {}
for label in place_reverse_map:
     try:
        final_map[label] = cons_map[place_reverse_map[label]]
     except KeyError:
        final_map[label] = f"{label}|{place_reverse_map[label]}|ERROR"

print ('"ORIGINAL"\t"DBFIELD"')
for label in final_map:
    print(f"\"{label}\"\t\"{final_map[label]}\"")        


