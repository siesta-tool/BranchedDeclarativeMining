import os

import requests, os


def extract_string_constraints(json):
    r = []
    for rule in json['position patterns']:
        for c in json['position patterns'][rule]:
            ev_a = c["ev"]
            sup = c["support"]
            if float(sup) > 0:
                r.append(f"{rule}|{ev_a}|{sup}")
    for rule in json['existence patterns']:
        if json['existence patterns'][rule]:
            for c in json['existence patterns'][rule]:
                sup = c["support"]
                if "ev" in c:
                    ev_a = c["ev"]
                    n = c["n"]
                    if float(sup) > 0:
                        r.append(f"{rule}|{ev_a}|{n}|{sup}")
                else:
                    ev_a = c["evA"]
                    ev_b = c["evB"]
                    if float(sup) > 0:
                        r.append(f"{rule}|{ev_a}|{ev_b}|{sup}")


    for rule in ["not-succession", "precedence", "response", "succession"]:
        for mode in ["", " alternate", " chain"]:
            for c in json[f"ordered relations{mode}"][rule]:
                ev_a = c["evA"]
                ev_b = c["evB"]
                sup = c["support"]
                if mode != "":
                    if rule=="not-succession":
                        con = f"not-{mode.lstrip()}-succession"
                    else:
                        con = f"{mode.lstrip()}-{rule}"
                else:
                    con = rule
                if float(sup) > 0:
                    r.append(f"{con}|{ev_a}|{ev_b}|{sup}")
    return set(r)


if __name__ == "__main__":
    siesta_server = "http://localhost:8090"
    logname = "test_inc"
    support = 0
    folder_path = '../output'
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    incremental_file = max(files, key=os.path.getmtime)
    print(os.getcwd())

    # submit  query  to siesta to get all declare constraints
    url = f"{siesta_server}/declare/?log_database={logname}&support={support}"
    response = requests.get(url)
    # create the strings of each rule
    s = extract_string_constraints(response.json())
    # read rules from incremental
    incr_constraints = []
    with open(incremental_file,'r') as f:
        for line in f:
            l = line.replace("\n","")
            incr_constraints.append(l)

    print("Not contained in SIESTA original")
    for x in incr_constraints:
        l=x.split("|")
        found = False
        if "|".join(l) not in s:
            for rule in s:
                if "|".join(l[:-1])  in rule:
                    sup = rule.split("|")[-1]
                    print("|".join(l),f"has diff support: {sup}")
                    found=True
                    break
            if not found:
                print("|".join(l),"does not exist")



    print("\nNot contained in SIESTA incremental")
    for x in s:
        l=x.split("|")
        found = False
        if "|".join(l) not in incr_constraints:
            for rule in incr_constraints:
                if "|".join(l[:-1])  in rule:
                    sup = rule.split("|")[-1]
                    print("|".join(l),f"has diff support: {sup}")
                    found=True
                    break
            if not found:
                print("|".join(l)," does not exist")
