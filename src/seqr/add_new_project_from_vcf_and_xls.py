#!/usr/bin/env python3
import sys
import os
import argparse
import yaml
from datetime import datetime, date


p = argparse.ArgumentParser("")
p.add_argument("-i", "--project-id", help="Project id", required=True)
p.add_argument("-n", "--project-name", help="Project name", required=True)
p.add_argument("-v", "--vcf", help="The vcf file", required=True)
p.add_argument("-x", "--xls", help="The xls file", required=True)
p.add_argument("--no-phenotips", action="store_true", help="don't add to phenotips")
p.add_argument("-f", "--force", help="Force annotation", action="store_true")
p.add_argument("-r", dest="run", action="store_true", help="Actually run the commands")
opts = p.parse_args()

vcf = opts.vcf
xls = opts.xls
ped = xls.replace(".xlsx", "").replace(".xls", "") + ".ped"
project_id = opts.project_id
project_name = opts.project_name

if not os.path.isfile(vcf):
    p.error("Invalid vcf: " + vcf)
if xls and not os.path.isfile(xls):
    p.error("Invalid xls: " + xls)


commands = [
    "kill `pgrep -f continuously_reload_all_projects_daemon.sh`",
    "python3 -u manage.py add_project %(project_id)s '%(project_name)s' ",
    "python3 -u manage.py add_custom_population_to_project %(project_id)s gnomad-exomes2",
    "python3 -u manage.py add_custom_population_to_project %(project_id)s gnomad-genomes2",
    "python3 -u manage.py add_custom_population_to_project %(project_id)s topmed",
    "python3 -u manage.py convert_xls_to_ped --xls '%(xls)s' --ped '%(ped)s'",
    "python3 -u manage.py add_individuals_to_project %(project_id)s --ped '%(ped)s'",
    "python3 -u manage.py add_vcf_to_project %(project_id)s '%(vcf)s'",
]

if not opts.no_phenotips:
    commands += [
        "python3 -u manage.py add_project_to_phenotips %(project_id)s '%(project_name)s' ",
        "python3 -u manage.py add_individuals_to_phenotips %(project_id)s --ped %(ped)s ",
    ]

commands += [
    "python3 -u manage.py generate_pedigree_images %(project_id)s",
    "python3 -u manage.py add_default_tags %(project_id)s",
    "python3 -u manage.py load_project %(project_id)s" + (" --force-annotations --force-clean " if opts.force else ""),
    "python3 -u manage.py load_project_datastore %(project_id)s",
#    "nohup ./continuously_reload_all_projects_daemon.sh &> logs/continuously_load_all_projects_daemon.log &"
]

commands = map(lambda s: s % globals(), commands )

print(date.strftime(datetime.now(), "%m/%d/%Y %H:%M:%S  -- Will run: "))
for c in commands:
    print(c)
#i = input("Do you want to continue? [y/n] ")
if opts.run:
    for c in commands:
        print(date.strftime(datetime.now(), "%m/%d/%Y %H:%M:%S") + " -- Running: " + c)
        sys.stdout.flush()
        r = os.system(c)
        if "continuously_reload_all_projects_daemon.sh" not in c and r != 0:
            print(date.strftime(datetime.now(), "%m/%d/%Y %H:%M:%S") + " -- Command failed: " + c + "\nExiting.." )
            break

