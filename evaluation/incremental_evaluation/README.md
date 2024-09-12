### Incremental Declare Mining
This experiment goal is to access how good the incremental process works.
It will run in repeat preprocessing/declare mining in log files that have been 
created by executed the multiple_datasets_realistic.py inside the create_datasets.

#### To execute
Run from the root directory
```bash
docker run -t incremental_declare -f evaluation/incremental_evaluation/Dockerfile .
```
+Once the process is completed,
+```bash
+docker run -d -v /home/mavroudopoulos/siesta/DeclareIncremental/evaluation/create_datasets/output/:/app/output --network siesta-net -p4040:4040 incremental_declare
+```
