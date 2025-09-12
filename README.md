# Comprehensive Branched Declare Constraints Mining extension module  

## Overview

This branch contains an extension module for the SIESTA framework, designed to extend the incremental mining procedure of Declare constraints. The module introduces support for branching mechanisms on both source and target events within the constraint discovery process.

## Purpose

The primary objective of this module is to enhance the standard Declare constraints mining by enabling branched relationships:
- Source Branching: Allowing multiple source events to jointly activate a constraint.
- Target Branching: Allowing multiple target events to be linked under a single activation event.

This is achieved by building on top of SIESTA’s original incremental mining strategy, maintaining scalability while supporting more expressive process models.

## Features

- Implementation of AND, OR, and XOR branching policies for both sources and targets.
- Compatible with batch event log processing.
- Optimized to maintain the efficiency of the incremental mining procedure, suitable for big data scenarios.

## Run with Docker Compose (v2+)
1. From `docker-compose.yml`, run the following S3 service:
```bash
  docker compose up minio
```
2. Preprocess a new log file (if not already done) saved locally in `input/`:
```bash
  docker compose up preprocess
  curl -X 'POST' 'http://localhost:8000/preprocess' \
  -H 'Content-Type: application/json' \
  -d '{
    "spark_master": "local[*]",
    "file": "test.xes",
    "logname": "test"
  }'
```
3. Configure the CBDeclare service by modifying it in the `docker-compose.yml` file:
   - Replace `test` in the `command` section with the logname of your log file.
   - Optionally, add any arguments in the `command` section as needed separated by commas. See the next section for all available arguments.
   - If your RAM is less than 10g, consider changing the `entrypoint` driver memory configuration as well.
   
   E.g. `command:["-l", "test", "--support", "0.5", "-p", "AND"]`
5. Run the CBDeclare service:
```bash
  docker compose up cbdeclare
```

_Note: if network issues arise, consider uncommenting the `external: true` configuration in the yml file._


## Arguments
### Required Argument

- `-l, --logname <logname>`  
  **Type**: `String`  
  **Required**: Yes  
  **Description**: Name of the log file. This refers to the corresponding log stored in S3.

### Optional Arguments

- `-s, --support <value>`  
  **Type**: `Double`  
  **Default**: `0`  
  **Description**: Minimum support threshold for constraint discovery. Only constraints with support greater than this value will be considered. If set to `0`, all constraints are considered.

- `-p, --branchingPolicy <AND|OR|XOR>`  
  **Type**: `String`  
  **Default**: `null`  
  **Description**: Branching policy to apply during constraint discovery.

- `-t, --branchingType <SOURCE|TARGET>` 
  **Type**: `String`  
  **Default**: `"TARGET"`  
  **Description**: Specifies the direction of the branching. Use `TARGET` to group multiple targets under a single source.

- `-b, --branchingBound <value>`  
  **Type**: `Int`  
  **Default**: `0`  
  **Description**: Maximum number of activities allowed in a branched group. If `0`, no branching bound is enforced.

- `-d, --dropFactor <value>`  
  **Type**: `Double`  
  **Default**: `1.5`  
  **Description**: Drop factor used to reduce candidate branching sets. A lower value increases the likelihood of early stopping during greedy expansion.

- `-r, --filterRare <true|false>`  
  **Type**: `Boolean`  
  **Default**: `false`  
  **Description**: If set to `true`, filters out events that are considered rare (e.g., based on frequency thresholds).

- `-u, --filterUnderBound <true|false>`  
  **Type**: `Boolean`  
  **Default**: `false`  
  **Description**: If enabled, removes templates that do not satisfy the required branching bound.

- `-h, --hardRediscover <true|false>`  
  **Type**: `Boolean`  
  **Default**: `false`  
  **Description**: Forces a full rediscovery from scratch, ignoring any previously cached intermediate results.

## Output File Naming

Output files use a concise naming convention: `constraints_{LOG_NAME}[_{COMPONENT}]*.json`

**Components** (only included when non-default):
- `s{VALUE}` - Support threshold (e.g., `s025` for 0.25)
- `{TYPE}{POLICY}{BOUND}` - Branching (e.g., `ta5` for target AND bound 5, `so` for source OR)
- `d{VALUE}` - Drop factor (e.g., `d25` for 2.5) 
- `f{LETTERS}` - Filters (`r`=rare, `u`=underbound)
- `m{LETTERS}` - Modes (`q`=quick, `h`=hard rediscovery)

**Branching notation:** `{TYPE}{POLICY}` where:
- **Type**: `t`=target, `s`=source
- **Policy**: `a`=AND, `o`=OR, `x`=XOR

**Examples:**
- `constraints_mylog.json` (default configuration)
- `constraints_mylog_s025_ta3_fr.json` (25% support, target AND branching bound 3, filter rare)
- `constraints_mylog_so_mq.json` (source OR branching unbounded, quick mining)

---
