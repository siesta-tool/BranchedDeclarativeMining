# DeclareIncremental Copilot Instructions

## Project Overview
This is a Scala-based extension of the SIESTA framework for incremental mining of branched Declare constraints from process mining event logs. The system extends traditional constraint mining with AND/OR/XOR branching policies on both source and target events.

## Architecture & Core Components

### Main Entry Points
- `src/main/scala/auth/datalab/siesta/Main.scala` - Application entry point with argument parsing
- `run-app.sh` - Production wrapper script that loads `.env` and handles common argument patterns
- `run.sh` - Development script for sbt commands

### Core Mining Pipeline
1. **S3Connector** (`io/S3Connector.scala`) - Handles MinIO/S3 data persistence with table-based storage
2. **DeclareMiner** (`mining/DeclareMining.scala`) - Main mining orchestrator (1300+ lines)
3. **BranchingResolver** - Routes to specific branching implementations based on policy
4. **Policy-specific miners**: `AndBranchingMiner`, `OrBranchingMiner`, `XorBranchingMiner`

### Key Data Structures (`model/Structs.scala`)
- `Config` - Complete configuration with branching parameters and feature flags
- `BranchingPolicy` enum: `AND`, `OR`, `XOR` with smart string parsing
- `BranchingType` enum: `SOURCE`, `TARGET` (default: TARGET)
- `PairConstraint` - Basic constraint with traces
- `TargetBranchedPairConstraint` - Multi-target constraint result

## Branching Mining Algorithms

### Pattern: BitSet-based Trace Intersection
All branching miners use `java.util.BitSet` for efficient trace set operations:
```scala
// Convert traces to BitSet for fast intersections
val bits = new BitSet()
constraint.traces.flatMap(t => traceToInt.get(t)).foreach(bits.set)
inter.and(otherBits) // AND policy
inter.or(otherBits)  // OR policy  
inter.xor(otherBits) // XOR policy
```

### Drop Factor Monitoring
For unbounded mining (`maxTargets = Int.MaxValue`), optional `dropFactor` enables early stopping when support drops significantly:
```scala
val shouldStop = dropStats.shouldStop(currentDrop, dropFactor.get)
// Uses: threshold = avg_drop + (dropFactor * std_dev_of_drops)
```

### Source vs Target Branching
Use `swap` parameter for source branching - internally swaps source/target fields then post-processes results.

## Build & Development Workflow

### Environment Setup
1. Create `.env` file with S3/MinIO credentials:
   ```
   s3accessKeyAws=minioadmin
   s3secretKeyAws=minioadmin
   s3endPointLoc=http://localhost:9000
   ```

### Local Development Commands
```bash
# Compile only
sbt compile

# Run with preprocessing (via Docker)
docker compose up minio preprocess
./run-app.sh log_t5e5 -p XOR -t TARGET -b 3

# Direct sbt execution
./run.sh "run -l log_t5e5 --support 0.1 --branchingPolicy AND"
```

### Testing Infrastructure
- Tests in `src/test/scala/auth/`
- Evaluation framework in `evaluation/` with Docker-based benchmarking
- Incremental evaluation: `evaluation/incremental_evaluation/`

## Configuration Patterns

### Argument Processing
Uses `scopt` library in `utils/Utilities.scala`. Key patterns:
- Logname is always required (`-l`)
- Branching requires both policy (`-p`) AND type (`-t`) 
- Use `config.isBranchingEnabled` to check if branching should activate

### Output File Naming Convention
Files follow pattern: `constraints_{LOG_NAME}[_{COMPONENTS}].json`
- Support: `s025` (for 0.25)
- Branching: `ta3` (target AND bound 3), `so` (source OR unbounded)
- Filters: `fr` (filter rare), `fu` (filter under-bound)
- Modes: `mh` (hard rediscovery), `mq` (quick mining)

## Docker & S3 Integration

### Multi-Service Architecture
- **minio**: S3-compatible storage on port 9000
- **preprocess**: Log preprocessing service on port 8000
- **cbdeclare**: Main mining service

### Table Structure in S3
The S3Connector manages multiple tables per log:
- `{logname}_seq_table` - Sequence data
- `{logname}_detailed_table` - Detailed constraints
- `{logname}_meta_table` - Metadata with trace counts
- `{logname}_index_table` - Indexing information

## Common Pitfalls & Guidelines

- Always check `config.isBranchingEnabled` before applying branching logic
- Unbounded mining (`branchingBound = 0` or `Int.MaxValue`) can be expensive - consider `dropFactor`
- Source branching internally swaps fields - don't manually swap in client code
- S3 initialization must happen before any mining operations
- BitSet operations are in-place - clone before modifying: `bits.clone().asInstanceOf[BitSet]`

## Key Dependencies
- **Spark 3.5.6**: Distributed processing (not in cluster mode - uses `local[*]`)
- **Hadoop 3.3.4**: S3 connectivity with AWS SDK
- **scopt**: Command-line argument parsing
- **json4s**: JSON output formatting