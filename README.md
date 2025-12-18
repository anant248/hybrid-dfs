# MP4-G76

## Name
Stream Processing System for CS 425 Group 76 Machine Problem 4

Developed by Anant Goyal (anantg2@illinois.edu) and Abuzar Hussain Mohammad (ahm7@illinois.edu)

## Description

MP4-G76 extends our MP3 HyDFS distributed file system with a distributed stream-processing engine called Rainstorm. The system supports multi-stage stream processing pipelines, fault tolerance through logging and replay, autoscaling, and integration with HyDFS for durable final outputs. Input data is streamed from HyDFS through Stage 0 tasks, processed through user-defined operators, and final outputs are appended to an output file stored on VM1.

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/anant248/hybrid-dfs.git
   cd hybrid-dfs
   ```
2. **Build the project:**
   - Ensure you have Java 11+ and Maven installed.
   - Run:
     ```bash
     mvn clean package
     ```
3. **Prepare directories:**
      Each VM should contain:
      ```bash
      mkdir -p hdfs hydfs inputs output logs
      ```
   - `hdfs/` — task logs for worker recovery  
   - `hydfs/` — replicated HyDFS storage  
   - `inputs/` — your CSV datasets  
   - `output/` — local temp output  
   - `logs/` — node logs

## Usage

1. **Start a node:**
   - Run the main class for your node, specifying configuration as needed.
   - Example:
     ```bash
     java -jar target/hybrid-dfs.jar --nodeId=<NODE_ID> --config=config.yaml
     ```
   - In case that doesnt work (replace XX with the current IP and introducer node IP respectively):
     ```bash
     mvn exec:java -Dexec.mainClass="com.uiuc.systems.Main" -Dexec.args="fa25-cs425-76XX.cs.illinois.edu fa25-cs425-76XX.cs.illinois.edu gossip nosuspect"
     ```

   Once running, each node participates in:
   - HyDFS membership & replication  
   - Rainstorm leader/worker communication  
   - Failure detection  

   ---

   ## Running the Rainstorm Stream Processor (on the Leader VM)

   Rainstorm jobs are started **from VM1**, which automatically becomes the leader.

   **Syntax:**
   ```
   rainstorm <Nstages> <Ntasks_per_stage> \
   <op1_exe> <op1_arg> … <opN_exe> <opN_arg> \
   <hydfs_src_directory> <hydfs_dest_filename> \
   <exactly_once> <autoscale_enabled> \
   <INPUT_RATE> <LW> <HW>
   ```

   - `op_exe` ∈ {identity, filter, transform, aggregate}
   - `op_arg` = operator-specific argument (use quotes for multi‑word patterns)
   - Output file is created on **VM1 under `/hydfs`**
   - Each task writes its recovery log under **`/hdfs` on its own VM**

   **Examples:**

   ```
   rainstorm 1 1 identity "i" dataset1.csv output.txt true false 100 100 100
   ```

   ```
   rainstorm 1 1 filter STONEOAK dataset1.csv output.txt true false 100 100 100
   ```

   ```
   rainstorm 2 1 filter "STONE" transform "t" dataset1.csv output.txt true false 100 100 100
   ```


## Example Workflow

1. **Start all 10 VMs** using the Main command.
2. **Place input data** (e.g., `dataset1.csv`) into:
   ```
   inputs/
   ```
3. **Start a Rainstorm pipeline** on VM1:
   ```bash
   rainstorm 2 1 filter "ERROR CODE" aggregate "3" dataset1.csv output.txt true false 200 100 300
   ```
4. **Workers start automatically**, each receiving tasks for their assigned stage.
5. **Final output** appears in:
   ```
   /hydfs/output.txt
   ```
6. **Task logs** (for replay after failure) appear on each VM under:
   ```
   /hdfs/rainstrom_task_<id>.log
   ```

## Testing (NOT APPLICABLE FOR MP4)

1. **Unit tests:** Run with Maven:
   ```bash
   mvn test
   ```
2. **Integration tests:** Simulate node failures, file operations, and membership changes by running multiple instances and observing system behavior.
3. **Manual testing:** Use the CLI to create, append, and get files, and verify data consistency across nodes.

## Roadmap

- [x] Read and understand MP4
- [x] Implement MP4
- [x] Demo MP4

## Project Status

**Completed** — MP4-G76 is now complete as part of the ECE 428 coursework. Bug reports and suggestions are welcome.
