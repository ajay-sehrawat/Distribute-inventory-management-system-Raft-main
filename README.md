Team Members -
1. Alankrit Sinha - 2025H1120165P
2. Ajay Sehrawat  - 2025H1120166P
3. Diti Nirmal    - 2025H1120149P

College - Bits Pilani, Pilani Campus

Platform: Windows 11
Tech Stack: Python, gRPC, Raft Algorithm, Distributed Systems

Distributed Inventory Management System (using Raft and gRPC)
-------------------------------------------------------------

This project is a Distributed Inventory Management System built using Python, gRPC, 
and the Raft consensus algorithm. It includes separate modules for:
 - llm_server → Handles LLM-related responses
 - server → Main inventory and authentication logic
 - client → For interacting with the distributed system


-------------------------------------------------------------
STEP 1: SETUP THE ENVIRONMENT
-------------------------------------------------------------

1. Open a terminal in the main project folder.

2. Create a virtual environment:
      python -m venv venv

3. Activate the virtual environment:
      On Windows:
         venv\Scripts\activate

4. Install required dependencies:
      pip install -r requirements.txt


-------------------------------------------------------------
STEP 2: DELETE OLD GRPC GENERATED FILES
-------------------------------------------------------------

Before generating new gRPC files, delete the existing ones to avoid import issues.

A) For llm_server, delete:
      llm_server\llm_pb2.py
      llm_server\llm_pb2_grpc.py

B) For server, delete:
      server\auth_pb2.py
      server\auth_pb2_grpc.py
      server\inventory_pb2.py
      server\inventory_pb2_grpc.py


-------------------------------------------------------------
STEP 3: REGENERATE GRPC FILES FROM PROTO DEFINITIONS
-------------------------------------------------------------

Make sure you are inside the main project directory:
   Distribute-inventory-management-system-Raft-main

Then run these commands one by one:

A) Generate files for llm_server:
      python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. llm_server/llm.proto

B) Generate files for server:
      python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. server/auth.proto
      python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. server/inventory.proto

If you get an error like "auth.proto not found", try this alternative:
      python -m grpc_tools.protoc -Iserver --python_out=. --grpc_python_out=. server/inventory.proto


-------------------------------------------------------------
STEP 4: RUN ALL MODULES
-------------------------------------------------------------

Make sure your virtual environment is active before running.

1. Run the LLM Server:
      python -m llm_server.main_llm_server
   Wait for the message that confirms the LLM server started successfully.

2. Run the Main Server:
      Open a new terminal, activate the virtual environment again, then run:
      python -m server.app_server
   You should see a message like "Server started on port 50051".

3. Run the Client:
      Open another terminal and run:
      python -m client.client
   The client will connect to the system and perform operations.


-------------------------------------------------------------
STEP 6: PROJECT DEMO VIDEO
-------------------------------------------------------------

You can watch the full working demo of this project here:

   >>> Project Demo Video Link:
   https://drive.google.com/file/d/1WeDHV8RpUv0N-Ieixq8kvaRjTjRArNRN/view?usp=sharing <<<

   (Open the above link in your browser to view the complete demo video.)



MILESTONE 2:
DISTRIBUTED INVENTORY MANAGEMENT SYSTEM (MILESTONE 2)

Project Overview
Milestone 2 extends the earlier single-server system into a fully replicated 3-node Raft cluster with leader election, log replication, and consistent inventory updates across all nodes, LLM server recommendations.

The system contains four main components:
llm_server – Handles LLM-based recommendations for inventory
server – Authentication and inventory logic combined with Raft
raft – Core Raft implementation which consists of leader election, log replication
client – Command-line client to interact with the distributed service

-------------------------------------------------------------
STEP 1: Setup all terminals (Milestone 2) 
-------------------------------------------------------------

A working Milestone 2 setup requires running four components:

LLM Server
Raft Node 1
Raft Node 2
Raft Node 3
Client

Important: Ensure the virtual environment is activated in every terminal before running any command.

-------------------------------------------------------------
STEP 2: Run LLM Server
-------------------------------------------------------------

Command:
python -m llm_server.main_llm_server
Wait for the message that the LLM server is running.

-------------------------------------------------------------
STEP 3: Run 3 RAFT Nodes in 3 Separate Terminals
-------------------------------------------------------------

Terminal 1->
python raft/run_node.py --id 1 --port 51001 --peers 2:127.0.0.1:51002 3:127.0.0.1:51003

Terminal 2->
python raft/run_node.py --id 2 --port 51002 --peers 1:127.0.0.1:51001 3:127.0.0.1:51003

Terminal 3->
python raft/run_node.py --id 3 --port 51003 --peers 1:127.0.0.1:51001 2:127.0.0.1:51002

The nodes will start leader election. You will see output such as->
Node 2 became LEADER (Term X).
This means leader election is working successfully.

-------------------------------------------------------------
STEP 4: Run the Client
-------------------------------------------------------------

Open a new terminal and run->
python -m client.client

Features available through the client->

User login (customer or manager)
View inventory
Place orders
Add stock (manager only)
Ask the LLM for inventory advice
Automatic updates reflected across all Raft nodes

FEATURES IMPLEMENTED IN MILESTONE 2 ->

Fully functional Raft cluster (3 nodes)
Leader election and automatic failover
Log replication: every write operation is stored and committed consistently
Followers forward write requests to the leader automatically
Inventory updates are consistently applied across all nodes
LLM integration for intelligent stock suggestions
The Stock gets updated as users purchase

-------------------------------------------------------------
STEP 5: PROJECT DEMO VIDEO
-------------------------------------------------------------
Milestone 2 Google Drive link below:

Project Demo Video Link: https://drive.google.com/file/d/16loXREwXEuz5sXUETxJOkOztjH_OQ3zH/view?usp=sharing
