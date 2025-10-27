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
      On macOS/Linux:
         source venv/bin/activate

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
STEP 5: VERIFY PROJECT STRUCTURE
-------------------------------------------------------------

Your folder layout should look like this:

Distribute-inventory-management-system-Raft-main
│
├── llm_server
│   ├── llm.proto
│   ├── llm_pb2.py
│   ├── llm_pb2_grpc.py
│   ├── main_llm_server.py
│   └── __init__.py
│
├── server
│   ├── app_server.py
│   ├── auth.proto
│   ├── inventory.proto
│   ├── auth_pb2.py
│   ├── auth_pb2_grpc.py
│   ├── inventory_pb2.py
│   ├── inventory_pb2_grpc.py
│   └── __init__.py
│
├── client
│   ├── client.py
│   └── __init__.py
│
└── requirements.txt


-------------------------------------------------------------
STEP 6: PROJECT DEMO VIDEO
-------------------------------------------------------------

After everything works, upload the working demo video and add the Google Drive link below.

Project Demo Video Link:
[PASTE YOUR GOOGLE DRIVE LINK HERE]


-------------------------------------------------------------
NOTES
-------------------------------------------------------------

- Always run commands from the project root folder.
- Make sure both servers are running before starting the client.
- If you modify any .proto file, regenerate the gRPC files again using the commands above.


Author: Ajay
Platform: Windows 11
Tech Stack: Python, gRPC, Raft Algorithm, Distributed Systems
