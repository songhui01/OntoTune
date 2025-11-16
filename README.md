# OntoTune
OntoTune for query optimization is a prototype platform for applying ontologized concepts and relationships in the query optimization study through the query hints selection. For more information about OntoTune, refer to the paper (https://arxiv.org/pdf/2511.06780). The platform is built based on the work of Bao's workflow(https://rm.cab/bao), and we added the artifacts of extracting the metadata info from the PG extension that reflects the ontology relationships of the query, GUC, and the database statistics. 

The purpose of this platform is to provide the basic structures for ontology information, and thus promotes the explanability and more in the next phase. As part of work in progress, we are also exploring the use of ontology-based embedded features for machine learning to better serve the purpose of query optimization, as we consider this to be a worthwhile exploration direction.

A tutorial to run this platform is available as follows.

## Install PG12 if it is not available in the environment.
* An so_pg PostgreSQL archive and related scripts of so_queries can be downloaded from "https://rmarcus.info/stack.html"

## Make the Python virtual environment:
* sudo apt update
* sudo apt-get install -y python3-venv
* python3 -m venv ontoenv
* source ontoenv/bin/activate
* pip install --upgrade pip
* pip install numpy scipy pandas scikit-learn psycopg2-binary torch torch_geometric

## Start the server:
Change directory to onto_server
### Directly start the server:
* python3 main.py
### Clearing up everything and killing the possible processes:
* ./restart_onto.sh clean path="a_log_file.txt"

## Install PG_ONTO as an extension:
In the pg_extension_onto folder:
* make clean
* sudo make PG_CONFIG=/usr/lib/postgresql/12/bin/pg_config install

## Edit the configuration file:
* sudo nano /etc/postgresql/12/main/postgresql.conf
* adjust the parameters
* append "shared_preload_libraries = 'pg_onto'" under "CUSTOMIZED OPTIONS"
* Restart the service: sudo systemctl restart postgresql

## Run sample queries:
* run_queries_onto.py so_queries/*/*.sql | tee ~/run_query_sampling_001.txt

# Acknowledgement
* This software is available under the AGPLv3 license. 
* The development of some helper functions in this code base, including onto_meta.h, cnn_net_delta.py, etc., along with other utilities, was fast-prototyped using AI tools (mainly ChatGPT) by prompting with the design details, and then examined and tested by the authors. ChatGPT is also asked to generate comments for some files.

# From Bao, thus similar restrictions apply to this platform
Bao is available under the AGPLv3 license. 

While this repository contains working prototype implementations of many of the pieces required to build a production-ready learned query optimizer, this code itself should not be used in production in its current form. Notable limitations include:

* The reward function is currently restricted to being a user-supplied value or the query latency in wall time. Thus, results may be inconsistent with high degrees of parallelism.
* The Bao server component does not perform any level of authentication or encryption. Do not run it on a machine directly accessible from an untrusted network.
* The code has not been audited for security issues. Since the PostgreSQL integration is written using the C hooks system, there are almost certainly issues.