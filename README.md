# About

This repository contains nlmatics code for all the api services provided by the discovery engine. This code is the backend of nlmatics app. It offers all the services needed by the app including file upload, file viewing, search, creation/saving/auditing of extracted fields, workspace creation, workspace access managedment, training data creation and much more. 

## Installation instructions

### Run the python server
First create a python venv and activate it. Code is tested with Python 10. Higher versions may not work. 

Install requirements
```
pip install -r requirements.txt
```
Run the server after setting the environment variables shown in section Dependencies
```
python -m server
```


### Run the docker file (available soon)
A docker image is available via public github container registry. 

Pull the docker image
```
docker pull ghcr.io/nlmatics/nlm-services:latest
```
Run the docker image mapping the port 5001 to port of your choice. 
```
docker run -p 5011:5001 ghcr.io/nlmatics/nlm-services:latest
```

## Dependencies 
This code uses a running elastic search, mongodb instance, rabbitmq (optional, recommended for production) and a file storage which could be local file storage or a cloud storaze such as Azure storage and GCP buckets. This also needs a running nlm-model-service backend. Here are the environment variables needed to run this code locally:

```
#import source code for local development
#you need to have cloned these 3 repos to work simultaneously on all the repos
export PYTHONPATH=/Users/ambikasukla/projects/nlm-utils:$PYTHONPATH
export PYTHONPATH=/Users/ambikasukla/projects/nlm-ingestor:$PYTHONPATH
export PYTHONPATH=/Users/ambikasukla/projects/nlm-discovery-engine:$PYTHONPATH

#setup connectivity to local or file storage
export PLATFORM=local
export INGESTOR_FILE_STORAGE=local
export OBJECT_STORE=local_filesystem
export OBJECTSTORE_DIR=/Users/ambikasukla/data/nlm-local
export INDEX_BUCKET_NAME=/Users/ambikasukla/data/doc-store
export LOCAL_ROOT_DIR=/Users/ambikasukla/data/doc-store
export STORAGE_NAME=/Users/ambikasukla/data/doc-store

#setup connectivity to mongodb database -- for local
export MONGO_HOST=localhost:27017

#setup connectivity to elasticsearch -- for local
export ES_URL=http://localhost:9200

#setup pubsub for ingestor
#setup authentication
#AUTH_PROVIDER options are auth0, fakeauth (for no auth system), azuread and saml
export AUTH_PROVIDER=fakeauth
export DEFAULT_USER=ambika@nlmatics.com
export BACKEND_URL=http://localhost:5001
export DEFAULT_USER_ACCESS_TYPE=EDITOR

#setup link to model server
export MODEL_SERVER_URL=http://ec2-54-162-174-7.compute-1.amazonaws.com:5001
```

## Code Structure
All the API endpoints use Swagger. Look at swagger.yaml to locate a service, then go to the controller of the service to see how an API call is implemented.

## Credits

This code was developed at Nlmatics Corp. from 2020-2023.

Suhail Kandanur wrote the inital framework and code. This code also has further contributions from Kiran Panicker, Yi Zhang, Ambika Sukla, Reshav Abraham, Tom Liu, Sonia Joseph, Jasmin Omanovic and Shivani Jha. 