# irelocate
The irelocate application replicates all data objects with one or
more replicas on a specified set of source resources to a 
configured destination resource.

# Dependancies
The Yoda custom microservice "msi_stat_vault" must exist on the
iRODS server. This service will be used to check if the data file
referenced by a replica is sufficiently compatible with the replica.

# Usage
Use "java -jar irelocate.jar -h" for help on the syntax.
  
The irelocate application expects a configuration file named
"irelocate.ini"  in the current directory.
This should be a text file with content lines formatted as: key=value

Keys that are not specified in the config file will be requested
interactively at application start. 

Example irelocate.ini:
```
host=irods
port=1247
zone=tempZone
username=rods
password=XXXREDACTEDXXX
auth_scheme=native
destinationResource=demoResc2
startDataId=0
sourceResources=demoResc,otherResc
```

 
