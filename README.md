# irelocate
The irelocate application can be used to relocate the replicas of data objects to a new resource.
It operates in either one of two modes: 'replicate' or 'trim'.

Replicate is the default mode of operation. irelocate replicates all data objects with one or
more replicas on a specified set of source resources to a 
configured destination resource, unless a 'good' replica already exists on the destination resource.
If the option "-nearby" is specified, then a 'good' replica on a resource located on the same host
as the destination resource suffices.

Trim mode is activated by the option "-trim". irelocate will trim all replicas located on 
any of the source resources, IF AND ONLY IF the destination resource contains a 'good' replica.
If the option "-nearby" is specified, then a 'good' replica on a resource located on the same host
as the destination resource will suffice.

irelocate considers a replica 'good' if it has the iRODS replica status 'GOOD' and in addition the
data file referenced by the replica exists and the data file's size matches the size registered with the replica.

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

 
