import sys
from swiftclient import client as swiftclient
from swiftclient import exceptions as swiftexceptions

'''
Authenticates with Swift using the parameters passed in and for each line in the infile (file object) deletes an object of that name from the container identified by cname.
Raises swiftclient.exceptions.ClientException in case the authentication parameters are wrong, or there is an error in deleting the object.
Raises ValueError if the passed in cname is not a valid container.
'''
def dodelete(os_auth, cname, infile):
    #Get a swift connection using the authentication params
    swift = swiftclient.Connection(**os_auth)
    try:
        headers, containers = swift.get_account()
        #get the list of containers so we can validate that the container requested by the user exists
        containernames = frozenset([c['name'] for c in containers])
            
    except swiftexceptions.ClientException as e:
        sys.stderr.write("Invalid parameters provided for swift authentication.\n")
        raise e

    #validate the passed in container name
    if cname not in containernames:
        err = "Swift storage does not have a container by name %s\n" % cname
        sys.stderr.write(err)
        raise ValueError(err)
        
    #Process each line from stdin and delete the object
    for line in infile:
        line = line.rstrip() #Remove the \n from the line
        swift.delete_object(cname, line)
        print "Deleted object %s" % line

