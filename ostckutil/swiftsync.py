import os
import sys
import time
import threading
import hashlib
import mimetypes
import ast
from swiftclient import client as swiftclient
from swiftclient import exceptions as swiftexceptions

'''
Append an entry in a log file in the specific directory relating to the success or failure of a command. The command would have been applied on a file in that directory.
'''
def write_log(fp, command, oname, success, extrainfo = ''):
    fp.write(str(time.time()) + ',' + command + ',' + oname + ',' + str(success) + ',' + extrainfo + '\n')

'''
PUT the specified file (fpath) into a swift container with a given object name (oname) and a content type (mtype).
Log the success or failure of this action in a log file in the same directory.
'''
def put_to_swift(swift, container, oname, fpath, mtype, logfile):
    fp = None
    try:
        fp = open(fpath, 'r')
        #When we PUT the file, we only need to pass in the file pointer, the swift library will read and chunk the file upload as necessary
        swift.put_object(container, oname, contents=fp, content_type=mtype)

        write_log(logfile, 'upload', oname, True)
        print "Successfully put file %s in container %s with object name %s and content-type %s" % (fpath, container, oname, mtype if mtype is not None else "Unspecified")

    except Exception as e:
        write_log(logfile, 'upload', oname, False, str(e))
        print "Could not upload file %s into Swift. Exception %s" % (fpath, str(e))

    finally:
        if fp is not None:
            fp.close()

'''
Swiftsync maintains state of having processed a particular folder in the same folder in a file called .swiftsync.state
The file is written as a dictionary with values, so it can be reread back in another pass through the same directory.
'''
def load_ss_state(dirpath):
    try:
        #swift sync stores its state files in each directory in a file called .swiftsync.state
        with open(os.path.join(dirpath,'.swiftsync.state')) as f:
            return ast.literal_eval(f.read()) #The contents of the file are just a dict of values
    except IOError: #no state file saved previously
        return {}
    except SyntaxError: #state file is corrupt
        return {}

'''
Swiftsync maintains state of having processed a particular folder in the same folder in a file called .swiftsync.state
The file is written as a dictionary with values, so it can be reread back in another pass through the same directory.
'''
def save_ss_state(dirpath,state):
    #swift sync stores its state files in each directory in a file called .swiftsync.state
    with open(os.path.join(dirpath,'.swiftsync.state'), 'w') as f:
        f.write(str(state)) #The contents of the file are just a dict of values

'''
This callback is invoked when processing a particular directory for the 'upload' command.
This function iterates over all files in this directory and if the file has been modified since the last pass (maintained in .swiftsync.state)
the file will be uploaded (new/replace) into swift storage.
'''
def upload_cb(swiftclient, dpath, cname, pseudofolder, ignore_unk_ct):
    ss_state = load_ss_state(dpath)
    lastreadstart = ss_state['readstart'] if 'readstart' in ss_state else 0.0 #load the time we last made a pass
    thisreadstart = time.time()

    #print dpath, lastreadstart, cname, pseudofolder

    #write the log file as we process each file in the directory
    with open(os.path.join(dpath, '.swiftsync.log'), 'a') as logfile: #log file should be in append mode
        #process all files which dont start with a .
        for fname in [f for f in os.listdir(dpath) if os.path.isfile(os.path.join(dpath,f)) and not f.startswith('.')]:
            fpath = os.path.join(dpath, fname) #full path to the file

            #if the file has not been modified since the previous run of swiftsync in this directory, ignore it
            if not os.path.getmtime(fpath) > lastreadstart:
                continue

            #this is a modified file, lets upload it

            #we guess the content-type to use for the upload. If we cannot guess it and unless it is overridden, we dont put the file
            ct,ce = mimetypes.guess_type(fname)
            if ct is None and ignore_unk_ct is False:
                print "  Ignoring file %s since its content-type could not be guessed. Use -c|--ignore-unknown-content-type to override." % fpath
                continue

            #create the objectname for this file we are ready to put into swift
            oname = fname if pseudofolder is None else pseudofolder + "/" + fname

            #PUT the file into swift
            put_to_swift(swiftclient, cname, oname, fpath, ct, logfile)

    #Save the state back into the .swiftsync.state file for the next pass
    ss_state['readstart'] = thisreadstart
    save_ss_state(dpath, ss_state) #write the state back

'''
http://stackoverflow.com/questions/3431825/generating-a-md5-checksum-of-a-file
'''
def md5(fname):
    hash = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
        return hash.hexdigest()

'''
This callback is invoked when processing a particular directory for the 'audit' command.
This function iterates over all files in this directory and calculates their md5 sums.
It also gets the object names and md5s of all corresponding objects in swift storage.
It generates a line starting with + if the file exists in local but not in swift, - if the file exists in swift and not in local and a ! if they exist in both, but are different (md5 doesnt match).
The audit cannot be processed for the 'root' folder of a container, only for pseudo-folders (enhancement required).
'''
def audit_cb(swiftclient, dpath, cname, pseudofolder, ignore_unk_ct):
    #Unfortunately, this model doesn't work for objects that are not in a pseudofolder. That is because if no pseudofolder is specified in the call to get_container below
    #swift returns all object names, thus breaking the directory by directory approach (and also probably running into the limit of objects in the returned list)
    if pseudofolder is None:
        return

    #Keep a dict of object names to md5 from Swift
    swiftdict = {}
    headers, objects = swiftclient.get_container(cname, path=pseudofolder)
    for o in objects:
        swiftdict[o['name']] = o['hash']

    #Keep a dict of object names to md5 for local files
    localdict = {}
    for fname in [f for f in os.listdir(dpath) if os.path.isfile(os.path.join(dpath,f)) and not f.startswith('.')]:
        fpath = os.path.join(dpath, fname) #full path to the file

        #form the objectname for this file
        oname = fname if pseudofolder is None else pseudofolder + "/" + fname
        localdict[oname] = md5(fpath)
    
    #check which items are only in local and not in swift, mark them with a + (i.e. to be added)
    for l in localdict.keys():
        if l not in swiftdict:
            print "+ %s %s" % (l, localdict[l])
    
    #check which items are only in swift and not in local, mark them with a - (i.e. to be removed)
    #Also, if they are in both, compare their md5 sums, if they are different, mark them with a ! (like how some linux diffs do)
    for s in swiftdict.keys():
        if s in localdict:
            if localdict[s] != swiftdict[s]:
                print "! %s %s %s" % (s, swiftdict[s], localdict[s])
        else:
            print "- %s %s" % (s, swiftdict[s])

'''
Process serially using os.walk() from the root folder that has to be 'rsynced' on down.
The first node after the root folder should represent the container name and all directories below will map to pseudo folders in Swift.
The leaf nodes are the files which will be uploaded and mapped as objects in Swift.
Depending on whether the user had issued an upload/audit command, the callback on reaching a specific directory will be invoked to take the required action.
'''
def processroot(threadid, num_threads, base, swiftclient, containernames, ignore_unk_ct, cmd_cb):
    #Walk from the root on down
    for root, dirs, fnames in os.walk(base):
        for dname in dirs:
            dpath = os.path.join(root, dname) #full path to directory

            #We divvy up directories to process amongst threads so there is no contention. We do that by distributing all directory path names across an almost even hash space
            #The threads dont even need to synchronize!
            if abs(hash(dpath)) % num_threads != threadid:
                continue #This directory is not for this thread, for some other thread

            #extract the container name and the pseudofolder after the container
            s = dpath[len(base)+1:].split('/',1) #assumes the base did not have trailing /s, which we removed before calling this method
            cname = s[0]
            pseudofolder = s[1] if len(s) > 1 else None #no pseudofolder if in the 'container' directory

            #We cannot put objects when the container itself hasn't been created. TODO: add warning in log?
            if not cname in containernames:
                sys.stderr.write('Ignoring path %s since swift does not have a container of that name\n' % dpath)
                continue

            #Invoke the upload/audit callback to process this folder
            cmd_cb(swiftclient, dpath, cname, pseudofolder, ignore_unk_ct)

'''
A worker thread. Creates its own swift connection so it doesn't clash with those of other threads.
Invokes the processroot method to run the upload/audit as desired.
If this is to be run as a daemon, continues in an infinite loop taking a break in between based on passed in interval-secs.
TODO: Improve exception handling
'''
def worker(threadid, num_threads, os_auth, rootdir, ignore_unk_ct, daemon_interval, cmd_cb):
    #Get a swift connection using the authentication params
    swift = swiftclient.Connection(**os_auth)
    try:
        headers, containers = swift.get_account()
        #get the list of containers so we can validate that the container exists when we come across it in a local directory
        containernames = frozenset([c['name'] for c in containers])
            
    except swiftexceptions.ClientException as e:
        sys.stderr.write("Invalid parameters provided for swift authentication. \n")
        return

    #infinite loop in case this is a daemon
    while True:
        start_time = time.time() #figure out when we are starting this run

        #Start processing
        processroot(threadid, num_threads, rootdir, swift, containernames, ignore_unk_ct, cmd_cb)

        if daemon_interval == -1:
            break #we are not running as a daemon

        end_time = time.time()

        #Decide how much to sleep for before starting the next loop. We dont sleep if the previous run took longer than the specified interval time
        if (end_time - start_time) < daemon_interval:
            time.sleep(daemon_interval - (end_time-start_time)) #our previous run didnt take as long as the interval between runs, sleep for the remaining time

'''
Start the required number of threads to process the command (upload/audit). Invoke the worker() method in each thread to the real work.
'''
def start_workers(num_threads, os_auth, rootdir, ignore_unk_ct, daemon_interval, cmd_cb):
    #Create the requisite number of threads and start the worker processes
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i, num_threads, os_auth, rootdir, ignore_unk_ct, daemon_interval, cmd_cb))
        threads.append(t)
        t.start()
