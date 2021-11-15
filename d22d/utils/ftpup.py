import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sys import platform
import json
import ntpath
import unicodedata
import threading
import os
import logging
from ftplib import FTP
from ftplib import FTP_TLS
from threading import Lock
from shutil import copy2
from datetime import datetime, timedelta

# --- constant connection values
ftpServerName = ""
ftpU = ""
ftpP = ""
directoriesToWatch = []
useTLS = True
dictOfWatchedDir = {}
synchAtStartupBool = False
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
lock = Lock()
workingDir = "/"
startBackupTime = "02:00"
backupDurationInHours = 1
delayToSend = -1
listOfFilesToBeSent = []
respectBackupHours = False


def sendDataCheck():
    # we want to avoid sending data within an interval that goes among <backupDurationInHours> hour before the backup time and <backupDurationInHours> hour after the backup time
    try:
        if (respectBackupHours):
            if (startBackupTime!="" and (":" in startBackupTime)):
                listOfParam = startBackupTime.split(":")
                backupHour = listOfParam[0]
                backupMinute = listOfParam[1]

                minorCheck = datetime.today() - timedelta(hours=backupDurationInHours, minutes=0)
                majorCheck = datetime.today() + timedelta(hours=backupDurationInHours, minutes=0)

                date = datetime.now()
                dateAndTimeOfBackup = date.replace(hour=int(backupHour), minute=int(backupMinute))

                if (dateAndTimeOfBackup > minorCheck and dateAndTimeOfBackup < majorCheck):
                    return False
                else:
                    return True
            else:
                #if anything goes wrong we return true
                return True
        else:
            return True

    except Exception as inst:
        #if anything goes wrong we return true
        return True

class Watcher(threading.Thread):
    # DIRECTORY_TO_WATCH = "/home/teo/Desktop/temp"

    def __init__(self, localDirPath):
        try:
            threading.Thread.__init__(self)
            self.observer = Observer()
            self.localDirPath = localDirPath
        except Exception as inst:
            logger.error("__init__ in Watcher Error for " + self.localDirPath + " - " + timeStamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly

    def run(self):
        try:
            event_handler = Handler()
            self.observer.schedule(event_handler, self.localDirPath, recursive=True)
            self.observer.start()
            logger.debug("The thread to monitor " + self.localDirPath + " was run")

            try:
                while True:
                    time.sleep(5)
            except:
                self.observer.stop()
                logger.error("An exception occured in the thread loop of " + self.localDirPath)
                # we want a service that is always run
                w = Watcher(self.localDirPath)
                logger.info("Restart the thread to observe changes in files in" + self.localDirPath)
                w.start()

            logger.info("Join the thread for " + self.localDirPath)
            self.observer.join()

        except Exception as inst:
            logger.error("run in Watcher Error for " + self.localDirPath + " - " + timeStamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        result = None
        try:
            if event.is_directory:
                # do nothing
                result = None
            elif event.event_type == 'created':
                # Take any action here when a file is first created.
                # when the file is create the "modified" event is also generated
                # so we do not anything
                logger.info("Received created event - %s." % event.src_path)
                # if (event.src_path in dictOfWatchedDir):
                #    moveFTPFiles(ftpServerName, ftpU, ftpP, event.src_path, dictOfWatchedDir[event.src_path], useTLS)

            elif event.event_type == 'modified':
                fileToUpload = event.src_path
                # Taken any action here when a file is modified.
                logger.info("Received modified event - %s." % fileToUpload)

                path_without_filename = path_without_leaf(fileToUpload)
                if (path_without_filename in dictOfWatchedDir):
                    if (delayToSend<=0 and sendDataCheck()):
                        # directly send the file without any delay
                        moveFTPFiles(ftpServerName, ftpU, ftpP, fileToUpload, dictOfWatchedDir[path_without_filename], useTLS)
                    else:
                        # insert the name of the file in a list that will be elaborated only once every delayToSend seconds
                        if (fileToUpload not in listOfFilesToBeSent):
                            listOfFilesToBeSent.append(fileToUpload)
            elif event.event_type == 'moved':
                # on linux, when a file is modified, the system create a temporal file (.goutputstream...)
                #  and them move it on the right one
                fileToUpload = event.dest_path

                # Taken any action here when a file is modified.
                logger.info("Received moved event - %s. - " % fileToUpload)

                path_without_filename = path_without_leaf(fileToUpload)
                if (path_without_filename in dictOfWatchedDir):
                    if (delayToSend<=0 and sendDataCheck()):
                        # directly send the file without any delay
                        moveFTPFiles(ftpServerName, ftpU, ftpP, fileToUpload, dictOfWatchedDir[path_without_filename], useTLS)
                    else:
                        # insert the name of the file in a list that will be elaborated only once every delayToSend seconds
                        if (fileToUpload not in listOfFilesToBeSent):
                            listOfFilesToBeSent.append(fileToUpload)
                # path_without_filename = path_without_leaf(fileToUpload)
                # if (path_without_filename in dictOfWatchedDir):
                #     moveFTPFiles(ftpServerName, ftpU, ftpP, fileToUpload, dictOfWatchedDir[path_without_filename], useTLS)
            elif event.event_type == 'deleted':
                # do nothing
                result = None
        except Exception as inst:
            logger.error("on_any_event Error - " + timeStamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly


def path_without_leaf(path):
    result = ""
    try:
        head, tail = ntpath.split(path)
        result = head
    except Exception as inst:
        logger.error("path_without_leaf Error - " + timeStamp())
        logger.error(type(inst))  # the exception instance
        logger.error(inst.args)  # arguments stored in .args
        logger.error(inst)  # __str__ allows args to be printed directly
        result = ""
    return result

#return the name of the file + extension
def path_leaf(path):
    result = ""
    try:
        head, tail = ntpath.split(path)
        result = tail or ntpath.basename(head)
    except Exception as inst:
        logger.error("Path_leaf Error - " + timeStamp())
        logger.error(type(inst))  # the exception instance
        logger.error(inst.args)  # arguments stored in .args
        logger.error(inst)  # __str__ allows args to be printed directly
        result = ""
    return result


def moveFTPFiles(serverName, userName, passWord, fileToUpload, remoteDirectoryPath, useTLS=False):
    lock.acquire(True)
    """Connect to an FTP server and bring down files to a local directory"""
    if (serverName != '' and userName != '' and passWord != ''):
        try:
            ftp = None
            if useTLS:
                ftp = FTP_TLS(serverName, timeout=120)
                if (ftp == None):
                    logger.info("LOG moveFTPFiles 3TLS ftp null")
            else:
                ftp = FTP(serverName)
            ftp.login(userName, passWord)
            if useTLS:
                ftp.prot_p()
            ftp.cwd(remoteDirectoryPath)
            ftp.set_pasv(True)
            filesMoved = 0

            try:

                logger.info("Connecting...")

                # create a full local filepath
                localFileName = path_leaf(fileToUpload)
                if localFileName != "" and not localFileName.startswith("."):

                    #create a copy of the file before sending it
                    tempFile = create_temporary_copy(fileToUpload, workingDir, localFileName)

                    # open a the local file
                    fileObj = open(tempFile, 'rb')
                    # Download the file a chunk at a time using RETR
                    ftp.storbinary('STOR ' + localFileName, fileObj)
                    # Close the file
                    fileObj.close()
                    filesMoved += 1
                    # remove the temp file
                    # to remove it I need to have the right permissions
                    os.chmod(tempFile, 777)
                    os.remove(tempFile)

                logger.info("Uploaded file: " + str(fileToUpload) + " on " + timeStamp())
            except Exception as inst:

                logger.error(type(inst))  # the exception instance
                logger.error(inst.args)  # arguments stored in .args
                logger.error(inst)  # __str__ allows args to be printed directly
                logger.error("Connection Error - " + timeStamp())
            ftp.quit()  # Close FTP connection
            ftp = None
        except Exception as inst:
            logger.error("Couldn't find server")
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly
    else:
        logger.error(
            "Connection was not possible because one of the following var was not set in the configuration file: ftpServerName , ftpU or ftpP")

    lock.release()


def create_temporary_copy(path, temp_dir, fileName):
    if platform == "win32":
        fileToCheck = temp_dir + '\\' + fileName
    else:
        fileToCheck = temp_dir + '/' + fileName
    # if the file already exists, we will delete it
    exists = os.path.isfile(fileToCheck)
    if exists:
        os.chmod(fileToCheck, 777)
        os.remove(fileToCheck)
    temp_path = os.path.join(temp_dir, fileName)
    copy2(path, temp_path)
    return temp_path


def timeStamp():
    """returns a formatted current time/date"""
    import time
    return str(time.strftime("%a %d %b %Y %I:%M:%S %p"))


def synchAtStartup():
    try:
        if (isinstance(directoriesToWatch, list) and len(directoriesToWatch) > 0):
            for dest in directoriesToWatch:
                if ('localDirPath' in dest and 'remoteDirPath' in dest):
                    logger.info("synchAtStartup: synchronizing " + dest['localDirPath'])
                    arr = os.listdir(dest['localDirPath'])
                    for file in arr:
                        if os.path.isdir(file) != True:
                            moveFTPFiles(ftpServerName, ftpU, ftpP, dest['localDirPath'] + "/" + file,
                                         dest['remoteDirPath'], useTLS)
    except Exception as inst:
        logger.error("synchAtStartup Error - " + timeStamp())
        logger.error(type(inst))  # the exception instance
        logger.error(inst.args)  # arguments stored in .args
        logger.error(inst)  # __str__ allows args to be printed directly

def elaborateAllChangedFiles():
    global listOfFilesToBeSent
    if (sendDataCheck()):
        while listOfFilesToBeSent:
            fileToUpload = listOfFilesToBeSent[0]
            ##fileToUpload = listOfFilesToBeSent.pop(0)
            path_without_filename = path_without_leaf(fileToUpload)
            if (path_without_filename in dictOfWatchedDir):
                moveFTPFiles(ftpServerName, ftpU, ftpP, fileToUpload, dictOfWatchedDir[path_without_filename], useTLS)
                #when the system has finished to upload the file, we remove it from the list
                listOfFilesToBeSent.remove(listOfFilesToBeSent[0])
            #listOfFilesToBeSent.pop(-1)
    else:
        logger.info("Not sending data because there could be a running update")
    #re-instantiate the thread that should send data
    threading.Timer(delayToSend, elaborateAllChangedFiles).start()


if __name__ == '__main__':
    conf = {
      "configuration":{
        "directoriesToWatch":[
          {
            "localDirPath": "/home/user/temp",
            "remoteDirPath": "temp1"
          },
          {
            "localDirPath": "/home/user/temp2",
            "remoteDirPath": "temp2"
          }
        ],
        "synchAtStartup":True,
        "ftpServerName":"server.domain.com",
        "ftpUser":"ftp-user",
        "ftpPass":"ftpPass",
        "useTLS":True,
        "workingDir": "/home/ftpWorkingDir",
        "delayToSend": 20,
        "startBackupTime": "10:12",
        "backupDurationInHours": 1,
        "respectBackupHours": True
        }
    }
    try:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # create a file handler
        handler = logging.FileHandler('log.log')
        handler.setLevel(logging.DEBUG)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)

        logger.info("loading configuration set")
        # read configuration parameters from the config.json file
        conf_path ='config.json'
        if os.path.exists(conf_path):
            with open(conf_path) as json_data_file:
                data = json.load(json_data_file)
        if 'configuration' in data:
            conf = data['configuration']
        if 'directoriesToWatch' in conf:
            directoriesToWatch = conf['directoriesToWatch']
        if 'ftpServerName' in conf:
            ftpServerName = conf['ftpServerName']
        if 'ftpUser' in conf:
            ftpU = conf['ftpUser']
        if 'ftpPass' in conf:
            ftpP = conf['ftpPass']
        if 'useTLS' in conf:
            useTLS = conf['useTLS']
        if 'synchAtStartup' in conf:
            synchAtStartupBool = conf['synchAtStartup']
        if 'workingDir' in conf:
            workingDir = conf['workingDir']
        if 'startBackupTime' in conf:
            startBackupTime = conf['startBackupTime']
        if 'backupDurationInHours' in conf:
            backupDurationInHours = conf['backupDurationInHours']
        if 'respectBackupHours' in conf:
            respectBackupHours = conf['respectBackupHours']
        if (not respectBackupHours):
            logger.info("The script will NOT respect backup hours (so it will upload file also during backup hours)")

        #check if the folder exists, otherwise, create it
        try:
            if not os.path.exists(workingDir):
                os.makedirs(workingDir)
        except Exception as inst:
            if platform == "win32":
                workingDir = '/'
            else:
                workingDir = 'C:\\'
        try:
            if 'delayToSend' in conf:
                if (isinstance(conf['delayToSend'], int)):
                    delayToSend = conf['delayToSend']
                else:
                    delayToSend = int(conf['delayToSend'],10)
        except Exception as inst:
            delayToSend = -1

        wrongPath = False

        logger.info("check correctness of links")
        if platform == "win32":
            # Windows...
            if (isinstance(directoriesToWatch, list) and len(directoriesToWatch) > 0):
                for dest in directoriesToWatch:
                    if ('/' in dest['localDirPath']):
                        wrongPath = True
                        logger.error("In the configuraton file you set a path (" + dest[
                            'localDirPath'] + ") as a linux-like path but you are under linux")
            #same check for the workingDir variable
            if ('/' in workingDir):
                wrongPath = True
                logger.error("In the configuraton file you set a path (" + workingDir + ") as a linux-like path but you are under linux")
        elif platform == "linux" or platform == "linux2" or platform == "darwin":
            # linux or OSX
            if (isinstance(directoriesToWatch, list) and len(directoriesToWatch) > 0):
                for dest in directoriesToWatch:
                    if ('\\' in dest['localDirPath']):
                        wrongPath = True
                        logger.error("In the configuraton file you set a path (" + dest[
                            'localDirPath'] + ") as a windows-like path but you are under linux")

            # same check for the workingDir variable
            if ('\\' in workingDir):
                wrongPath = True
                logger.error(
                    "In the configuraton file you set a path (" + workingDir + ") as a windows-like path but you are under linux")
        if (wrongPath):
            logger.error("Exiting due to errors in the path (e.g., we are under Windows but the user set a linux path)")
            exit()
        else:

            if synchAtStartupBool:
                logger.info("synchronizing files at startup")
                synchAtStartup()
            if (isinstance(directoriesToWatch, list) and len(directoriesToWatch) > 0):
                for dest in directoriesToWatch:
                    if 'localDirPath' in dest and 'remoteDirPath' in dest:
                        localDirPath = unicodedata.normalize('NFKD', dest['localDirPath']).encode('ascii', 'ignore')
                        remoteDirPath = unicodedata.normalize('NFKD', dest['remoteDirPath']).encode('ascii',
                                                                                                        'ignore')
                        dictOfWatchedDir[localDirPath] = remoteDirPath

                        w = Watcher(localDirPath)
                        logger.info("Start the thread to observe changes in files in" + localDirPath)
                        w.start()

            if (delayToSend>0):
                threading.Timer(delayToSend, elaborateAllChangedFiles).start()
    except Exception as inst:
        logger.error("main Error - " + timeStamp())
        logger.error(type(inst))  # the exception instance
        logger.error(inst.args)  # arguments stored in .args
        logger.error(inst)  # __str__ allows args to be printed directly