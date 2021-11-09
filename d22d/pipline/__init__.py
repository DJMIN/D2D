import json
import logging
import os
import time
import threading
import unicodedata

from datetime import datetime, timedelta

from sys import platform
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from d22d.utils.utils import time_stamp, path_without_leaf

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class FileSync:
    def __init__(self):
        self.directoriesToWatch = []
        self.dictOfWatchedDir = {}
        self.synchAtStartupBool = False
        self.workingDir = "/"
        self.startBackupTime = "02:00"
        self.backupDurationInHours = 1
        self.delayToSend = -1
        self.listOfFilesToBeSent = []
        self.respectBackupHours = False
        self.ftp = None

    def sync_at_startup(self):
        try:
            if isinstance(self.directoriesToWatch, list) and len(self.directoriesToWatch) > 0:
                for dest in self.directoriesToWatch:
                    if 'localDirPath' in dest and 'remoteDirPath' in dest:
                        logger.info("synchAtStartup: synchronizing " + dest['localDirPath'])
                        arr = os.listdir(dest['localDirPath'])
                        for file in arr:
                            if not os.path.isdir(file):
                                self.moveFTPFiles(self.ftp, dest['localDirPath'] + "/" + file,
                                             dest['remoteDirPath'])
        except Exception as inst:
            logger.error("synchAtStartup Error - " + time_stamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly

    def moveFTPFiles(self, *args, **kwargs):
        return NotImplementedError

    def elaborateAllChangedFiles(self):
        global listOfFilesToBeSent
        if self.send_data_check():
            while listOfFilesToBeSent:
                file_to_upload = listOfFilesToBeSent[0]
                # file_to_upload = listOfFilesToBeSent.pop(0)
                path_without_filename = path_without_leaf(file_to_upload)
                if path_without_filename in self.dictOfWatchedDir:
                    self.moveFTPFiles(file_to_upload, self.dictOfWatchedDir[path_without_filename])
                    # when the system has finished to upload the file, we remove it from the list
                    listOfFilesToBeSent.remove(listOfFilesToBeSent[0])
                # listOfFilesToBeSent.pop(-1)
        else:
            logger.info("Not sending data because there could be a running update")
        # re-instantiate the thread that should send data
        threading.Timer(self.delayToSend, self.elaborateAllChangedFiles).start()

    def send_data_check(self):
        """
            we want to avoid sending data within an interval that goes
            among <backupDurationInHours> hour before the backup time
             and <backupDurationInHours> hour after the backup time
        """
        try:
            if self.respectBackupHours:
                if self.startBackupTime != "" and (":" in self.startBackupTime):
                    listOfParam = self.startBackupTime.split(":")
                    backupHour = listOfParam[0]
                    backupMinute = listOfParam[1]

                    minorCheck = datetime.today() - timedelta(hours=self.backupDurationInHours, minutes=0)
                    majorCheck = datetime.today() + timedelta(hours=self.backupDurationInHours, minutes=0)

                    date = datetime.now()
                    dateAndTimeOfBackup = date.replace(hour=int(backupHour), minute=int(backupMinute))

                    if dateAndTimeOfBackup > minorCheck and dateAndTimeOfBackup < majorCheck:
                        return False
                    else:
                        return True
                else:
                    # if anything goes wrong we return true
                    return True
            else:
                return True

        except Exception as inst:
            # if anything goes wrong we return true
            return True


class Handler(FileSystemEventHandler):
    def __init__(self, file_sync: FileSync, *args, **kwargs):
        self.file_sync = file_sync
        super().__init__(*args, **kwargs)

    def on_any_event(self, event):
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
                file_to_upload = event.src_path
                # Taken any action here when a file is modified.
                logger.info("Received modified event - %s." % file_to_upload)

                path_without_filename = path_without_leaf(file_to_upload)
                if path_without_filename in self.file_sync.dictOfWatchedDir:
                    if self.file_sync.delayToSend <= 0 and self.file_sync.send_data_check():
                        # directly send the file without any delay
                        self.file_sync.moveFTPFiles(file_to_upload, self.file_sync.dictOfWatchedDir[path_without_filename])
                    else:
                        # insert the name of the file in a list that will be elaborated only once every delayToSend seconds
                        if file_to_upload not in listOfFilesToBeSent:
                            listOfFilesToBeSent.append(file_to_upload)
            elif event.event_type == 'moved':
                # on linux, when a file is modified, the system create a temporal file (.goutputstream...)
                #  and them move it on the right one
                file_to_upload = event.dest_path

                # Taken any action here when a file is modified.
                logger.info("Received moved event - %s. - " % file_to_upload)

                path_without_filename = path_without_leaf(file_to_upload)
                if path_without_filename in self.file_sync.dictOfWatchedDir:
                    if self.file_sync.delayToSend <= 0 and self.file_sync.send_data_check():
                        # directly send the file without any delay
                        self.file_sync.moveFTPFiles(file_to_upload, self.file_sync.dictOfWatchedDir[path_without_filename])
                    else:
                        # insert the name of the file in a list that will be elaborated only once every delayToSend seconds
                        if (file_to_upload not in listOfFilesToBeSent):
                            listOfFilesToBeSent.append(file_to_upload)
                # path_without_filename = path_without_leaf(file_to_upload)
                # if (path_without_filename in dictOfWatchedDir):
                #     moveFTPFiles(ftpServerName, ftpU, ftpP, file_to_upload, dictOfWatchedDir[path_without_filename], useTLS)
            elif event.event_type == 'deleted':
                # do nothing
                result = None
        except Exception as inst:
            logger.error("on_any_event Error - " + time_stamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly


class Watcher(threading.Thread):
    # DIRECTORY_TO_WATCH = "/home/teo/Desktop/temp"

    def __init__(self, local_dir_path):
        try:
            threading.Thread.__init__(self)
            self.observer = Observer()
            self.localDirPath = local_dir_path
        except Exception as inst:
            logger.error("__init__ in Watcher Error for " + self.localDirPath + " - " + time_stamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly

    def run(self):
        try:
            event_handler = Handler(FileSync())
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
            logger.error("run in Watcher Error for " + self.localDirPath + " - " + time_stamp())
            logger.error(type(inst))  # the exception instance
            logger.error(inst.args)  # arguments stored in .args
            logger.error(inst)  # __str__ allows args to be printed directly


def move_file():
    conf = {
        "configuration": {
            "directoriesToWatch": [
                {
                    "localDirPath": "/home/user/temp",
                    "remoteDirPath": "temp1"
                },
                {
                    "localDirPath": "/home/user/temp2",
                    "remoteDirPath": "temp2"
                }
            ],
            "synchAtStartup": True,
            "ftpServerName": "server.domain.com",
            "ftpUser": "ftp-user",
            "ftpPass": "ftpPass",
            "useTLS": True,
            "workingDir": "/home/ftpWorkingDir",
            "delayToSend": 20,
            "startBackupTime": "10:12",
            "backupDurationInHours": 1,
            "respectBackupHours": True
        }
    }
    try:

        logger.info("loading configuration set")
        # read configuration parameters from the config.json file
        conf_path = 'config.json'
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

        # check if the folder exists, otherwise, create it
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
                    delayToSend = int(conf['delayToSend'], 10)
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
            # same check for the workingDir variable
            if ('/' in workingDir):
                wrongPath = True
                logger.error(
                    "In the configuraton file you set a path (" + workingDir + ") as a linux-like path but you are under linux")
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
                sync_at_startup()
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

            if (delayToSend > 0):
                threading.Timer(delayToSend, elaborateAllChangedFiles).start()
    except Exception as inst:
        logger.error("main Error - " + time_stamp())
        logger.error(type(inst))  # the exception instance
        logger.error(inst.args)  # arguments stored in .args
        logger.error(inst)  # __str__ allows args to be printed directly
