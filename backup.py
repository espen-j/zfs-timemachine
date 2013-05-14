#!/usr/bin/env python

import optparse
import argparse
import subprocess
import logging
import StringIO
import shlex
from datetime import datetime, timedelta

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME="ch.espen:backup"
SNAPSHOT_PROPERTY_VALUE="true"

DATE = datetime.now().strftime("%Y%m%d%H%M")

LABEL = "{}_{}".format

ZPOOL_LIST = "/sbin/zpool list -H -o name"
ZPOOL_IMPORT = "sudo /sbin/zpool import {}".format
ZPOOL_EXPORT = "sudo /sbin/zpool export {}".format

ZFS_LIST_FILESYSTEMS = "/sbin/zfs list -H -r -o name {}".format
ZFS_LIST_SNAPSHOTS = "/sbin/zfs list -H -t snapshot -o name -s creation -r {}".format

ZFS_GET_PROPERTY = "/sbin/zfs get -H -o value {} {}".format
ZFS_SNAPSHOT = "{}@{}".format
ZFS_CREATE = "/sbin/zfs create {}/{}".format
ZFS_CREATE_SNAPSHOT = "/sbin/zfs snapshot {}".format

ZFS_SEND = "/sbin/zfs send {}".format
ZFS_SEND_INCREMENTAL = "/sbin/zfs send -i {} {}".format
ZFS_RECEIVE = "/sbin/zfs recv {}/{}".format

global options

def main():
    global options
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(description='Process benchmarks.')
    
    parser.add_argument("-b", "--backup", default=[], type=str, nargs='+', required=True,
                      help="whitespace separated list of backup pools")
    parser.add_argument('pools', nargs='*',
                      help="optional whitespace separated list of pools to backup")
    parser.add_argument("-l", "--log", type=str, default="WARNING",
                      help="Available levels are CRITICAL (3), ERROR (2), WARNING (1), INFO (0), DEBUG (-1)")
    parser.add_argument("-p", "--pretend", action="store_true", dest="pretend", default=False,
                      help="print actions instead of executing")
    options = parser.parse_args()

    try:
        loglevel = getattr(logging, options.log.upper())
    except AttributeError:
        loglevel = {3:logging.CRITICAL,
                    2:logging.ERROR,
                    1:logging.WARNING,
                    0:logging.INFO,
                    -1:logging.DEBUG,
                    }[int(options.log)]

    logging.basicConfig(level=loglevel)

    for pool in options.backup:
        importPool(pool)

    allPools = getPools()
    backupPools = list(set(allPools).intersection( set(options.backup) ))
    logging.debug("available backup pools: %s", " ".join(backupPools))

    pools = [x for x in allPools if x not in backupPools]
    if options.pools:
        pools = [x for x in pools if x in options.pools]

    logging.debug("pools to backup: %s", " ".join(pools))
    
    backup(pools, backupPools)

def backup(pools, backupPools):
    for backupPool in backupPools:
    
        for pool in pools:
            filesystems = [fs for fs in getFilesystems(pool) if getProperty(fs, SNAPSHOT_PROPERTY_NAME)=="true"]
            logging.debug("filesystems to backup to %s: %s", backupPool, " ".join(filesystems))
            for fs in filesystems:
                createFilesystem(backupPool, fs)
                snapshots = [s for s in getSnapshots(fs) if backupPool in s]
                newSnapshot = createSnapshot(fs, LABEL(backupPool, DATE))
                if not newSnapshot:
                    continue
                if not snapshots:
                    doBackup(backupPool, fs, newSnapshot)
                else:
                    for snapshot in snapshots:
                        returncode = doIncrementalBackup(backupPool, fs, newSnapshot, snapshot)
                        if returncode == 0:
                            break
        exportPool(backupPool)

def doBackup(destPool, filesystem, snapshot):
    logging.info("creating initial backup of %s to %s", snapshot, destPool)
    start = datetime.now()
    (stdoutdata, stderrdata, returncode) = pipeCommands(ZFS_SEND(snapshot), ZFS_RECEIVE(destPool, filesystem), options.pretend)
    stop = datetime.now()
    if returncode > 0:
        logging.error("Failed to do initial backup of %s@%s to %s: %s", filesystem, snapshot, destPool, stderrdata)
        return 1
    else:
        delta = datetime.now() - start
        logging.info("Initial backup of %s to %s successful in %s", filesystem, destPool, delta)
        return 0

def doIncrementalBackup(destPool, filesystem, newSnapshot, snapshot):
    logging.info("creating incremental backup of %s to %s based on %s", newSnapshot, destPool, snapshot)
    start = datetime.now()
    (stdoutdata, stderrdata, returncode) = pipeCommands(ZFS_SEND_INCREMENTAL(snapshot, newSnapshot), ZFS_RECEIVE(destPool, filesystem), options.pretend)
    if returncode > 0:
        logging.error("Failed to do incremental backup of %s@%s to %s based on %s: %s", filesystem, snapshot, destPool, newSnapshot, stderrdata)
        return 1
    else:
        delta = datetime.now() - start
        logging.info("Incremental backup of %s to %s successful in %s", filesystem, destPool, delta)
        return 0

def createSnapshot(fs, name):
    logging.debug("creating snapshot for %s", fs)
    snapshot = ZFS_SNAPSHOT(fs, name)
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_CREATE_SNAPSHOT(snapshot), options.pretend)
    if returncode > 0:
        logging.error("Failed to create snapshot %s: %s", snapshot, stderrdata)
        return None
    else:
        return snapshot

def getPools():
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_LIST)
    if returncode > 0:
        logging.error("Failed to retrieve pools: %s", stderrdata)
    pools = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    logging.debug("available pools: %s", " ".join(pools))

    return pools

def importPool(pool):
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_IMPORT(pool), options.pretend)
    if returncode > 0:
        logging.info("could not import pool %s: %s", pool, stderrdata)
    else:
        logging.info("%s imported", pool)

def exportPool(pool):   
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_EXPORT(pool), options.pretend)
    if returncode > 0:
        logging.info("could not export pool %s: %s", pool, stderrdata)
    else:
        logging.info("%s exported", pool)

def getFilesystems(pool):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_LIST_FILESYSTEMS(pool))
    filesystems = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    if returncode > 0:
        logging.error("could not get filesystems for %s", pool)
        return []
    else:
        filesystems = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
        logging.debug("filesystems in pool %s: %s", pool, " ".join(filesystems))
        return filesystems

def getProperty(filesystem, property):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_GET_PROPERTY(property, filesystem))
    if returncode > 0:
        logging.error("Failed to retrieve property %s from %s", property, filesystem)
    return stdoutdata.strip()

def getSnapshots(filesystem):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_LIST_SNAPSHOTS(filesystem, options.pretend))
    if returncode > 0:
        logging.error("could not get snapshots for %s: %s", filesystem, stderrdata)
        return []
    snapshots = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    return snapshots[::-1]

def createFilesystem(pool, filesystem):
    filesystems = getFilesystems(pool)
    paths = filesystem.split("/")
    for i in range(1, len(paths)+1):
        path = "/".join(paths[:i])
        if pool + "/" + path not in filesystems:
            logging.info("creating filesystem %s/%s", pool, path)
            (stdoutdata, stderrdata, returncode) = runCommand(ZFS_CREATE(pool, path), options.pretend)
            if returncode > 0:
                logging.error("Failed to create filesystem %s/%s: %s", pool, path, stderrdata)
                break

def runCommand(command, pretend=False):
    if pretend:
        logging.info("running command: %s", command)
        return ("", "", 0)
    else:
        args = shlex.split(command)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        return (stdoutdata, stderrdata, process.returncode)

def pipeCommands(command1, command2, pretend=False):
    if pretend:
        logging.info("running command: %s | %s", command1, command2)
        return ("", "", 0)
    else:
        args1 = shlex.split(command1)
        args2 = shlex.split(command1)
        process1 = subprocess.Popen(args1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process2 = subprocess.Popen(args2, stdin=process1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process2.communicate()
        return (stdoutdata, stderrdata, process2.returncode)    

if __name__ == "__main__":
    main()
