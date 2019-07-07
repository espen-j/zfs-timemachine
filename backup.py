#!/usr/bin/env /usr/local/bin/python2.7

import optparse
import argparse
import subprocess
import StringIO
import shlex
import logging
import errno
from datetime import datetime, timedelta

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME="ch.espen:backup"
SNAPSHOT_PROPERTY_VALUE="true"

DATE = datetime.now().strftime("%Y%m%d%H%M")

LABEL = "{}_{}".format

ZPOOL_LIST = "/sbin/zpool list -H -o name"
ZPOOL_LIST_DEVICES = "zpool list -Hvo name {}".format
ZPOOL_IMPORT = "/usr/local/bin/sudo /sbin/zpool import {}".format
ZPOOL_IMPORT_PATH = "/usr/local/bin/sudo /sbin/zpool import -d {} {}".format
ZPOOL_EXPORT = "/usr/local/bin/sudo /sbin/zpool export {}".format

ZFS_LIST_FILESYSTEMS = "/sbin/zfs list -H -r -o name {}".format
ZFS_LIST_SNAPSHOTS = "/sbin/zfs list -H -t snapshot -o name -s creation -r {}".format

ZFS_GET_PROPERTY = "/sbin/zfs get -H -o value {} {}".format
ZFS_SNAPSHOT = "{}@{}".format
ZFS_CREATE = "/sbin/zfs create {}/{}".format
ZFS_CREATE_SNAPSHOT = "/sbin/zfs snapshot {}".format
ZFS_KEEP_SNAPSHOT = "/sbin/zfs hold ch.espen:backup {}".format
ZFS_RELEASE_SNAPSHOT = "/sbin/zfs release ch.espen:backup {}".format
ZFS_DESTROY_SNAPSHOT = "/sbin/zfs destroy {}".format

ZFS_SEND = "/sbin/zfs send {}".format
ZFS_SEND_INCREMENTAL = "/sbin/zfs send -i {} {}".format
ZFS_RECEIVE = "/sbin/zfs recv {}/{}".format
ZFS_STREAM_SIZE = "/sbin/zfs send -nP {}".format
ZFS_INCREMENTAL_STREAM_SIZE = "/sbin/zfs send -nP -i {} {}".format
ZFS_AVAILABLE_SPACE = "/sbin/zfs get -o value -Hp available {}".format

global options
global logger

def main():
    global options
    global logger
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(description='Process benchmarks.')
    
    parser.add_argument("-b", "--backup", default=[], type=str, nargs='+', required=True,
                      help="whitespace separated list of backup pools")
    parser.add_argument('pools', nargs='*',
                      help="optional whitespace separated list of pools to backup")
    parser.add_argument("-x", "--destroy", action="store_true", dest="destroy", default=False,
                      help="destroy snapshots when not used anymore")
    parser.add_argument("-l", "--log", type=str, default="WARNING",
                      help="Available levels are CRITICAL (3), ERROR (2), WARNING (1), INFO (0), DEBUG (-1)")
    parser.add_argument("-o", "--output", dest="logfile", default=None,
                      help="logfile for output")
    parser.add_argument("-p", "--pretend", action="store_true", dest="pretend", default=False,
                      help="print actions instead of executing")
    parser.add_argument("-d", "--device", type=str, dest="device", default=None,
                      help="Only backup to pools on device. Must be single device pool.")
    options = parser.parse_args()

    try:
        loglevel = getattr(logging, options.log.upper())
    except AttributeError:
        loglevel = { 3:logging.CRITICAL,
                     2:logging.ERROR,
                     1:logging.WARNING,
                     0:logging.INFO,
                     -1:logging.DEBUG,
                   } [int(options.log)]

    if options.logfile:
        logHandler = logging.FileHandler(options.logfile)
    else:
        logHandler = logging.StreamHandler()

    logHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger(__file__)
    logger.setLevel(loglevel)
    logger.addHandler(logHandler)

    device = options.device
    if device.startswith("/dev/"):
        device = device[5:]
    if device:
        logger.debug("device: %s", device)

    backupPools = []
    allPools = getPools()

    for backupPool in options.backup:
        imported = False
        if backupPool not in allPools:
            imported = importPool(backupPool)
            if not imported:
                continue
        if device:
            if not all(dev==device for dev in getDevices(backupPool)):
                if imported:
                    exportPool(backupPool)
                continue;
        backupPools.append(backupPool)
        allPools.append(backupPool)

    logger.debug("available backup pools: %s", " ".join(backupPools))

    if not backupPools:
        logger.info("No backup pools available, exiting...")
        exit

    pools = [x for x in allPools if x not in options.backup]
    if options.pools:
        pools = [x for x in pools if x in options.pools]

    logger.debug("pools to backup: %s", " ".join(pools))
    
    backup(pools, backupPools)

def backup(pools, backupPools):
    for backupPool in backupPools:
    
        for pool in pools:
            filesystems = [fs for fs in getFilesystems(pool) if getProperty(fs, SNAPSHOT_PROPERTY_NAME)=="true"]
            logger.debug("filesystems to backup to %s: %s", backupPool, " ".join(filesystems))
            for fs in filesystems:
                returncode = createFilesystem(backupPool, fs)
                if returncode > 0:
                    continue
                snapshots = [s for s in getSnapshots(fs) if backupPool in s]
                newSnapshot = createSnapshot(fs, LABEL(backupPool, DATE))
                success = False
                if not newSnapshot:
                    continue
                if not snapshots:
                    returncode = doBackup(backupPool, fs, newSnapshot)
                    if returncode == 0:
                        success = True
                else:
                    for snapshot in snapshots:
                        returncode = doIncrementalBackup(backupPool, fs, newSnapshot, snapshot)
                        if returncode == 0:
                            success = True
                            holdSnapshot(snapshot, False)
                            destroySnapshot(snapshot)
                            break
                        elif returncode == 2:
                            break
                if success:
                    holdSnapshot(newSnapshot)
                    if options.destroy:
                        # keep last two snapshots on backup pool
                        for snapshot in getSnapshots(backupPool + "/" +fs)[2:]:
                            destroySnapshot(snapshot)
                else:
                    if options.destroy:
                        destroySnapshot(newSnapshot)

        exportPool(backupPool)

def doBackup(destPool, filesystem, snapshot):

    streamSize = getStreamSize(snapshot)
    poolSize = getFreeSpace(destPool)

    if streamSize > poolSize:
        logger.error("Size of %s stream (%s) exceeds size of pool %s (%s)", snapshot, size(streamSize), destPool, size(poolSize))
        return 1
    
    logger.debug("creating initial backup of %s to %s", snapshot, destPool)
    start = datetime.now()
    (stdoutdata, returncode) = pipeCommands(ZFS_SEND(snapshot), ZFS_RECEIVE(destPool, filesystem), options.pretend)
    stop = datetime.now()
    if returncode > 0:
        logger.error("Failed to do initial backup of %s@%s to %s: %s", filesystem, snapshot, destPool, stdoutdata)
        return 1
    else:
        delta = datetime.now() - start
        logger.info("Initial backup of %s to %s successful in %s, size %s", filesystem, destPool, delta, size(streamSize))
        return 0

def doIncrementalBackup(destPool, filesystem, newSnapshot, snapshot):
    streamSize = getStreamSize(newSnapshot, snapshot)
    poolSize = getFreeSpace(destPool)

    if streamSize > poolSize:
        logger.error("Size of %s stream (%s) exceeds size of pool %s (%s)", snapshot, size(streamSize), destPool,  size(poolSize))
        return 2

    logger.debug("creating incremental backup of %s to %s based on %s", newSnapshot, destPool, snapshot)
    start = datetime.now()
    (stdoutdata, returncode) = pipeCommands(ZFS_SEND_INCREMENTAL(snapshot, newSnapshot), ZFS_RECEIVE(destPool, filesystem), options.pretend)
    if returncode > 0:
        logger.error("Failed to do incremental backup of %s@%s to %s based on %s: %s", filesystem, snapshot, destPool, newSnapshot, stdoutdata)
        return 1
    else:
        delta = datetime.now() - start
        logger.info("Incremental backup of %s to %s successful in %s, size %s", filesystem, destPool, delta, size(streamSize))
        return 0

def holdSnapshot(snapshot, hold=True):
    if hold:
        (stdoutdata, stderrdata, returncode) = runCommand(ZFS_KEEP_SNAPSHOT(snapshot))
        if returncode > 0:
            logger.error("Failed to hold snapshot %s: %s", snapshot, stderrdata)
        else:
            logger.debug("Holding snapshot %s", snapshot)
    else:
        (stdoutdata, stderrdata, returncode) = runCommand(ZFS_RELEASE_SNAPSHOT(snapshot))
        if returncode > 0:
            logger.error("Failed to release snapshot %s: %s", snapshot, stderrdata)
        else:
            logger.debug("Released snapshot %s", snapshot)

def destroySnapshot(snapshot):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_DESTROY_SNAPSHOT(snapshot), options.pretend)
    if returncode > 0:
        logger.error("Failed to destroy snapshot %s: %s", snapshot, stderrdata)
    else:
        logger.debug("Destroyed snapshot %s", snapshot)

def getFreeSpace(pool):
    if options.pretend:
        return 2
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_AVAILABLE_SPACE(pool))
    if returncode > 0:
        logger.error("Failed to get free space for %s", pool)
        return None
    else:
        return long(stdoutdata)

def getStreamSize(newSnapshot, snapshot=None):
    size = None
    if options.pretend:
        return 1
    if snapshot:
        (stdoutdata, stderrdata, returncode) = runCommand(ZFS_INCREMENTAL_STREAM_SIZE(snapshot, newSnapshot))
    else:
        (stdoutdata, stderrdata, returncode) = runCommand(ZFS_STREAM_SIZE(newSnapshot))
    if returncode > 0:
        logger.error("Failed to get stream size for %s: %s", newSnapshot, stderrdata)
    else:
        output = stdoutdata if stdoutdata else stderrdata
        size = "".join([line.split()[1] for line in StringIO.StringIO(output).readlines()[1:]])

    return long(size)

def createSnapshot(fs, name):
    logger.debug("creating snapshot for %s", fs)
    snapshot = ZFS_SNAPSHOT(fs, name)
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_CREATE_SNAPSHOT(snapshot), options.pretend)
    if returncode > 0:
        logger.error("Failed to create snapshot %s: %s", snapshot, stderrdata)
        return None
    else:
        return snapshot

def getPools():
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_LIST)
    if returncode > 0:
        logger.error("Failed to retrieve pools: %s", stderrdata)
    pools = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    logger.debug("available pools: %s", " ".join(pools))

    return pools

def importPool(pool, searchPath=None):
    if searchPath:
        (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_IMPORT_PATH(searchPath, pool))
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_IMPORT(pool))
    if returncode > 0:
        logger.debug("could not import pool %s: %s %s", pool, stderrdata, returncode)
        return False
    else:
        logger.debug("%s imported", pool)
        return True

def exportPool(pool):   
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_EXPORT(pool))
    if returncode > 0:
        logger.debug("could not export pool %s: %s", pool, stderrdata)
    else:
        logger.debug("%s exported", pool)

def getDevices(pool):
    (stdoutdata, stderrdata, returncode) = runCommand(ZPOOL_LIST_DEVICES(pool))
    if returncode > 0:
        logger.debug("could not get devices for %s: %s", pool, stderrdata)
        return []
    else:
        devices = [line.split()[0] for line in StringIO.StringIO(stdoutdata).readlines()[1:]]
        logger.debug("devices in %s: %s", pool, devices)
        return devices

def getFilesystems(pool):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_LIST_FILESYSTEMS(pool))
    filesystems = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    if returncode > 0:
        logger.error("could not get filesystems for %s", pool)
        return []
    else:
        filesystems = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
        logger.debug("filesystems in pool %s: %s", pool, " ".join(filesystems))
        return filesystems

def getProperty(filesystem, property):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_GET_PROPERTY(property, filesystem))
    if returncode > 0:
        logger.error("Failed to retrieve property %s from %s", property, filesystem)
    return stdoutdata.strip()

def getSnapshots(filesystem):
    (stdoutdata, stderrdata, returncode) = runCommand(ZFS_LIST_SNAPSHOTS(filesystem, options.pretend))
    if returncode > 0:
        logger.error("could not get snapshots for %s: %s", filesystem, stderrdata)
        return []
    snapshots = [line.strip() for line in StringIO.StringIO(stdoutdata).readlines()]
    return snapshots[::-1]

def createFilesystem(pool, filesystem):
    filesystems = getFilesystems(pool)
    paths = filesystem.split("/")
    for i in range(1, len(paths)):
        path = "/".join(paths[:i])
        if pool + "/" + path not in filesystems:
            logger.info("creating filesystem %s/%s", pool, path)
            (stdoutdata, stderrdata, returncode) = runCommand(ZFS_CREATE(pool, path), options.pretend)
            if returncode > 0:
                logger.error("Failed to create filesystem %s/%s: %s", pool, path, stderrdata)
                return 1
    return 0

def runCommand(command, pretend=False):
    if pretend:
        logger.info("running command: %s", command)
        return ("", "", 0)
    else:
        args = shlex.split(command)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        return (stdoutdata, stderrdata, process.returncode)

def pipeCommands(command1, command2, pretend=False):
    if pretend:
        logger.info("running command: %s | %s", command1, command2)
        return ("", 0)
    else:
        returncode = 0
        try:
            stdoutdata = subprocess.check_output(command1 + " | " + command2 , stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            stderrdata = e.output
            returncode = e.returncode
            return (e.output, e.returncode)
        return (stdoutdata, returncode)    

def size(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

if __name__ == "__main__":
    main()
