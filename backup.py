#!/usr/bin/env python

import optparse
import subprocess
import logging
import StringIO
import shlex
import time

# backup pools
BACKUP_POOLS=['usbstick', 'bla']

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME="ch.espen:auto-backup"
SNAPSHOT_PROPERTY_VALUE="true"

DATE = time.strftime("%Y%m%d%H%M", time.localtime())

global options

def main():
    global options
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-l", "--log",
                      type="string", default="WARNING",
                      help="Available levels are CRITICAL (3), ERROR (2), WARNING (1), INFO (0), DEBUG (-1)")
    parser.add_option("-p", "--pretend",
                      action="store_true", dest="pretend", default=False,
                      help="print actions instead of executing")
    (options, args) = parser.parse_args()

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

    allPools = getPools()
    backupPools = list(set(allPools).intersection( set(BACKUP_POOLS) ))
    logging.info("available backup pools: %s", " ".join(backupPools))

    pools = [x for x in allPools if x not in backupPools]
    logging.info("pools to backup: %s", " ".join(pools))
    
    backup(pools, backupPools)

def backup(pools, backupPools):
    for backupPool in backupPools:
        label = backupPool + "_" + DATE
    
        for pool in pools:
            filesystems = [fs for fs in getFilesystems(pool) if hasProperty(fs, SNAPSHOT_PROPERTY_NAME)]
            logging.info("backing up following filesystems from pool %s to backup pool %s: %s", pool, backupPool, " ".join(filesystems))
            for fs in filesystems:
                createFilesystem(backupPool, fs)
                snapshots = [s for s in getSnapshots(fs) if backupPool in s]
                newSnapshot = createSnapshot(fs, label)
                if not snapshots:
                    doBackup(backupPool, fs, newSnapshot)
                else:
                    for snapshot in snapshots:
                        doIncrementalBackup(backupPool, fs, newSnapshot, snapshot)

def doBackup(destPool, filesystem, snapshot):
    logging.info("creating initial backup of %s to %s", snapshot, destPool)
    command1 = "zfs send " + snapshot
    command2 = "zfs recv " + destPool + "/" + filesystem
    pipeCommands(command1, command2, options.pretend)

def doIncrementalBackup(destPool, filesystem, newSnapshot, snapshot):
    logging.info("creating incremental backup of %s to %s based on %s", newSnapshot, destPool, snapshot)
    command1 = "zfs send -i " + snapshot + " " + newSnapshot
    command2 = "zfs recv " + destPool + "/" + filesystem
    pipeCommands(command1, command2, options.pretend)

def createSnapshot(fs, label):
    logging.info("creating snapshot for %s", fs)
    snapshot = fs + "@" + label
    command = "zfs snapshot " + snapshot
    runCommand(command, options.pretend)
    return snapshot

def getPools():
    command = "zpool list"
    pools = [line.split(" ")[0] for line in runCommand(command).readlines()[1:]]
    logging.info("pools available in system: %s", " ".join(pools))

    return pools

def getFilesystems(pool):
    command = "zfs list -r " + pool
    filesystems = [line.split()[0] for line in runCommand(command).readlines()[1:]]

    logging.debug("filesystems in pool %s: \n%s", pool, "\n".join(filesystems))
    return filesystems

def hasProperty(filesystem, property):
    # ${ZFS} get ${SNAPSHOT_PROPERTY_NAME} $fs
    command = "zfs get " + property + " " + filesystem
    output = [line.split()[2] for line in runCommand(command).readlines()[1:]]
    return "true" == "".join(output)

def getSnapshots(filesystem):
    #${ZFS} list -t snapshot -o name,creation -s creation -r ${fs} | cut -d' ' -f1 | ${TAIL} -n 1 | grep "${fs}@${pool}" | tr '\n' ' '
    command = "zfs list -t snapshot -o name -s creation -r "+filesystem
    snapshots = [line.split()[0] for line in runCommand(command).readlines()[1:]]
    return snapshots[::-1]

def createFilesystem(pool, filesystem):
    filesystems = getFilesystems(pool)
    paths = filesystem.split("/")
    for i in range(1, len(paths)+1):
        path = "/".join(paths[:i])
        if pool + "/" + path not in filesystems:
            logging.info("creating filesystem %s on pool %s", path, pool)
            runCommand("zfs create " + pool + "/" + path, options.pretend)

def runCommand(command, pretend=False):
    if pretend:
        logging.info("running command: %s", command)
    else:
        args = shlex.split(command)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process.communicate()
        if process.returncode != 0:
            logging.error("could not execute command %s: error: %s returncode: %s", command, stderrdata, process.returncode)
            exit(1)
        else:
            return StringIO.StringIO(stdoutdata)

def pipeCommands(command1, command2, pretend=False):
    if pretend:
        logging.info("running command: %s | %s", command1, command2)
    else:
        args1 = shlex.split(command1)
        args2 = shlex.split(command1)
        process1 = subprocess.Popen(args1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process2 = subprocess.Popen(args2, stdin=process1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutdata, stderrdata) = process2.communicate()
        if process2.returncode != 0:
            logging.error("could not execute command %s: error: %s returncode: %s", command2, stderrdata, process2.returncode)
            exit(1)    

if __name__ == "__main__":
    main()
