#!/usr/bin/env /usr/local/bin/python3.9

import argparse
import io
import logging
import shlex
import subprocess
from datetime import datetime

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME = "ch.espen:backup"
SNAPSHOT_PROPERTY_VALUE = "true"

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
        loglevel = {3: logging.CRITICAL,
                    2: logging.ERROR,
                    1: logging.WARNING,
                    0: logging.INFO,
                    -1: logging.DEBUG,
                    }[int(options.log)]

    if options.logfile:
        log_handler = logging.FileHandler(options.logfile)
    else:
        log_handler = logging.StreamHandler()

    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger(__file__)
    logger.setLevel(loglevel)
    logger.addHandler(log_handler)

    device = options.device
    if device.startswith("/dev/"):
        device = device[5:]
    if device:
        logger.debug("device: %s", device)

    backup_pools = []
    all_pools = get_pools()

    for backupPool in options.backup:
        imported = False
        if backupPool not in all_pools:
            imported = import_pool(backupPool)
            if not imported:
                continue
        if device:
            if not all(dev == device for dev in get_devices(backupPool)):
                if imported:
                    export_pool(backupPool)
                continue
        backup_pools.append(backupPool)
        all_pools.append(backupPool)

    logger.debug("available backup pools: %s", " ".join(backup_pools))

    if not backup_pools:
        logger.info("No backup pools available, exiting...")
        exit()

    pools = [x for x in all_pools if x not in options.backup]
    if options.pools:
        pools = [x for x in pools if x in options.pools]

    logger.debug("pools to backup: %s", " ".join(pools))

    backup(pools, backup_pools)


def backup(pools, backup_pools):
    for backupPool in backup_pools:

        for pool in pools:
            filesystems = [fs for fs in get_filesystems(pool) if get_property(fs, SNAPSHOT_PROPERTY_NAME) == "true"]
            logger.debug("filesystems to backup to %s: %s", backupPool, " ".join(filesystems))
            for fs in filesystems:
                return_code = create_filesystem(backupPool, fs)
                if return_code > 0:
                    continue
                snapshots = [s for s in get_snapshots(fs) if backupPool in s]
                new_snapshot = create_snapshot(fs, LABEL(backupPool, DATE))
                success = False
                if not new_snapshot:
                    continue
                if not snapshots:
                    return_code = do_backup(backupPool, fs, new_snapshot)
                    if return_code == 0:
                        success = True
                else:
                    for snapshot in snapshots:
                        return_code = do_incremental_backup(backupPool, fs, new_snapshot, snapshot)
                        if return_code == 0:
                            success = True
                            hold_snapshot(snapshot, False)
                            destroy_snapshot(snapshot)
                            break
                        elif return_code == 2:
                            break
                if success:
                    hold_snapshot(new_snapshot)
                    if options.destroy:
                        # keep last two snapshots on backup pool
                        for snapshot in get_snapshots(backupPool + "/" + fs)[2:]:
                            destroy_snapshot(snapshot)
                else:
                    if options.destroy:
                        destroy_snapshot(new_snapshot)

        export_pool(backupPool)


def do_backup(destination_pool, filesystem, snapshot):
    stream_size = get_stream_size(snapshot)
    pool_size = get_free_space(destination_pool)

    if stream_size > pool_size:
        logger.error("Size of %s stream (%s) exceeds size of pool %s (%s)", snapshot, size(stream_size),
                     destination_pool,
                     size(pool_size))
        return 1

    logger.debug("creating initial backup of %s to %s", snapshot, destination_pool)
    start = datetime.now()
    (data, return_code) = pipe_commands(ZFS_SEND(snapshot), ZFS_RECEIVE(destination_pool, filesystem), options.pretend)
    if return_code > 0:
        logger.error("Failed to do initial backup of %s@%s to %s: %s", filesystem, snapshot, destination_pool, data)
        return 1
    else:
        delta = datetime.now() - start
        logger.info("Initial backup of %s to %s successful in %s, size %s", filesystem, destination_pool, delta,
                    size(stream_size))
        return 0


def do_incremental_backup(destination_pool, filesystem, new_snapshot, snapshot):
    stream_size = get_stream_size(new_snapshot, snapshot)
    pool_size = get_free_space(destination_pool)

    if stream_size > pool_size:
        logger.error("Size of %s stream (%s) exceeds size of pool %s (%s)", snapshot, size(stream_size),
                     destination_pool,
                     size(pool_size))
        return 2

    logger.debug("creating incremental backup of %s to %s based on %s", new_snapshot, destination_pool, snapshot)
    start = datetime.now()
    (data, return_code) = pipe_commands(ZFS_SEND_INCREMENTAL(snapshot, new_snapshot),
                                        ZFS_RECEIVE(destination_pool, filesystem), options.pretend)
    if return_code > 0:
        logger.error("Failed to do incremental backup of %s@%s to %s based on %s: %s", filesystem, snapshot,
                     destination_pool,
                     new_snapshot, data)
        return 1
    else:
        delta = datetime.now() - start
        logger.info("Incremental backup of %s to %s successful in %s, size %s", filesystem, destination_pool, delta,
                    size(stream_size))
        return 0


def hold_snapshot(snapshot, hold=True):
    if hold:
        (data, err_data, return_code) = run_command(ZFS_KEEP_SNAPSHOT(snapshot))
        if return_code > 0:
            logger.error("Failed to hold snapshot %s: %s", snapshot, err_data)
        else:
            logger.debug("Holding snapshot %s", snapshot)
    else:
        (data, err_data, return_code) = run_command(ZFS_RELEASE_SNAPSHOT(snapshot))
        if return_code > 0:
            logger.error("Failed to release snapshot %s: %s", snapshot, err_data)
        else:
            logger.debug("Released snapshot %s", snapshot)


def destroy_snapshot(snapshot):
    (data, err_data, return_code) = run_command(ZFS_DESTROY_SNAPSHOT(snapshot), options.pretend)
    if return_code > 0:
        logger.error("Failed to destroy snapshot %s: %s", snapshot, err_data)
    else:
        logger.debug("Destroyed snapshot %s", snapshot)


def get_free_space(pool):
    if options.pretend:
        return 2
    (data, err_data, return_code) = run_command(ZFS_AVAILABLE_SPACE(pool))
    if return_code > 0:
        logger.error("Failed to get free space for %s", pool)
        return None
    else:
        return long(data)


def get_stream_size(new_snapshot, snapshot=None):
    calc_size = None
    if options.pretend:
        return 1
    if snapshot:
        (data, err_data, return_code) = run_command(ZFS_INCREMENTAL_STREAM_SIZE(snapshot, new_snapshot))
    else:
        (data, err_data, return_code) = run_command(ZFS_STREAM_SIZE(new_snapshot))
    if return_code > 0:
        logger.error("Failed to get stream size for %s: %s", new_snapshot, err_data)
    else:
        output = data if data else err_data
        calc_size = "".join([line.split()[1] for line in io.StringIO(output).readlines()[1:]])

    return long(calc_size)


def create_snapshot(fs, name):
    logger.debug("creating snapshot for %s", fs)
    snapshot = ZFS_SNAPSHOT(fs, name)
    (data, err_data, return_code) = run_command(ZFS_CREATE_SNAPSHOT(snapshot), options.pretend)
    if return_code > 0:
        logger.error("Failed to create snapshot %s: %s", snapshot, err_data)
        return None
    else:
        return snapshot


def get_pools():
    (data, err_data, return_code) = run_command(ZPOOL_LIST)
    if return_code > 0:
        logger.error("Failed to retrieve pools: %s", err_data)
    pools = [line.strip() for line in io.StringIO(data).readlines()]
    logger.debug("available pools: %s", " ".join(pools))

    return pools


def import_pool(pool, search_path=None):
    if search_path:
        (data, err_data, return_code) = run_command(ZPOOL_IMPORT_PATH(search_path, pool))
    else:
        (data, err_data, return_code) = run_command(ZPOOL_IMPORT(pool))
    if return_code > 0:
        logger.debug("could not import pool %s: %s %s", pool, err_data, return_code)
        return False
    else:
        logger.debug("%s imported", pool)
        return True


def export_pool(pool):
    (data, err_data, return_code) = run_command(ZPOOL_EXPORT(pool))
    if return_code > 0:
        logger.debug("could not export pool %s: %s", pool, err_data)
    else:
        logger.debug("%s exported", pool)


def get_devices(pool):
    (data, err_data, return_code) = run_command(ZPOOL_LIST_DEVICES(pool))
    if return_code > 0:
        logger.debug("could not get devices for %s: %s", pool, err_data)
        return []
    else:
        devices = [line.split()[0] for line in io.StringIO(data).readlines()[1:]]
        logger.debug("devices in %s: %s", pool, devices)
        return devices


def get_filesystems(pool):
    (data, err_data, return_code) = run_command(ZFS_LIST_FILESYSTEMS(pool))
    if return_code > 0:
        logger.error("could not get filesystems for %s", pool)
        return []
    else:
        filesystems = [line.strip() for line in io.StringIO(data).readlines()]
        logger.debug("filesystems in pool %s: %s", pool, " ".join(filesystems))
        return filesystems


def get_property(filesystem, zfs_property):
    (data, err_data, return_code) = run_command(ZFS_GET_PROPERTY(zfs_property, filesystem))
    if return_code > 0:
        logger.error("Failed to retrieve property %s from %s", zfs_property, filesystem)
    return data.strip()


def get_snapshots(filesystem):
    (data, err_data, return_code) = run_command(ZFS_LIST_SNAPSHOTS(filesystem, options.pretend))
    if return_code > 0:
        logger.error("could not get snapshots for %s: %s", filesystem, err_data)
        return []
    snapshots = [line.strip() for line in io.StringIO(data).readlines()]
    return snapshots[::-1]


def create_filesystem(pool, filesystem):
    filesystems = get_filesystems(pool)
    paths = filesystem.split("/")
    for i in range(1, len(paths)):
        path = "/".join(paths[:i])
        if pool + "/" + path not in filesystems:
            logger.info("creating filesystem %s/%s", pool, path)
            (data, err_data, return_code) = run_command(ZFS_CREATE(pool, path), options.pretend)
            if return_code > 0:
                logger.error("Failed to create filesystem %s/%s: %s", pool, path, err_data)
                return 1
    return 0


def run_command(command, pretend=False):
    if pretend:
        logger.info("running command: %s", command)
        return "", "", 0
    else:
        args = shlex.split(command)
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (data, err_data) = process.communicate()
        return data, err_data, process.returncode


def pipe_commands(command1, command2, pretend=False):
    if pretend:
        logger.info("running command: %s | %s", command1, command2)
        return "", 0
    else:
        return_code = 0
        try:
            data = subprocess.check_output(command1 + " | " + command2, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            return e.output, e.returncode
        return data, return_code


def size(num):
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if 1024.0 > num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


if __name__ == "__main__":
    main()
