# Timemachine-like backup on FreeBSD with ZFS

Automates creation of full/incremental backups of ZFS filesystems to USB disks with emphasis on full automation and ease of use:  
* Auto-detects known backup disks when plugged in, starts backup automagically.
* Creates incremental backups for existing filesystems or full backups for newly configured filesystems.
  * Older snapshots can automatically be cleaned up  
* Creates logs including metrics about performed operations.
* No external dependencies
  * python without external libraries
  * _devd_ for drive detection
* Can be run periodically from cron.

Gives a Time Machine like experience for FreeBSD using ZFS.

## Usage

```
$ backup -h
usage: backup [-h] -b BACKUP [BACKUP ...] [-x] [-l LOG] [-o LOGFILE] [-p]
              [-d DEVICE]
              [pools [pools ...]]
ZFS backup script.
positional arguments:
  pools                 optional whitespace separated list of pools to backup
optional arguments:
  -h, --help            show this help message and exit
  -b BACKUP [BACKUP ...], --backup BACKUP [BACKUP ...]
                        whitespace separated list of backup pools
  -x, --destroy         destroy snapshots when not used anymore
  -l LOG, --log LOG     Available levels are CRITICAL (3), ERROR (2), WARNING
                        (1), INFO (0), DEBUG (-1)
  -o LOGFILE, --output LOGFILE
                        logfile for output
  -p, --pretend         print actions instead of executing
  -d DEVICE, --device DEVICE
                        Only backup to pools on device. Must be single device
                        pool.
```

## Setup

This script has been used in production on FreeBSD 9.x up to 14.x. It needs python 3.9 and devd for automated disk detection.

### Mark filesystems to backup

Filesystems to back up are identified by a _ch.espen:backup_ property on the zfs filesystem:

```
$ zfs set ch.espen:backup=true <pool>/<fs>
```

To not have to run this script as root, we will have to set some permissions for your backup user.

Furthermore, we will mark all filesystem we want to have backed up.

## Automagically run the script

If you want to run the script, when you plug in your backup usb drive, you will have to configure a new devfs rule.

**Note:** 
Use gpt labels only. something like (gpt/backup-pool-1|da[0-9]$) would make the script run twice, but fail once. It might also work with non-gpt-labels. Just don't mix them.

This example will backup the <pool> to one of the two backup-pools specified with <backup-pool-1> and <backup-pool-2>.

```
$ cat /usr/local/etc/devd/backup.conf
# Execute backup script automatically when the configured disks are detected
notify 20 {
        match "system" "GEOM";
        match "type" "CREATE";
        match "cdev" "gpt/(wd-backup|lacie-backup)";
        action "echo Running automated backup to device: $cdev >> /var/log/backup.log";
        action "/usr/local/bin/backup -b lacie-backup wd-backup -l info -o /var/log/backup.log -d $cdev -x lake ocean";
};
```

**Note:**
Careful with single quotes in the action: https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=240411
Hence this wouldn't work:

```
action "su backup -c '/home/backup/bin/backup -b lacie-backup wd-backup -l info -o /var/log/backup.log -d $cdev -x ocean'";
```

## Run as Cronjob
This will make the script run every sunday morning at 2:15. If none of the backup-pools is currently connected, it will just log a notice and return.

```
$ crontab -e
#minute hour    mday    month   wday    command
15      2       *       *       0       /home/backup/bin/backup -b   -l info -o /var/log/backup.log -x
```

## ZFS permissions for unprivileged user

These permissions enable an unprivileged user to execute all actions the backup script uses:


```
$ zfs allow -u <user> create,mount,receive(,destroy) <backup-pool>
$ zfs allow -u <user> snapshot,send,hold,release(,destroy,mount) <pool>
```

Note the privileges in the brackets: Those allow an unprivileged user to destroy old snapshots.

To enable destroying unused snapshots pass the -x argument to the backup script. It will always keep the two newest snapshots.

*Note:* Giving permission to destroy snapshots will also grant permission to destroy a filesystem, so be careful!

### Import and export permission

Allow users to export and import the backup pools. There is no way to delegate this using ZFS permission, so either use
root or sudo. sudo is available from the ports tree.

sudoers configuration:

```
$ cat /usr/local/etc/sudoers.d/zpool
Cmnd_Alias    ZPOOL = /sbin/zpool import *, /sbin/zpool export *, !/sbin/zpool import ocean, !/sbin/zpool export ocean

<user> ALL=(root) NOPASSWD: ZPOOL
```

## Notes

### Mount filesystems from backup pool

Be careful when mounting filesystems from the backup pool. It might write meta data to the filesystem diverging the 
backup pool from the matching snapshot:
 
Set it to read-only to overcome this.
```
$ zfs set mountpoint=none <backup-pool>
```

Worst case you can manually force write a new backup to recover from this, but the script won't recover itself.

## Debugging

### devd

To debug your `devd` rules, stop the service and run the daemon in debug mode: 
```shell
service devd stop
devd -dn -f /etc/devd.conf
```

### Run script from CLI:

```shell
/usr/local/bin/backup -b wd-backup -l info -o /var/log/backup.log -d gpt/wd-backup >> /var/log/backup.log
```