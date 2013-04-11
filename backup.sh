#!/usr/bin/env bash
#
# zfs_backup.sh version v 0.1 2013-04-10
# Copyright 2013 espen
#
# backup pool to another pool
# -h help page

# Path to binaries used 
ZPOOL="/sbin/zpool"
ZFS="/sbin/zfs"
EGREP="/usr/bin/egrep"
GREP="/usr/bin/grep"
TAIL="/usr/bin/tail"
SORT="/usr/bin/sort"
XARGS="/usr/bin/xargs"
DATE="/bin/date"
CUT="/usr/bin/cut"
TAIL="/usr/bin/tail"
TR="/usr/bin/tr"


# backup pools
BACKUP_POOLS="usbstick,bla"

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME="ch.espen:auto-backup"
SNAPSHOT_PROPERTY_VALUE="true"


# available pools for backup: zpool list - excludes 
ALLPOOLS=`${ZPOOL} list | ${TAIL} -n +2 | ${CUT} -d' ' -f1 | tr '\n' ' '` 
for item in ${BACKUP_POOLS//,/ }; do
	ALLPOOLS=`echo $ALLPOOLS | sed -e s/^"${item}"[^:alnum:.:-]//g -e s/[^:alnum:.:-]"${item}"[^:alnum:.:-]/\ /g -e s/[^:alnum:.:-]"${item}"$//g`
done
