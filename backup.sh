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

CURRENT_DATE=`${DATE} +"%Y%m%H%M"`

# backup pools
BACKUP_POOLS=(usbstick bla)

# property used to check if auto updates should be made or not
SNAPSHOT_PROPERTY_NAME="ch.espen:auto-backup"
SNAPSHOT_PROPERTY_VALUE="true"


# go through passed options and assign to variables
while getopts 'hvp' OPTION
do
	case $OPTION in
	h) 	# help goes here ... somehow 
		;;
	v) 	vflag=1
		;;
	p) 	pflag=1
		;;
	esac
done

function run_cmd() {
	if [ "$vflag" ]; then
		echo "$2"
	fi
	if [ "$pflag" ]; then
		echo "command: $1"
	else
		eval $1
		ret=$?
		if [[ $ret -eq 0 ]]
		then
			if [ "$vflag" ]; then
				echo "Successfully ran [ $1 ]"
			fi
		else
			if [ "$vflag" ]; then
				echo "Error: Command [ $1 ] returned $ret"
			fi
			return $ret
 	   	fi
	fi
}

# available pools for backup: zpool list - excludes 
ALLPOOLS=(`${ZPOOL} list | ${TAIL} -n +2 | ${CUT} -d' ' -f1 | tr '\n' ' '`); 


l2=" ${BACKUP_POOLS[*]} "                    # add framing blanks
for item in ${ALLPOOLS[@]}; do
  if [[ $l2 =~ " $item " ]] ; then    # use $item as regexp
    AVAILABLE_BACKUP_POOLS+=($item)
  fi
done

if [ "$vflag" ]; then
        echo "available backup-pools: ${AVAILABLE_BACKUP_POOLS[@]}"
fi

AVAILABLE_POOLS=(${ALLPOOLS[@]})

for backup_pool in "${BACKUP_POOLS[@]}"; do
	AVAILABLE_POOLS=( ${AVAILABLE_POOLS[@]/${backup_pool}/} )
done

	
if [ "$vflag" ]; then
	echo "available pools to backup: ${AVAILABLE_POOLS[@]}"
fi

# get a list of all available zfs filesystems by listing them and then look for property and take snapshots
for pool in ${AVAILABLE_POOLS}; do
	for fs in $(${ZFS} list -r ${pool} | ${TAIL} -n +2 | ${TR} -s " " | ${CUT} -f 1 -d ' ') ; do
        	# get state of auto-snapshot property, either true or false
	 		VALUE=`${ZFS} get ${SNAPSHOT_PROPERTY_NAME} $fs | ${TAIL} -n 1 | ${TR} -s ' ' | ${CUT} -f 3 -d ' '`	
			if [ $VALUE = $SNAPSHOT_PROPERTY_VALUE ]; then
				FILESYSTEMS+=($fs)
			fi
	done
done
 
if [ "$vflag" ]; then
        echo "backing up filesystems: ${FILESYSTEMS[@]}"
fi


containsElement () {
  local e
  for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
  return 1
}

for pool in ${AVAILABLE_BACKUP_POOLS}; do
	SNAPSHOT_LABEL=${pool}_${CURRENT_DATE}
	echo $SNAPSHOT_LABEL
	BACKUP_FILESYSTEMS=(`${ZFS} list -o name -r ${pool}`);
	
	for fs in ${FILESYSTEMS}; do
		# array of snapshots for current filesystem ordered by creation time
		SNAPSHOT_LIST=(`${ZFS} list -t snapshot -o name,creation -s creation -r ${fs} | cut -d' ' -f1 | ${TAIL} -n 1 | grep "${fs}@${pool}" | tr '\n' ' '`);
		if [ "$vflag" ]; then
  		      echo "snapshots for ${fs}: ${SNAPSHOT_LIST[@]}"
		fi

		# create backup fs on the fly
		PATH="";
		first=true
		for sub_path in ${fs//\// } ; do 
			if $first; then
				PATH="${sub_path}" 
			else
				PATH="$PATH/${sub_path}"
			fi

			if [ ! $(containsElement ${PATH} "${BACKUP_FILESYSTEMS[@]}") ];then
				run_cmd "${ZFS} create ${pool}/${PATH}" "creating filesystem ${pool}/${PATH}"
			fi
			first=false;
		done

		run_cmd "${ZFS} snapshot ${fs}@${SNAPSHOT_LABEL}" "creating snapshot ${fs}@${SNAPSHOT_LABEL}"

		if [ ${#SNAPSHOT_LIST[@]} = 0 ]; then
			run_cmd "${ZFS} send ${fs}@${SNAPSHOT_LABEL} | ${ZFS} recv ${pool}/${fs}" "sending initial snapshot ${fs}@${SNAPSHOT_LABEL} to ${pool}/${fs}"
		else
			for snapshot in ${SNAPSHOT_LIST}; do
				run_cmd "${ZFS} send -i ${snapshot} ${fs}@${SNAPSHOT_LABEL} | ${ZFS} recv ${pool}/${fs}" \
					 "sending incremental ${snapshot} snapshot ${fs}@${SNAPSHOT_LABEL} to ${pool}/${fs}"
				if [ "$?" = "0" ]; then
					break
				fi
			done
		fi
	done
done

