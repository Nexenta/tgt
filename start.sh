#!/bin/bash

#
# Defaults
#
# MaxRecvDataSegmentLength=8192
# MaxXmitDataSegmentLength=8192
# HeaderDigest=None
# DataDigest=None
# InitialR2T=Yes
# MaxOutstandingR2T=1
# ImmediateData=Yes
# FirstBurstLength=65536
# MaxBurstLength=262144
# DataPDUInOrder=Yes
# DataSequenceInOrder=Yes
# ErrorRecoveryLevel=0
# IFMarker=No
# OFMarker=No
# DefaultTime2Wait=2
# DefaultTime2Retain=20
# OFMarkInt=Reject
# IFMarkInt=Reject
# MaxConnections=1

BLOCK_SIZE=4096
LUN_START=0
LUN_COUNT=1
LUN_END=1
SKIP_TGT_CREATE=false
BS_TYPE="ccowbd"
DELETE=false

control_port=0
driver="iscsi"
next_tid=1
targetname="iqn.2005-11.com.nexenta:storage.disk${next_tid}.ccow.gateway2"
device_type="disk"
PARAMS="vendor_id=NEXENTA,product_id=NEXENTAEDGE,product_rev=0000,removable=1,sense_format=0,thin_provisioning=1"

# -----------------------------------------------------------------------------
# function: Usage
# -----------------------------------------------------------------------------
function Usage
{
	echo " "
	echo "NAME"
	echo " "
	echo "  ./start.sh [-h] [-l start_lun] [-n lun_count] [-b block_size] -s"
	echo "       [-B directory]"
	echo " "
	echo "OPTIONS"
	echo " "
	echo "  -h Display help and exit."
	echo " "
	echo "  -b Specify block size. (Defaults to 4096.)"
	echo " "
	echo "  -l Specify the LUN of the first logical unit."
	echo "     (Defaults to 0.)"
	echo " "
	echo "  -n Specify the number of logical units." 
	echo "     (Defaults to 1.)"
	echo " "
	echo "  -s Skip target creation."
	echo " "
	echo "  -B Enable backing store type \"rdrw\", using the specified"
	echo "     directories for the LUN data files."
	echo "     (defaults to backing store type \"ccowbd\".)"
	echo " "
	echo "  -D Delete LUNs"
	echo " "
}

# -----------------------------------------------------------------------------
# function: ExecCmd
# -----------------------------------------------------------------------------
function ExecCmd
{
	echo $CMD
	$CMD
	RV="$?"
	echo "rv = $RV"

	if [ ! "$RV" = "0" ]
	then
		if [ ! "$BS_TYPE" = "rdwr" ]
		then
			exit
		fi
	fi
}

# -----------------------------------------------------------------------------
# function: CreateTarget
# -----------------------------------------------------------------------------
function CreateTarget
{
	if [ ! "$SKIP_TGT_CREATE" = true ]
	then
		CMD="tgtadm --lld $driver --mode target --op new --tid $next_tid --targetname $targetname" 
		ExecCmd
	fi
}

# -----------------------------------------------------------------------------
# function: SetTargetOptions
# -----------------------------------------------------------------------------
function SetTargetOptions
{
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name HeaderDigest --value CRC32C"
	ExecCmd
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name DataDigest --value CRC32C"
	ExecCmd
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name MaxRecvDataSegmentLength --value 524288"
	ExecCmd
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name InitialR2T --value No"
	ExecCmd
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name FirstBurstLength --value 262144"
	ExecCmd
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name MaxBurstLength --value 16776192"
	ExecCmd
	# SJM tgtadm --lld $driver --mode target --op update --tid $next_tid --name MaxQueueCmd --value 256
	CMD="tgtadm --lld $driver --mode target --op update --tid $next_tid --name MaxQueueCmd --value 1"
	ExecCmd
}

# -----------------------------------------------------------------------------
# function: CreateLuns
# -----------------------------------------------------------------------------
function CreateLuns
{
	LUN_END=`expr $LUN_START + $LUN_COUNT - 1`

	for LUN in `seq $LUN_START $LUN_END`
	do
		if [ "$BS_TYPE" = "rdwr" ]
		then
			BACKING_STORE="$BS_DIR/$LUN"
			CMD="dd if=/dev/zero of=$BACKING_STORE bs=1M count=16"
			ExecCmd
		else
			BACKING_STORE="cltest/test/ccowbd/$LUN"
		fi

		CMD="tgtadm -C $control_port --lld $driver --op new --mode logicalunit "
		CMD="$CMD --tid $next_tid --lun $LUN -b $BACKING_STORE "
		CMD="$CMD --device-type $device_type --bstype $BS_TYPE --blocksize $BLOCK_SIZE"

		ExecCmd

		CMD="tgtadm -C $control_port --lld $driver --op update --mode logicalunit "
		CMD="$CMD --tid $next_tid --lun $LUN --params $PARAMS"

		ExecCmd
	done
}

# -----------------------------------------------------------------------------
# function: DeleteLuns
# -----------------------------------------------------------------------------
function DeleteLuns
{
	LUN_END=`expr $LUN_START + $LUN_COUNT - 1`

	for LUN in `seq $LUN_START $LUN_END`
	do
		CMD="tgtadm -C $control_port --lld $driver --op delete "
		CMD="$CMD --mode logicalunit --tid $next_tid --lun $LUN"

		ExecCmd
	done
}

# -----------------------------------------------------------------------------
# function: SetTargetAccess
# -----------------------------------------------------------------------------
function SetTargetAccess
{
	CMD="tgtadm --lld iscsi --op bind --mode target --tid 1 -I ALL"
	ExecCmd

	CMD="tgtadm --lld iscsi --op show --mode target"
	ExecCmd
}

# ----------------------------------------------------------------------------- 
# exection starts here:
# -----------------------------------------------------------------------------
echo "start.sh - v1.0"

while getopts hb:l:n:sB:D opt; do
	case $opt in 

	b)  BLOCK_SIZE=$OPTARG
	    ;;

	l)  LUN_START=$OPTARG
	    ;;

	n)  LUN_COUNT=$OPTARG
	    ;;

	s)  SKIP_TGT_CREATE=true
	    ;;

	B)  BS_TYPE="rdwr"
	    BS_DIR=$OPTARG
	    ;;

	D)  DELETE=true
	    ;;

	h)  Usage
	    exit 1
	    ;;

	\?) echo "Invalid option: -$OPTARG" >&2
	    exit 1
	    ;;

	esac
done

if [ "$DELETE" = true ]
then
	DeleteLuns
	exit
fi

CreateTarget
SetTargetOptions
CreateLuns
SetTargetAccess
