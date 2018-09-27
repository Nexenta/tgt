/*
 * Copyright (C) 2014 Nexenta Systems Inc
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <jemalloc/jemalloc.h>
#include <assert.h>
#include <iscsi/md5.h>
#include <time.h>

#include "ccow.h"
#include "list.h"
#include "util.h"
#include "queue.h"
#include "tgtd.h"
#include "scsi.h"
#include "bs_thread.h"
#include "spc.h"
#include "target.h"
#include "libccowvol.h"
#define BS_CCOWD_RLIMIT_NOFILE 65536

#define BS_CCOW_OP_CNT 512
#define BS_CCOW_WRITE_SAME_BLOCKS_MAX 16384
#define BS_CCOW_WSNZ 0
#define BS_CCOW_TRANSFER_SIZE_MAX (16 * 1024 * 1024)
#define BS_CCOW_TRANSFER_SIZE_OPTIMAL 0
#define BS_CCOW_UNMAP_SIZE_MAX (16 * 1024 * 1024)
#define BS_CCOW_UNMAP_DESCRIPTORS_MAX 1024
#define BS_CCOW_COMPARE_AND_WRITE_SIZE_MAX (64 * 1024)


#define BS_CCOW_LISTID_NUM 256
/* LID1 define 8 bit List Id so corresponding BS_CCOW_LISTID_NUM = 256 */
#define BS_CCOW_CSCD_DESCRIPTOR_COUNT_MAX 32
#define BS_CCOW_SEGMENT_DESCRIPTOR_COUNT_MAX 32
#define BS_CCOW_DESCRIPTOR_LIST_LENGTH_MAX (BS_CCOW_CSCD_DESCRIPTOR_COUNT_MAX * 32 +\
		BS_CCOW_SEGMENT_DESCRIPTOR_COUNT_MAX * 28)

#define XCOPY_IN_PROGRESS 0
#define XCOPY_COMPLETED 1
#define XCOPY_COMPLETED_WITH_ERRORS 2

#define RECEIVE_COPY_STATUS_VALIDITY_TIME 15

struct receive_copy_status {
	uint8_t copy_manager_status;
	uint16_t segments_processed;
	uint8_t transfer_count_units;
	uint32_t transfer_count;
	time_t	update_time;
};

extern void (* signal_tgtd) (int signum);

static QUEUE ci_que = QUEUE_INIT_STATIC(ci_que);

/* ccowbd_info is allocated just after the bs_thread_info */
#define BS_CCOWBD_I(lu)	((struct ccow_info *) \
				((char *)lu + \
				sizeof(struct scsi_lu) + \
				sizeof(struct bs_thread_info)) \
			)

#define BS_CCOWBD_N(_ci) ((struct scsi_lu *)					\
	((char *) _ci -								\
	 (sizeof(struct scsi_lu) + sizeof(struct bs_thread_info))))

#define TIMER_INTERVAL 15

pthread_mutex_t  timer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t   timer_cv = PTHREAD_COND_INITIALIZER;
pthread_t	 timer_thread;

static void
bs_ccowbd_signal(int signum)
{
	QUEUE *q, *qn, *ciq = &ci_que;

	QUEUE_FOREACH_SAFE(q, qn, ciq) {
		struct ccow_info *ci = QUEUE_DATA(q, struct ccow_info, ci_lnk);

		ccow_t tctx = ci->tctx;
		ccow_stats_t stats;

		ccow_get_stats(tctx, &stats);
		ccow_print_stats(stats);
	}
}

static void
set_medium_error(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_READ_ERROR;
}

static void
set_illegal_request(int *result, uint8_t *key, uint16_t *asc, uint16_t sense)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = ILLEGAL_REQUEST;
	*asc = sense;
}

static void
set_copy_aborted(int *result, uint8_t *key, uint16_t *asc, uint16_t sense)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = COPY_ABORTED;
	*asc = sense;
}


static struct scsi_lu *device_lookup(struct target *target, uint64_t lun)
{
	struct scsi_lu *lu;

	list_for_each_entry(lu, &target->device_list, device_siblings)
		if (lu->lun == lun)
			return lu;
	return NULL;
}
static int bs_ccowbd_read(struct ccow_info *ci, uint64_t offset,  char *buf,
		size_t length, struct scsi_cmd *cmd);

static int bs_ccowbd_write(struct ccow_info *ci, uint64_t offset, char *buf,
		size_t length, struct scsi_cmd *cmd);

void *
timer_func(void * arg)
{
	int err;
	struct timespec to;
	QUEUE *ciq = arg, *q, *qn;

	do {
		if (!ciq) {
			pthread_exit(NULL);
			return NULL;
		}
		pthread_mutex_lock(&timer_mutex);

		to.tv_sec = time(NULL) + TIMER_INTERVAL;
		to.tv_nsec = 0;

		err = pthread_cond_timedwait(&timer_cv, &timer_mutex, &to);
		if (err == ETIMEDOUT) {
			QUEUE_FOREACH_SAFE(q, qn, ciq) {
				struct ccow_info *ci = QUEUE_DATA(q, struct ccow_info, ci_lnk);

				/* only flush new version if LUN was trully idle */
				int need_flush = ci->writes > 0;
				if (need_flush) {
					dprintf("Timer finalize flush for ci %p\n", ci);
					struct ccow_aio aio;
					aio.ci = ci;
					pthread_rwlock_wrlock(&ci->user_lck);
					err = ccow_vol_synchronize(&aio);
					pthread_rwlock_unlock(&ci->user_lck);
					if (err) {
						eprintf("Timer finalize err=%d\n", err);
					}
					ci->writes = 0;
				} else
					dprintf("Timer finalize empty for ci %p\n", ci);
			}
		} else {
			break;
		}

		pthread_mutex_unlock(&timer_mutex);
	} while (1);

	pthread_mutex_unlock(&timer_mutex);

	pthread_exit(NULL);
}

static void
bs_ccowbd_extended_copy(struct scsi_cmd *cmd)
{
	assert(cmd->scb[0] == 0x83);

	int result = SAM_STAT_GOOD;
	uint8_t key;
	uint16_t asc;
	int err;

	uint i, l;
	uint8_t designator_length;
	uint8_t lid = 0;
	struct receive_copy_status * copy_status = cmd->it_nexus->xcopy_status;

	uint8_t * out_buf = scsi_get_out_buffer(cmd);

	if (cmd->scb_len < 16) { /* that is length of xcopy */
		set_illegal_request(&result, &key, &asc, ASC_PARAMETER_LIST_LENGTH_ERR);
		goto _exit;
	}

	if (cmd->out_sdb.length == 0)
		goto _exit;

	if (cmd->out_sdb.length < 16 + 32 || /* xcopy + 1 cscd */
		cmd->out_sdb.length > 16 + BS_CCOW_DESCRIPTOR_LIST_LENGTH_MAX ) {
		set_illegal_request(&result, &key, &asc, ASC_PARAMETER_LIST_LENGTH_ERR);
		goto _exit;
	}

	switch ((cmd->scb[1] & 0x0F)) {

	case 0x00:

		/* EXTENDED_COPY(LID1) */

		/* get List ID */
		lid = out_buf[0];
		/* get target descriptor list length*/
		l = get_unaligned_be16(&out_buf[2]);
		i = 16; /* offset of the first target descriptor */

		struct scsi_lu *src_lu = NULL;
		struct scsi_lu *dst_lu = NULL;
		uint src_blk_size = 0;
		uint dst_blk_size = 0;
		uint16_t src_dsc_index, dst_dsc_index;
		uint64_t src_offset, dst_offset;
		struct ccow_info *ci;
		size_t length;
		char *cp_buf;
		int descriptors_num = 0;

		if (!copy_status) {
		/* important, don't use je_malloc as it_nexus_destroy will use plain free */
			copy_status = calloc(BS_CCOW_LISTID_NUM,
									sizeof(struct receive_copy_status));
			if (!copy_status) {
				set_medium_error(&result, &key, &asc);
				goto _exit;
			}
			cmd->it_nexus->xcopy_status = copy_status;
		}

		copy_status[lid].copy_manager_status = XCOPY_IN_PROGRESS;
		copy_status[lid].segments_processed = 0;
		copy_status[lid].transfer_count_units = 0;
		copy_status[lid].transfer_count = 0;
		copy_status[lid].update_time = time(NULL);

		/* check target descriptor list */

		while (l > 0) {
			if (out_buf[i] == 0xE4) {
				if (l < 32) {
					set_illegal_request(&result, &key, &asc,
								ASC_PARAMETER_LIST_LENGTH_ERR);
					goto _exit;
				}
				/* some checks special for libiscsi test cu */
				if (out_buf[i + 1] & 0x1 << 5) {
					/* check for SPC-3 - nul */
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				if (out_buf[i + 1] & 0x3 << 6) {
					/* SPC-3 - LU ID is reserved */
					set_illegal_request(&result, &key, &asc,
								ASC_INVALID_FIELD_IN_CDB);
					goto _exit;
				}
				/* just count descriptors*/
				descriptors_num++;
				l -= 32;
				i += 32;
			}
			else {
				/* we support only identification target descriptors */
				set_illegal_request(&result, &key, &asc,
							ASC_UNSUPPORTED_TARGET_DESCRIPTOR_TYPE);
				goto _exit;
			}
		}

		if (descriptors_num >= BS_CCOW_CSCD_DESCRIPTOR_COUNT_MAX) {
			set_illegal_request(&result, &key, &asc,
					ASC_TOO_MANY_TARGET_DESCRIPTORS);
			goto _exit;
		}

		l = get_unaligned_be32(&out_buf[8]); /*segment descriptor list length*/
		i = 16 + get_unaligned_be16(&out_buf[2]); /*first segment descriptor */
		/* decode segment descriptor list */
		while (l > 0) {
			switch (out_buf[i]) {
			/*block to block*/
			case 0x02:
				if (l < 28 || cmd->out_sdb.length < 16 + 32 + 28) {
					set_illegal_request(&result, &key, &asc,
								ASC_PARAMETER_LIST_LENGTH_ERR);
					goto _exit;
				}
				if (l / 28 >= BS_CCOW_SEGMENT_DESCRIPTOR_COUNT_MAX) {
					set_illegal_request(&result, &key, &asc,
							ASC_TOO_MANY_TARGET_DESCRIPTORS);
					goto _exit;
				}
				src_dsc_index = get_unaligned_be16(&out_buf[i + 4]);
				if (src_dsc_index >= descriptors_num) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				dst_dsc_index = get_unaligned_be16(&out_buf[i + 6]);
				if (dst_dsc_index >= descriptors_num) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}

				/* decode target descriptors */
				/* source lu */
				designator_length = out_buf[16 + src_dsc_index * 32 + 7];
				/*
				 * We should inquire VPDs of all LU, build designators from them
				 * and search there LU that match with received designator field,
				 * code set, association and designator type fields.
				 * However we use just the last byte of designator to identify LUN.
				 */
				uint8_t lun = out_buf[16 + src_dsc_index * 32 + 8 + designator_length - 1];
				src_lu = device_lookup(cmd->c_target, lun);
				if (!src_lu) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				src_blk_size = get_unaligned_be24(&out_buf[16 + src_dsc_index * 32 + 29]);
				if (src_blk_size != 1 << src_lu->blk_shift) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				/* destination lu */
				designator_length = out_buf[16 + dst_dsc_index * 32 + 7];
				lun = out_buf[16 + dst_dsc_index * 32 + 8 + designator_length - 1];
				dst_lu = device_lookup(cmd->c_target, lun);
				if (!dst_lu) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				dst_blk_size = get_unaligned_be24(&out_buf[16 + dst_dsc_index * 32 + 29]);
				if (dst_blk_size != 1 << dst_lu->blk_shift) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}

				if (out_buf[i + 1] & 0x2) { /* DC bit */
					length = dst_blk_size * get_unaligned_be16(&out_buf[i + 10]);
				}
				else {
					length = src_blk_size * get_unaligned_be16(&out_buf[i + 10]);
				}
				src_offset = src_blk_size * get_unaligned_be64(&out_buf[i + 12]);
				dst_offset = dst_blk_size * get_unaligned_be64(&out_buf[i + 20]);
				if (src_offset + length > src_lu->size
						|| dst_offset + length > dst_lu->size) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}
				cp_buf = je_malloc(length);
				if (cp_buf == NULL) {
					set_copy_aborted(&result, &key, &asc, 0);
					goto _exit;
				}

				copy_status[lid].segments_processed++;

				ci = BS_CCOWBD_I(src_lu);
				err = bs_ccowbd_read(ci, src_offset, cp_buf, length, NULL);
				if (err) {
					set_copy_aborted(&result, &key, &asc, 0);
					je_free(cp_buf);
					goto _exit;
				}
				ci = BS_CCOWBD_I(dst_lu);
				err = bs_ccowbd_write(ci, dst_offset, cp_buf, length, NULL);
				if (err) {
					set_copy_aborted(&result, &key, &asc, 0);
					je_free(cp_buf);
					goto _exit;
				}
				copy_status[lid].transfer_count += length;

				je_free(cp_buf);
				l -= 28;
				i += 28;
				break;
			default:
				set_illegal_request(&result, &key, &asc,
							ASC_UNSUPPORTED_TARGET_DESCRIPTOR_TYPE);
				goto _exit;
				break;
			}
		}
		copy_status[lid].update_time = time(NULL);
		copy_status[lid].copy_manager_status = XCOPY_COMPLETED;
		break;

	default:
		set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
		scsi_set_result(cmd, result);
		sense_data_build(cmd, key, asc);
		return;
	}

_exit:
	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		sense_data_build(cmd, key, asc);
		if (copy_status) {
			copy_status[lid].update_time = time(NULL);
			copy_status[lid].copy_manager_status = XCOPY_COMPLETED_WITH_ERRORS;
		}
	}
}

static void
bs_ccowbd_receive_copy(struct scsi_cmd *cmd)
{
	assert(cmd->scb[0] == 0x84);

	int result = SAM_STAT_GOOD;
	uint8_t key;
	uint16_t asc;

	char * in_buf = scsi_get_in_buffer(cmd);
	size_t in_len = scsi_get_in_length(cmd);
	uint8_t lid;

	switch ((cmd->scb[1] & 0x1F)) {

	case 0x00:  /* RECEIVE COPY STATUS(LID1) */

		lid = cmd->scb[2];
		struct receive_copy_status * copy_status = cmd->it_nexus->xcopy_status;

		if (!copy_status) {
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			goto _exit;
		}
		if (time(NULL) - copy_status[lid].update_time > RECEIVE_COPY_STATUS_VALIDITY_TIME) {
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			goto _exit;
		}
		put_unaligned_be32(0x8, &in_buf[0]); // available data
		in_buf[4] = copy_status[lid].copy_manager_status;
		put_unaligned_be16(copy_status[lid].segments_processed, &in_buf[5]);
		in_buf[7] = copy_status[lid].transfer_count_units;
		put_unaligned_be32(copy_status[lid].transfer_count, &in_buf[8]);
		break;

	case 0x03:  /* RECEIVE_COPY_OPERATING_PARAMETERS */
		memset(in_buf, 0, in_len);

		/* available data */
		put_unaligned_be32((256 - 4), &in_buf[0]);

		/* enable SNLID bit to support EXTENDED_COPY */
		in_buf[4] = 0x1;

		/* maximum cscd descriptor count */
		put_unaligned_be16(BS_CCOW_CSCD_DESCRIPTOR_COUNT_MAX, &in_buf[8]);

		/* maximum segment descriptor count */
		put_unaligned_be16(BS_CCOW_SEGMENT_DESCRIPTOR_COUNT_MAX, &in_buf[10]);

		/* maximum descriptor list length */
		put_unaligned_be32(BS_CCOW_DESCRIPTOR_LIST_LENGTH_MAX, &in_buf[12]);

		/* maximum segment length */
		put_unaligned_be32(BS_CCOW_TRANSFER_SIZE_MAX, &in_buf[16]);

		/* inline data length */
		put_unaligned_be32(0, &in_buf[20]);

		/* held data limit */
		put_unaligned_be32(0, &in_buf[24]);

		/* maximum stream device transfer size */
		put_unaligned_be32(0, &in_buf[28]);

		/* total concurrent copies */
		put_unaligned_be16(0, &in_buf[34]);

		/* maximum concurrent copies */
		in_buf[36] = 1;

		/* data segment granularity */
		in_buf[37] = 9;

		/* inline data granularity */
		in_buf[38] = 0;

		/* held data granularity */
		in_buf[39] = 9;

		/* implemented descriptor list length */
		in_buf[43] = 2;

		/* implemented descriptor list */
		in_buf[44] = 0x02;	// copy block to block device
		in_buf[45] = 0xE4;	// Identification Descriptor CSCD descriptor

		break;

	default:
		result = SAM_STAT_CHECK_CONDITION;
		key = ILLEGAL_REQUEST;
		asc = ASC_INVALID_OP_CODE;
		goto _exit;
	}

_exit:
	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		sense_data_build(cmd, key, asc);
	}
}

/**
 *
 *
 **/
static inline void
bs_ccowbd_init_aio(struct ccow_aio * aio, struct ccow_info *ci, uint64_t offset,  char *buf,
	size_t length, struct scsi_cmd *cmd)
{
	if (aio != NULL) {
		memset(aio, 0, sizeof(struct ccow_aio));
		aio->aio_offset = offset;
		aio->aio_buf = buf;
		aio->aio_nbytes = length;
		aio->aio_cbfunction = NULL;
		aio->aio_cbargs = aio;
		aio->aio_sigevent = CCOW_VOL_SIGEV_NONE;
		aio->ci = ci;
	}
}

static int
bs_ccowbd_finish_aio(ccow_aio_t aio)
{
	int err = 0;
	ssize_t nbytes = 0;

	err = ccow_vol_error(aio);
	if (err == 0) {
		nbytes = ccow_vol_return(aio);
		if (nbytes < 0) {
			err = nbytes;
		} else if ((size_t)nbytes == aio->aio_nbytes) {
			err = 0;
		} else {
			err = EIO;
			goto rb_exit;
		}
	} else if (err == EINPROGRESS) {
		err = -EINPROGRESS;
	}

	while (err == -EINPROGRESS) {
		err = ccow_vol_suspend(&aio, 1, NULL);
		if (err != 0)
			goto rb_exit;
		nbytes = ccow_vol_return(aio);
		if (nbytes < 0) {
			err = nbytes;
		} else if ((size_t)nbytes == aio->aio_nbytes) {
			err = 0;
			break;
		} else {
			err = EIO;
			goto rb_exit;
		}
	}

rb_exit:
	return err;
}

static int
bs_ccowbd_read(struct ccow_info *ci, uint64_t offset,  char *buf, size_t length,
	    struct scsi_cmd *cmd)
{
	int err;
	struct ccow_aio aio;

	bs_ccowbd_init_aio(&aio, ci, offset, buf, length, cmd);

	err = ccow_vol_read(&aio);

	if (err != 0)
		return err;

	err = bs_ccowbd_finish_aio(&aio);

	return err;
}

static int
bs_ccowbd_write(struct ccow_info *ci, uint64_t offset,  char *buf, size_t length,
	    struct scsi_cmd *cmd)
{
	int err;
	struct ccow_aio aio;

	bs_ccowbd_init_aio(&aio, ci, offset, buf, length, cmd);

	err = ccow_vol_write(&aio);
	if (err != 0)
		return err;

	err = bs_ccowbd_finish_aio(&aio);

	return err;
}

static int
bs_ccowbd_unmap(struct ccow_info *ci, uint64_t offset, size_t length,
	    struct scsi_cmd *cmd)
{
	int err;
	struct ccow_aio aio;

	bs_ccowbd_init_aio(&aio, ci, offset, NULL, length, cmd);

	err = ccow_vol_unmap(&aio);
	if (err != 0)
		return err;

	err = bs_ccowbd_finish_aio(&aio);

	return err;
}

static int
bs_ccowbd_write_same(struct ccow_info *ci, uint64_t offset,  char *buf,
	size_t in_size, struct scsi_cmd *cmd, size_t length)
{
	int err;
	struct ccow_aio aio;

	bs_ccowbd_init_aio(&aio, ci, offset, buf, length, cmd);
	aio.aio_in_size = in_size;

	err = ccow_vol_write_same(&aio);

	if (err != 0)
		return err;

	err = bs_ccowbd_finish_aio(&aio);

	return err;
}

static void
bs_ccowbd_request(struct scsi_cmd *cmd)
{
	struct scsi_lu *lu = cmd->dev;
	uint64_t offset = cmd->offset;
	struct ccow_info *ci = BS_CCOWBD_I(lu);
	struct iovec *iov = NULL;
	char *buf = NULL;
	size_t length = 0;
	int err = 0;
	int result = SAM_STAT_GOOD;
	uint8_t key;
	uint16_t asc;
	uint32_t info = 0;
	int info_valid = 0;
	int err_finalize = 0;

	if (ci->stream_broken) {
		result = SAM_STAT_CHECK_CONDITION;
		key = HARDWARE_ERROR;
		asc = ASC_INTERNAL_TGT_FAILURE;
		pthread_rwlock_rdlock(&ci->user_lck);
		goto _exit;
	}

	switch (cmd->scb[0])
	{
	case EXTENDED_COPY:
		dprintf("EXTENDED_COPY\n");
		pthread_rwlock_wrlock(&ci->user_lck);
		bs_ccowbd_extended_copy(cmd);
		pthread_rwlock_unlock(&ci->user_lck);
		return;
		break;

	case RECEIVE_COPY:
		dprintf("RECEIVE_COPY\n");
		bs_ccowbd_receive_copy(cmd);
		return;
		break;

	default:
		pthread_rwlock_rdlock(&ci->user_lck);
		break;
	};

	dprintf("chunk_size = 0x%x : blk_size = 0x%x \n",
	    ci->chunk_size, ci->blk_size);

	switch (cmd->scb[0])
	{
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
		{
		dprintf("SYNCHRONIZE_CACHE\n");
		struct ccow_aio aio;
		aio.ci = ci;
		err = ccow_vol_synchronize(&aio);

		if (err) {
			set_medium_error(&result, &key, &asc);
			break;
		}

		dprintf("SYNCHRONIZE_CACHE done\n");
		break;
		}
	case COMPARE_AND_WRITE:
		/* Blocks are transferred twice, first the set that
		 * we compare to the existing data, and second the set
		 * to write if the compare was successful.
		 */
		length = scsi_get_out_length(cmd) / 2;

		dprintf("COMPARE_AND_WRITE : length = %zu (0x%zx) : "
		    "offset = %"PRId64" (0x%"PRIx64") \n",
		    length, length, offset, offset);

		if (cmd->tl != length) {
			dprintf("COMPARE_AND_WRITE : transfer length %d is not equal to "
					"data length %lu \n", cmd->tl, length);

			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		if (cmd->tl > BS_CCOW_COMPARE_AND_WRITE_SIZE_MAX) {
			dprintf("COMPARE_AND_WRITE : transfer length %d is greater than max %d \n",
					cmd->tl, BS_CCOW_COMPARE_AND_WRITE_SIZE_MAX);

			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		char * tmpbuf = je_malloc(length);
		if (tmpbuf == NULL) {
			eprintf("memory allocation of COMPARE_AND_WRITE buffer failed."
			    "(length = %zu)\n", length);
			set_medium_error(&result, &key, &asc);
			break;
		}

		err = bs_ccowbd_read(ci, offset, tmpbuf, length, cmd);

		dprintf("COMPARE_AND_WRITE : bs_ccowbd_read err = %d \n", err);

		if (err) {
			set_medium_error(&result, &key, &asc);
			je_free(tmpbuf);
			err_finalize = err;
			break;
		}

		if (memcmp(scsi_get_out_buffer(cmd), tmpbuf, length)) {
			size_t pos = 0;
			char *spos = scsi_get_out_buffer(cmd);
			char *dpos = tmpbuf;
			/*
			 * Data differed, this is assumed to be 'rare'
			 * so use a much more expensive byte-by-byte
			 * comparasion to find out at which offset the
			 * data differs.
			 */
			for (pos = 0; pos < length && *spos++ == *dpos++;
			     pos++)
				;
			info = pos;
			info_valid = 1;
			result = SAM_STAT_CHECK_CONDITION;
			key = MISCOMPARE;
			asc = ASC_MISCOMPARE_DURING_VERIFY_OPERATION;
			je_free(tmpbuf);
			break;
		}

		je_free(tmpbuf);

		buf = scsi_get_out_buffer(cmd) + length;
		goto _write;

	case WRITE_SAME:
	case WRITE_SAME_16:

		length = cmd->tl;

		/*
		 * length is the length of all blocks that should be written to storage.
		 * len_out is the length of the pattern that should be written
		 * count times to fill all blocks
		 */
		char * buf_out = scsi_get_out_buffer(cmd);
		size_t len_out = scsi_get_out_length(cmd);
		int unmap = 0;

		dprintf("WRITE_SAME : length = %zu (0x%zx) : "
		    "offset = %"PRId64" (0x%"PRIx64") \n",
		    length, length, offset, offset);

		if (cmd->scb[1] & 0x10) { /*anchoring isn't supported */
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			break;
		}

		if (length == 0 || len_out == 0) {
			if (BS_CCOW_WSNZ || !(cmd->scb[1] & 0x08)) {
				set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
				break;
			}
		}

		if (cmd->scb[1] & 0x08) { /* check unmap bit*/
			len_out = 1 << lu->blk_shift;
			buf_out = je_calloc(1, len_out);
			if (!buf_out) {
				set_medium_error(&result, &key, &asc);
				break;
			}
			unmap = 1;
		}

		if (length % len_out != 0) {
			eprintf("invalid length parameters (length = %zu : "
			    "len_out = %zu)\n", length, len_out);
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			break;
		}
		if (length / len_out > BS_CCOW_WRITE_SAME_BLOCKS_MAX) {
			eprintf("too many blocks %zu, BS_CCOW_WRITE_SAME_BLOCKS_MAX %u)\n",
					length / len_out, BS_CCOW_WRITE_SAME_BLOCKS_MAX);
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			break;
		}
		if (len_out & (len_out - 1) ) {
			eprintf("invalid len_out parameters, it should be power of 2 "
					"(length = %zu : len_out = %zu)\n", length, len_out);
			set_illegal_request(&result, &key, &asc, ASC_INVALID_FIELD_IN_CDB);
			break;
		}

		err = bs_ccowbd_write_same(ci, offset, buf_out, len_out, cmd, length);

		if (unmap)
			je_free(buf_out);

		if (err != 0) {
			eprintf("write same error. err = %d\n", err);
			set_medium_error(&result, &key, &asc);
			break;
		}

		break;


	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:

		buf = scsi_get_out_buffer(cmd);
		length = scsi_get_out_length(cmd);

		dprintf("WRITE : length = %zu (0x%zx) : "
		    "offset = %"PRId64" (0x%"PRIx64") \n",
		    length, length, offset, offset);
_write:
		err = bs_ccowbd_write(ci, offset, buf, length, cmd);

		dprintf("WRITE done\n");

		if (err != 0) {
			eprintf("Put error on I/O submit: %d\n", err);
			set_medium_error(&result, &key, &asc);
			err_finalize = err;
			goto _exit;
		}

		/*
		 * check FUA bit in SCB. if set, check WCE flag in caching mode
		 * page to see if write back cache is enabled.
		 */
		struct mode_pg *pg;
		pg = find_mode_page(cmd->dev, 0x08, 0);
		if (pg == NULL) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		if ((cmd->scb[0] != WRITE_6) && (cmd->scb[1] & 0x8) &&
		    (pg->mode_data[0] & 0x04)) {
			dprintf("WRITE_6 SYNC\n");
			struct ccow_aio aio;
			aio.ci = ci;
			err = ccow_vol_synchronize(&aio);
			if (err) {
				result = SAM_STAT_CHECK_CONDITION;
				key = NOT_READY;
				asc = ASC_MANUAL_INTERVENTION_REQ;
				eprintf("ccow stream failed to synchronize, err %d"
						" switching offline", err);
				lu->dev_type_template.lu_offline(lu);
				break;
			}
		}
		break;

	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:

		buf = scsi_get_in_buffer(cmd);
		length = scsi_get_in_length(cmd);

		if (length == 0)
			break;
		int read_retry_cnt = 0;

_retry_read:
		dprintf("READ : length = %zu (0x%zx) : "
		    "offset = %"PRId64" (0x%"PRIx64") \n",
		    length, length, offset, offset);

		err = bs_ccowbd_read(ci, offset, buf, length, cmd);

		dprintf("READ done\n");

		if (err != 0) {
			if (err == -EAGAIN && read_retry_cnt++ < 10) {
				usleep(50000);
				goto _retry_read;
			}
			eprintf("Get error on I/O submit (after %d retries): %d\n",
			    read_retry_cnt, err);
			set_medium_error(&result, &key, &asc);
			err_finalize = err;
			goto _exit;
		}

		break;

	case UNMAP:
		if (!cmd->dev->attrs.thinprovisioning) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}
		size_t pl_length = scsi_get_out_length(cmd);
		char * pl_buf = scsi_get_out_buffer(cmd);

		if (pl_length < 8)
			break;

		pl_length -= 8;
		pl_buf += 8;

		while (pl_length >= 16) {
			offset = get_unaligned_be64(&pl_buf[0]);
			offset <<= cmd->dev->blk_shift;

			length = get_unaligned_be32(&pl_buf[8]);
			length <<= cmd->dev->blk_shift;

			if (offset + length > cmd->dev->size) {
				eprintf("UNMAP beyond end of storage\n");
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_LBA_OUT_OF_RANGE;
				goto _exit;
			}
			if (length > BS_CCOW_UNMAP_SIZE_MAX) {
				eprintf("unmap length %zu too big, BS_CCOW_UNMAP_SIZE_MAX %u \n",
					length, BS_CCOW_UNMAP_SIZE_MAX);
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_INVALID_FIELD_IN_CDB;
				goto _exit;
			}
			dprintf("UNMAP : length = %zu (0x%zx) : "
					"offset = %"PRId64" (0x%"PRIx64") \n",
					length, length, offset, offset);

			err = bs_ccowbd_unmap(ci, offset, length, cmd);

			if (err) {
				eprintf("write bs_ccowbd_unmap returned error %d\n", err);
				set_medium_error(&result, &key, &asc);
				goto _exit;
			}
			pl_length -= 16;
			pl_buf += 16;
		}
		break;

	default:
		eprintf("Unsupported cmd on I/O submit: %x\n", cmd->scb[0]);
		result = SAM_STAT_CHECK_CONDITION;
		key = ILLEGAL_REQUEST;
		asc = ASC_INVALID_OP_CODE;
		goto _exit;
	}


_exit:
	if (err == -ENOSPC) {
		eprintf("LUN is out of space, switching read only, err = %d \n", err);
		lu->attrs.swp = 1;
	}

	if (err_finalize) {
		dprintf("ERR SYNC\n");
		struct ccow_aio aio;
		aio.ci = ci;
		err = ccow_vol_synchronize(&aio);
		if (err != 0) {
			eprintf("error synchronizing volume. err = %d \n", err);
		}
	}

	pthread_rwlock_unlock(&ci->user_lck);

	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		if (info_valid)
			sense_info_data_build(cmd, key, asc, info);
		else
			sense_data_build(cmd, key, asc);
	}

	if (iov)
		je_free(iov);
}


static void bs_ccowbd_update_block_limits_vpd(struct scsi_lu *lu, void *data)
{
	struct vpd *vpd_pg = lu->attrs.lu_vpd[PCODE_OFFSET(0xb0)];
	unsigned block_size = 1 << lu->blk_shift;
	struct ccow_info *ci = BS_CCOWBD_I(lu);
	size_t chunk_size = ci->chunk_size;

	/* define WSNZ (write same non zero length option  */
	vpd_pg->data[0] = BS_CCOW_WSNZ & 0x1;

	/* maximum compare and write length */
	vpd_pg->data[1] = BS_CCOW_COMPARE_AND_WRITE_SIZE_MAX / block_size;

	/* Optimal transfer length granularity (in blocks) */
	put_unaligned_be16(chunk_size / block_size, vpd_pg->data + 2);

	/* Maximum transfer length	(in blocks) */
	put_unaligned_be32(BS_CCOW_TRANSFER_SIZE_MAX / block_size, vpd_pg->data + 4);

	/* Optimal transfer length (in blocks) 0 - not specified*/
	put_unaligned_be32(BS_CCOW_TRANSFER_SIZE_OPTIMAL / block_size, vpd_pg->data + 8);

	/* Maximum prefetch length */
	/* put_unaligned_be32(0, vpd_pg->data + 12); Pre-fetch isn't supported */

	if (lu->attrs.thinprovisioning) {
		/* maximum unmap lba count */
		put_unaligned_be32(BS_CCOW_UNMAP_SIZE_MAX / block_size, vpd_pg->data + 16);

		/* maximum unmap block descriptor count */
		put_unaligned_be32(BS_CCOW_UNMAP_DESCRIPTORS_MAX, vpd_pg->data + 20);

		/* Optimal unmap granularity */
		put_unaligned_be32(chunk_size / block_size, vpd_pg->data + 24);

		/* Unmap granularity alignment (set valid bit, alignment 0) */
		put_unaligned_be32(1U << 31, vpd_pg->data + 28);

	} else {
		put_unaligned_be32(0, vpd_pg->data + 16);
		put_unaligned_be32(0, vpd_pg->data + 20);
	}

	/* Maximum write same length (blocks) */
	put_unaligned_be64(BS_CCOW_WRITE_SAME_BLOCKS_MAX, vpd_pg->data + 32);
}


static tgtadm_err bs_ccowbd_init(struct scsi_lu *lu, char *bsopts)
{
	int err;
	struct bs_thread_info *info = BS_THREAD_I(lu);
	struct ccow_info *ci = BS_CCOWBD_I(lu);

	static int inited = 0;

	if (!inited) {
		inited = 1;
#ifdef CCOW_VALGRIND
		if (!RUNNING_ON_VALGRIND) {
#endif
			struct rlimit limit;
			limit.rlim_cur = BS_CCOWD_RLIMIT_NOFILE;
			limit.rlim_max = BS_CCOWD_RLIMIT_NOFILE;
			if (setrlimit(RLIMIT_NOFILE, &limit) != 0) {
				eprintf("setrlimit() failed with err=%d\n", -errno);
				return -1;
			}
#ifdef CCOW_VALGRIND
		}
#endif
	}

	memset(ci, 0, sizeof(*ci));
	QUEUE_INIT(&ci->ci_lnk);

	pthread_mutex_lock(&timer_mutex);
	if (QUEUE_EMPTY(&ci_que)) {
		err = pthread_create(&timer_thread, NULL, timer_func, &ci_que);
		if (err != 0) {
			int e = errno;
			pthread_mutex_unlock(&timer_mutex);
			eprintf("pthread_create returned err=%d\n", e);
			eprintf("\"%s\".\n", strerror(e));
			return -1;
		}
	}
	pthread_mutex_unlock(&timer_mutex);

	err = bs_thread_open(info, bs_ccowbd_request, nr_iothreads);
	if (err != 0) {
		eprintf("bs_thread_open returned error %d\n", err);
		pthread_mutex_lock(&timer_mutex);
		if (QUEUE_EMPTY(&ci_que)) {
			pthread_mutex_unlock(&timer_mutex);
			pthread_cond_signal(&timer_cv);
			pthread_join(timer_thread, NULL);
			pthread_mutex_lock(&timer_mutex);
		}
		pthread_mutex_unlock(&timer_mutex);
	}

	return err;
}

static void bs_ccowbd_exit(struct scsi_lu *lu)
{
	pthread_mutex_lock(&timer_mutex);
	if (QUEUE_EMPTY(&ci_que)) {
		pthread_mutex_unlock(&timer_mutex);
		pthread_cond_signal(&timer_cv);
		pthread_join(timer_thread, NULL);
		pthread_mutex_lock(&timer_mutex);
	}
	pthread_mutex_unlock(&timer_mutex);

	return;
}

static int bs_ccowbd_open(struct scsi_lu *lu, char *uri, int *fd,
			  uint64_t *size)
{
	int err;
	struct ccow_info *ci = BS_CCOWBD_I(lu);

	err = ccow_vol_open(ci, uri, fd, size);

	if (err != 0) {
		eprintf("failed to open volume. err = %d\n", err);
		goto _rm_evt_exit;
	}

	if (signal_tgtd == NULL)
		signal_tgtd = bs_ccowbd_signal;

	/*
	 * Update callback for Block Limits VPD page according to ccow attributes
	 * this overwrites default b0 VPD page set at spc_lu_init
	 */

	lu->attrs.lu_vpd[PCODE_OFFSET(0xb0)]->vpd_update = bs_ccowbd_update_block_limits_vpd;

	signal(SIGPIPE, SIG_IGN);

	/* set up SIGHUP handler */
	ccow_hup_lg();

	pthread_mutex_lock(&timer_mutex);
	QUEUE_INSERT_TAIL(&ci_que, &ci->ci_lnk);
	pthread_mutex_unlock(&timer_mutex);

	return 0;

_rm_evt_exit:
	return -1;
}

static void bs_ccowbd_close(struct scsi_lu *lu)
{
	struct ccow_info *ci = BS_CCOWBD_I(lu);
	struct bs_thread_info *info = BS_THREAD_I(lu);

	pthread_mutex_lock(&timer_mutex);

	ccow_vol_close(ci);

	QUEUE_REMOVE(&ci->ci_lnk);
	pthread_mutex_unlock(&timer_mutex);

	bs_thread_close(info);
}

static struct backingstore_template ccowbd_bst = {
	.bs_name = "ccowbd",
	.bs_datasize = sizeof(struct bs_thread_info) + sizeof(struct ccow_info),
	.bs_init = bs_ccowbd_init,
	.bs_exit = bs_ccowbd_exit,
	.bs_open = bs_ccowbd_open,
	.bs_close = bs_ccowbd_close,
	.bs_cmd_submit = bs_thread_cmd_submit
};

void register_bs_module(void)
{
	register_backingstore_template(&ccowbd_bst);
}
