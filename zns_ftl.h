// SPDX-License-Identifier: GPL-2.0-only

#ifndef _NVMEVIRT_ZNS_FTL_H
#define _NVMEVIRT_ZNS_FTL_H

#include <linux/types.h>
#include "nvmev.h"
#include "nvme_zns.h"

#define NVMEV_ZNS_DEBUG(string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_DEBUG(string,                                                                \
							args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_CONV_RW_DEBUG(                                                               \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_RW_DEBUG(string,                                                             \
							   args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_RW_DEBUG_VERBOSE(                                                            \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_L2P_DEBUG(                                                                   \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_CONV_MAPPING_DEBUG(                                                          \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_MAPPING_DEBUG(                                                               \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_L2P_DEBUG_VERBOSE(                                                           \
	string, args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_GC_DEBUG(string, args...)                                                    \
	printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_GC_DEBUG_VERBOSE(string, args...)                                            \
	printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_CONZONE_PRINT_BW(string,                                                             \
							   args...) // printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)

enum {
	SUCCESS = 0,
	FAILURE = 1,
};

enum {
	LOC_NORMAL = 0,
	LOC_PSLC = 1,
};

// Zoned Namespace Command Set Specification Revision 1.1a
struct znsparams {
	uint32_t nr_zones;
	uint32_t nr_active_zones;
	uint32_t nr_open_zones;
	uint32_t dies_per_zone;
	uint32_t zone_size; // bytes
	uint32_t zone_wb_size;

	/*related to zrwa*/
	uint32_t nr_zrwa_zones;
	uint32_t zrwafg_size;
	uint32_t zrwa_size;
	uint32_t zrwa_buffer_size;
	uint32_t lbas_per_zrwafg;
	uint32_t lbas_per_zrwa;

	int ns_type;
	struct nvmev_ns *ns;
	uint64_t logical_size;
	uint64_t physical_size;
	uint32_t nr_wb; // # of write buffer
	unsigned long tt_lines;
	unsigned long pslc_lines;
	unsigned long pgs_per_line;
	unsigned long pslc_pgs_per_line;

	// main area
	uint32_t zone_capacity; // NOT defined in ZAC/ZBC commands
	uint32_t chunk_size;	// bytes
	uint32_t pgs_per_chunk;
	uint32_t pgs_per_zone;

	int pba_pcent; // (physical space / logical space) * 100

	// l2p
	uint64_t tt_lpns;
	int pre_read; // the size of pre-read window

	// pSLC
	int pslc_blks; // # of blocks in each chip that configured as pSLC

	// GC
	uint64_t tt_ppns;
	uint32_t gc_thres_lines_high;
	uint32_t migrate_thres_lines_low;
	bool enable_gc_delay;
};

struct zone_resource_info {
	__u32 acquired_cnt;
	__u32 total_cnt;
};

struct zns_ftl {
	struct ssd *ssd;

	struct znsparams zp;
	struct zone_resource_info res_infos[RES_TYPE_COUNT];
	struct zone_descriptor *zone_descs;
	struct zone_report *report_buffer;
	struct buffer *zone_write_buffer;
	struct buffer *zrwa_buffer;
	void *storage_base_addr;
};

struct migrating_lineid {
	int parent_id;
	int id;
	uint64_t write_order; /*write order of this line*/
	struct list_head entry;
	/* position in the priority queue for migrating lines */
	size_t pos;
};

struct zms_line {
	int id;	 /* line id, the same as corresponding block id if interleave */
	int ipc; /* invalid page count in this line */
	int vpc; /* valid page count in this line */
	int rpc; /* reserved page count in this line*/
	struct list_head entry;
	/* position in the priority queue for victim lines */
	size_t pos;
	int blkid; /*the block id*/
	int type;  /*User or Internal*/
	struct migrating_lineid mid;
	// max 2 level lines
	int parent_id;
	struct zms_line *sub_lines;

	unsigned long pgs_per_line;
	// for rsv multiple lines
	struct zms_line *rsv_nextline;
};

/* wp: record next write addr */
struct zms_write_pointer {
	struct zms_line *curline;
	bool loc;
	uint32_t ch;
	uint32_t lun;
	uint32_t pl;
	uint32_t blk;
	uint32_t pg;
};

struct zms_line_mgmt {
	struct zms_line *lines;

	/* free line list, we only need to maintain a list of blk numbers */
	struct list_head free_line_list;
	pqueue_t *victim_line_pq;
	struct list_head full_line_list;

	uint32_t tt_lines;
	uint32_t free_line_cnt;
	uint32_t victim_line_cnt;
	uint32_t full_line_cnt;

	struct list_head pslc_free_line_list;
	pqueue_t *pslc_victim_line_pq;
	struct list_head pslc_full_line_list;

	uint32_t pslc_tt_lines;
	uint32_t pslc_free_line_cnt;
	uint32_t pslc_victim_line_cnt;
	uint32_t pslc_full_line_cnt;
};

struct zms_write_flow_control {
	long int write_credits;
	long int credits_to_refill;
};

struct zms_ftl {
	struct ssd *ssd;

	struct znsparams zp;
	struct zone_resource_info res_infos[RES_TYPE_COUNT];
	struct zone_descriptor *zone_descs;
	struct zone_report *report_buffer;
	struct buffer *write_buffer;
	struct buffer *zrwa_buffer;
	void *storage_base_addr;

	uint64_t current_time;
	// l2p
	struct ppa *maptbl;
	int *l2pcache_idx;

	struct zms_line_mgmt lm;
	// pSLC
	struct zms_write_pointer pslc_wp;
	struct zms_write_pointer pslc_gc_wp;
	struct zms_write_flow_control pslc_wfc;
	// GC
	struct zms_write_pointer wp;
	struct zms_write_pointer gc_wp;
	struct zms_write_flow_control wfc;
	uint64_t *rmap; // reverse mapptbl, assume it's stored in OOB
	struct ppa last_gc_ppa;

	// for debug
	uint64_t nopg_last_lpn;
	uint64_t last_slba;
	uint64_t last_nlb;
	uint64_t last_stime;
	int device_full;
	int pslc_full;
	int pending_for_migrating;

	// GC
	uint64_t *gc_agg_lpns;
	int gc_agg_len;
	int gc_agg_ttlpns;

	// Migration
	int num_aggs;
	int *zone_agg_pgs;
	uint64_t **zone_agg_lpns; // agg lpn
	int zone_write_unit;	  // oneshot page for normal blocks

	pqueue_t *migrating_line_pq;
	int line_write_cnt;

	// statistic
	uint64_t host_w_pgs; //# of pgs written to the flush
	uint64_t device_w_pgs;
	uint64_t migration_pgs;
	uint64_t gc_pgs;
	uint64_t l2p_misses;
	uint64_t l2p_hits;
	uint64_t read_wb_hits;
	uint64_t unmapped_read_cnt;
	uint64_t host_r_pgs;
	uint64_t zone_reset_cnt;	//# of zone resets
	uint64_t zone_write_cnt;	//# of zone write
	uint64_t host_wrequest_cnt; //# of host write requests
	uint64_t host_rrequest_cnt; //# of host write requests
	uint64_t host_flush_cnt;	//# of fua request from host
	uint32_t hot_zone_migrate;
	uint32_t warm_zone_migrate;
	uint32_t cold_zone_migrate;
	uint32_t normal_erase_cnt;
	uint32_t slc_erase_cnt;
	int gc_count;
	int early_flush_cnt;
	int inplace_update;	  // for debug
	int flush_to_slc;	  // for debug
	int flush_to_regular; // for debug
	int gc_copy_pgs;	  // for debug
};

/* zns internal functions */
static inline void *get_storage_addr_from_zid(struct zns_ftl *zns_ftl, uint64_t zid)
{
	return (void *)((char *)zns_ftl->storage_base_addr + zid * zns_ftl->zp.zone_size);
}

static inline bool is_zone_resource_avail(struct zns_ftl *zns_ftl, uint32_t type)
{
	return zns_ftl->res_infos[type].acquired_cnt < zns_ftl->res_infos[type].total_cnt;
}

static inline bool is_zone_resource_full(struct zns_ftl *zns_ftl, uint32_t type)
{
	return zns_ftl->res_infos[type].acquired_cnt == zns_ftl->res_infos[type].total_cnt;
}

static inline bool acquire_zone_resource(struct zns_ftl *zns_ftl, uint32_t type)
{
	if (is_zone_resource_avail(zns_ftl, type)) {
		zns_ftl->res_infos[type].acquired_cnt++;
		return true;
	}

	return false;
}

static inline void release_zone_resource(struct zns_ftl *zns_ftl, uint32_t type)
{
	ASSERT(zns_ftl->res_infos[type].acquired_cnt > 0);

	zns_ftl->res_infos[type].acquired_cnt--;
}

static inline void change_zone_state(struct zns_ftl *zns_ftl, uint32_t zid, enum zone_state state)
{
	NVMEV_ZNS_DEBUG("change state zid %d from %d to %d \n", zid, zns_ftl->zone_descs[zid].state,
					state);

	// check if transition is correct
	zns_ftl->zone_descs[zid].state = state;
}

static inline uint32_t lpn_to_zone(struct zns_ftl *zns_ftl, uint64_t lpn)
{
	return (lpn) / (zns_ftl->zp.zone_size / zns_ftl->ssd->sp.pgsz);
}

static inline uint64_t zone_to_slpn(struct zns_ftl *zns_ftl, uint32_t zid)
{
	return (zid) * (zns_ftl->zp.zone_size / zns_ftl->ssd->sp.pgsz);
}

static inline uint32_t lba_to_zone(struct zns_ftl *zns_ftl, uint64_t lba)
{
	return (lba) / (zns_ftl->zp.zone_size / zns_ftl->ssd->sp.secsz);
}

static inline uint64_t zone_to_slba(struct zns_ftl *zns_ftl, uint32_t zid)
{
	return (zid) * (zns_ftl->zp.zone_size / zns_ftl->ssd->sp.secsz);
}

static inline uint64_t zone_to_elba(struct zns_ftl *zns_ftl, uint32_t zid)
{
	return zone_to_slba(zns_ftl, zid + 1) - 1;
}

static inline uint64_t zone_to_elpn(struct zns_ftl *zns_ftl, uint32_t zid)
{
	return zone_to_elba(zns_ftl, zid) / zns_ftl->ssd->sp.secs_per_pg;
}

static inline uint32_t die_to_channel(struct zns_ftl *zns_ftl, uint32_t die)
{
	return (die) % zns_ftl->ssd->sp.nchs;
}

static inline uint32_t die_to_lun(struct zns_ftl *zns_ftl, uint32_t die)
{
	return (die) / zns_ftl->ssd->sp.nchs;
}

static inline uint64_t lba_to_lpn(struct zns_ftl *zns_ftl, uint64_t lba)
{
	return lba / zns_ftl->ssd->sp.secs_per_pg;
}

static inline uint32_t __nr_lbas_from_rw_cmd(struct nvme_rw_command *cmd)
{
	return cmd->length + 1;
}

static inline bool __check_boundary_error(struct zns_ftl *zns_ftl, uint64_t slba, uint32_t nr_lba)
{
	return (lba_to_zone(zns_ftl, slba) == lba_to_zone(zns_ftl, slba + nr_lba - 1));
}

static inline void __increase_write_ptr(struct zns_ftl *zns_ftl, uint32_t zid, uint32_t nr_lba)
{
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	uint64_t cur_write_ptr = zone_descs[zid].wp;
	uint64_t zone_capacity = zone_descs[zid].zone_capacity;

	cur_write_ptr += nr_lba;

	zone_descs[zid].wp = cur_write_ptr;

	if (cur_write_ptr == (zone_to_slba(zns_ftl, zid) + zone_capacity)) {
		// change state to ZSF
		release_zone_resource(zns_ftl, OPEN_ZONE);
		release_zone_resource(zns_ftl, ACTIVE_ZONE);

		if (zone_descs[zid].zrwav) {
			ASSERT(0);
		}

		change_zone_state(zns_ftl, zid, ZONE_STATE_FULL);
	} else if (cur_write_ptr > (zone_to_slba(zns_ftl, zid) + zone_capacity)) {
		NVMEV_ERROR("[%s] Write Boundary error!!\n", __func__);
	}
}

static inline struct ppa __lpn_to_ppa(struct zns_ftl *zns_ftl, uint64_t lpn)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct znsparams *zpp = &zns_ftl->zp;
	uint64_t zone = lpn_to_zone(zns_ftl, lpn); // find corresponding zone
	uint64_t off = lpn - zone_to_slpn(zns_ftl, zone);

	uint32_t sdie = (zone * zpp->dies_per_zone) % spp->tt_luns;
	uint32_t die = sdie + ((off / spp->pgs_per_oneshotpg) % zpp->dies_per_zone);

	uint32_t channel = die_to_channel(zns_ftl, die);
	uint32_t lun = die_to_lun(zns_ftl, die);
	struct ppa ppa = {
		.g =
			{
				.lun = lun,
				.ch = channel,
				.pg = off % spp->pgs_per_oneshotpg,
			},
	};

	return ppa;
}

/* zns external interface */
void zns_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
						uint32_t cpu_nr_dispatcher);
void zns_remove_namespace(struct nvmev_ns *ns);

void zns_zmgmt_recv(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
void zns_zmgmt_send(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);

#if (BASE_SSD == CONZONE_PROTOTYPE)
void zms_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
						uint32_t cpu_nr_dispatcher);
void zms_remove_namespace(struct nvmev_ns *ns);
void zms_remove_ssd(struct nvmev_ns *ns);
void zms_realize_namespaces(struct nvmev_ns *ns, int nr_ns, uint64_t size,
							uint32_t cpu_nr_dispatcher);
bool zms_zoned_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
								struct nvmev_result *ret);
bool zms_block_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
								struct nvmev_result *ret);
bool block_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool block_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zoned_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zoned_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
void zone_reset(struct zms_ftl *zms_ftl, uint64_t zid, int sqid);
void zms_print_statistic_info(struct zms_ftl *zms_ftl);
struct ppa get_maptbl_ent(struct zms_ftl *zms_ftl, uint64_t lpn);
uint64_t buffer_flush(struct zms_ftl *zms_ftl, struct buffer *write_buffer, uint64_t nsecs_start);

struct ppa get_current_page(struct zms_ftl *zms_ftl, struct zms_write_pointer *wp);
void update_write_pointer(struct zms_write_pointer *wp, struct ppa ppa);

struct zms_write_pointer *zms_get_wp(struct zms_ftl *ftl, uint32_t io_type, int loc);
struct zms_line *get_next_free_line(struct zms_ftl *zms_ftl, int location);
int lmid_2_blkid(struct zms_ftl *zms_ftl, struct zms_line *line);

struct list_head *zms_get_free_list(struct zms_ftl *zms_ftl, int location);
struct list_head *zms_get_full_list(struct zms_ftl *zms_ftl, int location);
struct pqueue_t *zms_get_victim_pq(struct zms_ftl *zms_ftl, int location);
void print_agg(struct zms_ftl *zms_ftl, int agg_len, uint64_t *agg_lpns);
void print_lines(struct zms_ftl *zms_ftl);
void print_zone_mapping(struct zms_ftl *zms_ftl, uint32_t zid);
void print_ppa(struct ppa ppa);

void dec_free_cnt(struct zms_ftl *zms_ftl, int location);
void inc_free_cnt(struct zms_ftl *zms_ftl, int location);
void dec_victim_cnt(struct zms_ftl *zms_ftl, int location);
void inc_victim_cnt(struct zms_ftl *zms_ftl, int location);
void dec_full_cnt(struct zms_ftl *zms_ftl, int location);
void inc_full_cnt(struct zms_ftl *zms_ftl, int location);
void dec_line_rpc(struct zms_ftl *zms_ftl, struct ppa *ppa);
struct ppa get_first_page(struct zms_ftl *zms_ftl, struct zms_line *line);
struct nand_block *line_2_blk(struct zms_ftl *zms_ftl, struct zms_line *line);
#endif
#endif
