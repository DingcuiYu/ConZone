// SPDX-License-Identifier: GPL-2.0-only

#ifndef _NVMEVIRT_ZNS_FTL_H
#define _NVMEVIRT_ZNS_FTL_H

#include <linux/types.h>
#include "nvmev.h"
#include "nvme_zns.h"

#define NVMEV_ZNS_DEBUG(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_DEBUG(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_RW_DEBUG(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_RW_DEBUG_VERBOSE(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_L2P_DEBUG(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_L2P_DEBUG_VERBOSE(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_GC_DEBUG(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)
#define NVMEV_ZMS_GC_DEBUG_VERBOSE(string, args...) //printk(KERN_INFO "%s: " string, NVMEV_DRV_NAME, ##args)


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
	uint32_t zone_size; //bytes
	uint32_t zone_wb_size;
	uint32_t nr_zone_wb; // # of write buffer for all zones
	uint32_t zone_capacity; //NOT defined in ZAC/ZBC commands

	/*related to zrwa*/
	uint32_t nr_zrwa_zones;
	uint32_t zrwafg_size;
	uint32_t zrwa_size;
	uint32_t zrwa_buffer_size;
	uint32_t lbas_per_zrwafg;
	uint32_t lbas_per_zrwa;

	uint32_t gc_thres_lines_high;
	bool enable_gc_delay;
	uint32_t chunk_size; //bytes
	uint32_t pgs_per_chunk;
	uint32_t pgs_per_zone;
	int zone_type;
};

struct zone_resource_info {
	__u32 acquired_cnt;
	__u32 total_cnt;
};

//#if(BASE_SSD == ZMS_PROTOTYPE)
struct l2pcache_ent{
	uint64_t la;
	int gran;
	int resident;
	int next; //for LRU
	int last;
};

struct l2pcache{
	int size;
	int num_slots;
	int slot_size;
	int* slot_len;
	int* tail;
	int* head;
	int evict_policy;
	struct l2pcache_ent** mapping;//only record cached lpns
};

struct l2plog{
	size_t aggregate;
	size_t size;
};

struct zms_line {
	int id; /* line id, the same as corresponding block id */
	int ipc; /* invalid page count in this line */
	int vpc; /* valid page count in this line */
	struct list_head entry;
	/* position in the priority queue for victim lines */
	size_t pos;
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
};

struct zms_write_flow_control {
	uint32_t write_credits;
	uint32_t credits_to_refill;
};

//#endif

struct zns_ftl {
	struct ssd *ssd;

	struct znsparams zp;
	struct zone_resource_info res_infos[RES_TYPE_COUNT];
	struct zone_descriptor *zone_descs;
	struct zone_report *report_buffer;
	struct buffer *zone_write_buffer;
	struct buffer *zrwa_buffer;
	void *storage_base_addr;
	//#if(BASE_SSD == ZMS_PROTOTYPE)
	int ssd_type; //conventional zone or seqtional zone
	//unaligned
	uint64_t unaligned_slpn;
	//l2p 
	int pre_read;
	uint64_t tt_lpns;
	struct l2pcache l2pcache;
	struct l2plog l2plog;
	struct ppa* maptbl;
	//pSLC
	int pslc_ttpgs;
	int pslc_blks; // # of blocks in each chip that configured as pSLC
	int uspace_reduction; //whether the pSLC buffer reduces the total configurable user space
	struct zms_write_pointer pslc_wp;
	struct zms_write_pointer pslc_gc_wp;
	struct zms_write_pointer unaligned_wp;
	struct zms_line_mgmt pslc_lm;
	struct zms_write_flow_control pslc_wfc;
	//GC
	struct zms_write_pointer wp;
	struct zms_write_pointer gc_wp;
	struct zms_line_mgmt lm;
	uint64_t *rmap; /* reverse mapptbl, assume it's stored in OOB */
	struct zms_write_flow_control wfc;
	//for debug
	uint64_t last_slba;
	uint64_t last_nlb;
	uint64_t last_stime;
	//statistic
	uint64_t host_w_pgs;
	uint64_t device_w_pgs;
	uint64_t l2p_misses;
	uint64_t l2p_hits;
	//#endif
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

/* zns external interface */
void zns_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
			uint32_t cpu_nr_dispatcher);
void zns_remove_namespace(struct nvmev_ns *ns);

void zns_zmgmt_recv(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
void zns_zmgmt_send(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);
bool zns_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret);

//#if(BASE_SSD == ZMS_PROTOTYPE)
void zms_reset_zone(struct zns_ftl *zns_ftl, uint64_t zid);
uint64_t AU_to_slpn(struct zns_ftl *zns_ftl, int au, uint64_t lpn);

int l2p_search(struct zns_ftl *zns_ftl, uint64_t lpn,uint64_t* rla);
void l2p_access(struct zns_ftl *zns_ftl, uint64_t la, int idx);
int l2p_insert(struct zns_ftl *zns_ftl, uint64_t la,int gran,int res);
int l2p_replace(struct zns_ftl *zns_ftl, uint64_t la, int gran,int res);
struct zms_write_pointer * zms_get_wp(struct zns_ftl *ftl, uint32_t io_type, bool pSLC);
struct zms_line *get_next_free_line(struct zns_ftl *zns_ftl,bool pSLC);
//#endif

#endif
