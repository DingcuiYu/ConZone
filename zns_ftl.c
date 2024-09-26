// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

//#if (BASE_SSD == ZMS_PROTOTYPE)
//only for conventional zone
struct zms_write_pointer * zms_get_wp(struct zns_ftl *ftl, uint32_t io_type, bool pSLC)
{
	if (io_type == USER_IO) {
		return pSLC?&ftl->pslc_wp:&ftl->wp;
	} else if (io_type == GC_IO) {
		return pSLC?&ftl->pslc_gc_wp:&ftl->gc_wp;
	}
	else if (io_type == UNALIGNED_IO)
	{
		return &ftl->unaligned_wp;
	}
	

	NVMEV_ASSERT(0);
	return NULL;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
	return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
	return ((struct zms_line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct zms_line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
	return ((struct zms_line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
	((struct zms_line *)a)->pos = pos;
}

static void zms_init_lines(struct zns_ftl *zns_ftl,bool pSLC)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zms_line_mgmt *lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct zms_line *line;
	int i;

	lm->tt_lines = pSLC?zns_ftl->pslc_blks:spp->blks_per_pl-zns_ftl->pslc_blks;
	lm->lines = vmalloc(sizeof(struct zms_line) * lm->tt_lines);

	INIT_LIST_HEAD(&lm->free_line_list);
	INIT_LIST_HEAD(&lm->full_line_list);

	lm->victim_line_pq = pqueue_init(lm->tt_lines, victim_line_cmp_pri, victim_line_get_pri,
					 victim_line_set_pri, victim_line_get_pos,
					 victim_line_set_pos);

	lm->free_line_cnt = 0;
	for (i = 0; i < lm->tt_lines; i++) {
		lm->lines[i] = (struct zms_line){
			.id = i,
			.ipc = 0,
			.vpc = 0,
			.pos = 0,
			.entry = LIST_HEAD_INIT(lm->lines[i].entry),
		};

		/* initialize all the lines as free lines */
		list_add_tail(&lm->lines[i].entry, &lm->free_line_list);
		lm->free_line_cnt++;
		//NVMEV_INFO("%s add %d curr line %p\n",pSLC?"slc":"qlc",i,list_first_entry_or_null(&lm->free_line_list, struct zms_line, entry));
	}

	NVMEV_ASSERT(lm->free_line_cnt == lm->tt_lines);
	lm->victim_line_cnt = 0;
	lm->full_line_cnt = 0;
	//NVMEV_INFO("init %s lines, tt line = %d\n",pSLC?"slc":"qlc",lm->tt_lines);
}

static void zms_remove_lines(struct zns_ftl *zns_ftl,bool pSLC)
{
	struct zms_line_mgmt *lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	pqueue_free(lm->victim_line_pq);
	vfree(lm->lines);
}

struct zms_line *get_next_free_line(struct zns_ftl *zns_ftl,bool pSLC)
{
	struct zms_line_mgmt *lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct zms_line *curline = list_first_entry_or_null(&lm->free_line_list, struct zms_line, entry);

	if (!curline) {
		NVMEV_ERROR("No free line left in %s VIRT !!!!\n",pSLC?"pslc":"normal");
		return NULL;
	}

	list_del_init(&curline->entry);
	lm->free_line_cnt--;
	NVMEV_DEBUG("%s: free_line_cnt %d\n", __func__, lm->free_line_cnt);
	return curline;
}

static void prepare_write_pointer(struct zns_ftl *zns_ftl, uint32_t io_type,bool pSLC)
{
	struct zms_write_pointer *wp = zms_get_wp(zns_ftl, io_type,pSLC);
	struct zms_line *curline = get_next_free_line(zns_ftl,pSLC);

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

	/* wp->curline is always our next-to-write super-block */
	*wp = (struct zms_write_pointer){
		.curline = curline,
		.loc = pSLC,
		.ch = 0,
		.lun = 0,
		.pg = 0,
		.blk = curline->id,
		.pl = 0,
	};
	NVMEV_INFO("prepared %s wp for %s IO,line(%p) id %d\n",pSLC?"slc":"qlc",io_type==USER_IO?"USER":(io_type==GC_IO?"GC":"UNALIGNED"),curline,wp->blk);
}
//#endif

static void __init_descriptor(struct zns_ftl *zns_ftl)
{
	struct zone_descriptor *zone_descs;
	uint32_t zone_size = zns_ftl->zp.zone_size;
	uint32_t nr_zones = zns_ftl->zp.nr_zones;
	uint64_t zslba = 0;
	uint32_t i = 0;
	const uint32_t zrwa_buffer_size = zns_ftl->zp.zrwa_buffer_size;
	const uint32_t zone_wb_size = ZONED_DEVICE?zns_ftl->zp.zone_wb_size:zns_ftl->zp.zone_wb_size*zns_ftl->zp.nr_zone_wb;
	const uint32_t nr_zone_wb = ZONED_DEVICE?(BASE_SSD == ZMS_PROTOTYPE? zns_ftl->zp.nr_zone_wb:nr_zones):1;

	zns_ftl->zone_descs = kzalloc(sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);
	zns_ftl->report_buffer = kmalloc(
		sizeof(struct zone_report) + sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);

	if (zrwa_buffer_size)
		zns_ftl->zrwa_buffer = kmalloc(sizeof(struct buffer) * nr_zones, GFP_KERNEL);

	if (zone_wb_size)
		zns_ftl->zone_write_buffer = kmalloc(sizeof(struct buffer) * nr_zone_wb, GFP_KERNEL);

	zone_descs = zns_ftl->zone_descs;

	for (i = 0; i < nr_zones; i++) {
		zone_descs[i].state = ZONE_STATE_EMPTY;
		zone_descs[i].type = ZONE_TYPE_SEQ_WRITE_REQUIRED;

		zone_descs[i].zslba = zslba;
		zone_descs[i].wp = zslba;
		zslba += BYTE_TO_LBA(zone_size);
		zone_descs[i].zone_capacity = BYTE_TO_LBA(zone_size);

		if (zrwa_buffer_size)
			buffer_init(&(zns_ftl->zrwa_buffer[i]), zrwa_buffer_size);

		NVMEV_ZNS_DEBUG("[%d] zslba 0x%llx zone capacity 0x%llx, wp 0x%llx\n", i,
			zone_descs[i].zslba, zone_descs[i].zone_capacity, zone_descs[i].wp);
	}

	for (i = 0; i < nr_zone_wb; i++)
	{
		if(!zone_wb_size) break;
		buffer_init(&(zns_ftl->zone_write_buffer[i]), zone_wb_size);
	}
	if(zone_wb_size)
		NVMEV_INFO("# of write buffer = %d , write buffer size = %d KiB\n",nr_zone_wb,BYTE_TO_KB(zone_wb_size));
	
}

static void __remove_descriptor(struct zns_ftl *zns_ftl)
{
	if (zns_ftl->zp.zrwa_buffer_size)
		kfree(zns_ftl->zrwa_buffer);

	if (zns_ftl->zp.zone_wb_size)
	{
		kfree(zns_ftl->zone_write_buffer);
	}
		

	kfree(zns_ftl->report_buffer);
	kfree(zns_ftl->zone_descs);
}

static void __init_resource(struct zns_ftl *zns_ftl)
{
	struct zone_resource_info *res_infos = zns_ftl->res_infos;

	res_infos[ACTIVE_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};

	res_infos[OPEN_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};

	res_infos[ZRWA_ZONE] = (struct zone_resource_info){
		.total_cnt = zns_ftl->zp.nr_zones,
		.acquired_cnt = 0,
	};
}

static void zns_init_params(struct znsparams *zpp, struct ssdparams *spp, uint64_t capacity, uint32_t nsid)
{
	uint32_t zone_size = ZONE_SIZE;
	uint32_t nr_zones = capacity / zone_size;
	*zpp = (struct znsparams){
		.zone_size = zone_size,
		.nr_zones = nr_zones,
		.dies_per_zone = DIES_PER_ZONE,
		.nr_active_zones = nr_zones, // max
		.nr_open_zones = nr_zones, // max
		.nr_zrwa_zones = MAX_ZRWA_ZONES,
		.zone_wb_size = ZONE_WB_SIZE,
		.nr_zone_wb = NS_SSD_TYPE(nsid) == SSD_TYPE_ZNS?NR_SEQ_ZONE_WB:NR_CONV_ZONE_WB,
		.zrwa_size = ZRWA_SIZE,
		.zrwafg_size = ZRWAFG_SIZE,
		.zrwa_buffer_size = ZRWA_BUFFER_SIZE,
		.lbas_per_zrwa = ZRWA_SIZE / spp->secsz,
		.lbas_per_zrwafg = ZRWAFG_SIZE / spp->secsz,
		.gc_thres_lines_high = 2, // Need only two lines.(host write, gc)
		.enable_gc_delay = 1,
		.zone_type = NS_SSD_TYPE(nsid),
	};

	NVMEV_ASSERT((capacity % zpp->zone_size) == 0);
	//#if(BASE_SSD == ZMS_PROTOTYPE)
	zpp->chunk_size = CHUNK_SIZE;
	zpp->pgs_per_chunk = zpp->chunk_size / spp->pgsz;
	zpp->pgs_per_zone = ZONE_SIZE / spp->pgsz;
	//#endif
	/* It should be 4KB aligned, according to lpn size */
	NVMEV_ASSERT((zpp->zone_size % spp->pgsz) == 0);

	NVMEV_INFO("zone_size=%u(Byte),%u(MB), # zones=%d # die/zone=%d zone type = %s\n", zpp->zone_size,
		   BYTE_TO_MB(zpp->zone_size), zpp->nr_zones, zpp->dies_per_zone, zpp->zone_type==SSD_TYPE_ZNS?"seq":"conv");
}

static void __init_zms(struct zns_ftl *zns_ftl,struct znsparams *zpp,struct ssd *ssd)
{
	int i,j;

	zns_ftl->ssd_type = zpp->zone_type;
	//for debug
	zns_ftl->last_slba = 0;
	zns_ftl->last_nlb = 0;
	zns_ftl->tt_lpns = (zpp->zone_size*zpp->nr_zones)/ssd->sp.pgsz;

	zns_ftl->maptbl = kmalloc(sizeof(struct ppa)*(ssd->sp.tt_pgs), GFP_KERNEL);
	for (i = 0; i < ssd->sp.tt_pgs; i++) {
		zns_ftl->maptbl[i].ppa = UNMAPPED_PPA;
	}

	zns_ftl->l2pcache.size = L2P_CACHE_SIZE/L2P_ENTRY_SIZE; //# of entries
	zns_ftl->l2pcache.evict_policy = L2P_EVICT_POLICY;
	zns_ftl->l2pcache.num_slots = L2P_CACHE_HASH_SLOT;
	NVMEV_ASSERT(( zns_ftl->l2pcache.size %  zns_ftl->l2pcache.num_slots) == 0);
	zns_ftl->l2pcache.slot_size = zns_ftl->l2pcache.size / zns_ftl->l2pcache.num_slots;
	zns_ftl->l2pcache.mapping = kmalloc(sizeof(struct l2pcache_ent*)*(zns_ftl->l2pcache.num_slots), GFP_KERNEL);
	
	zns_ftl->l2pcache.slot_len = kmalloc(sizeof(int)*zns_ftl->l2pcache.num_slots, GFP_KERNEL);
	zns_ftl->l2pcache.head = kmalloc(sizeof(int)*zns_ftl->l2pcache.num_slots, GFP_KERNEL);
	zns_ftl->l2pcache.tail = kmalloc(sizeof(int)*zns_ftl->l2pcache.num_slots, GFP_KERNEL);

	for(i = 0;i < zns_ftl->l2pcache.num_slots;i++)
	{
		zns_ftl->l2pcache.mapping[i] = kmalloc(sizeof(struct l2pcache_ent)*(zns_ftl->l2pcache.slot_size), GFP_KERNEL);
		for(j = 0; j < zns_ftl->l2pcache.slot_size; j++)
		{
			zns_ftl->l2pcache.mapping[i][j].la = INVALID_LPN;
			zns_ftl->l2pcache.mapping[i][j].gran = PAGE_MAP;
			zns_ftl->l2pcache.mapping[i][j].resident = 0;
			zns_ftl->l2pcache.mapping[i][j].last = -1;
			zns_ftl->l2pcache.mapping[i][j].next = -1;
		}
		zns_ftl->l2pcache.head[i] = 0;
		zns_ftl->l2pcache.tail[i] = 0;
		zns_ftl->l2pcache.slot_len[i] = 0;
	}

	zns_ftl->l2plog.size = L2P_LOG_SIZE;
	NVMEV_INFO("# of l2p entries (cached/all) = %d / %lld, policy = %d\n",zns_ftl->l2pcache.size,zns_ftl->tt_lpns,zns_ftl->l2pcache.evict_policy);
	zns_ftl->pre_read = L2P_PREREAD;

	zns_ftl->pslc_blks = pSLC_INIT_BLKS;
	zns_ftl->pslc_ttpgs = ssd->sp.pgs_per_blk * zns_ftl->pslc_blks * ssd->sp.pls_per_lun * ssd->sp.luns_per_ch * ssd->sp.nchs;
	zns_ftl->uspace_reduction = USER_SPACE_REDUCTION;
	NVMEV_ASSERT(zns_ftl->pslc_blks<zns_ftl->ssd->sp.blks_per_pl);

	zns_ftl->rmap = kmalloc(sizeof(struct ppa)*(ssd->sp.tt_pgs), GFP_KERNEL);
	for (i = 0; i < ssd->sp.tt_pgs; i++) {
		zns_ftl->rmap[i] = INVALID_LPN;
	}
	NVMEV_INFO("rmap entries %ld\n",ssd->sp.tt_pgs);

	zms_init_lines(zns_ftl,LOC_NORMAL);
	prepare_write_pointer(zns_ftl, USER_IO,LOC_NORMAL);
	//only for conventional zones
	if(!ZONED_DEVICE || zns_ftl->ssd_type==SSD_TYPE_ZNS_CONV)
	{
		prepare_write_pointer(zns_ftl, GC_IO,LOC_NORMAL);
	}

	//only for pSLC area
	zms_init_lines(zns_ftl,LOC_PSLC);
	prepare_write_pointer(zns_ftl, USER_IO,LOC_PSLC);
	prepare_write_pointer(zns_ftl, GC_IO,LOC_PSLC);
	prepare_write_pointer(zns_ftl, UNALIGNED_IO, LOC_PSLC);
	NVMEV_INFO("all lines: %d slc lines: %d qlc lines: %d\n",zns_ftl->ssd->sp.blks_per_pl,zns_ftl->pslc_lm.tt_lines,zns_ftl->lm.tt_lines);

	zns_ftl->unaligned_slpn = ssd->sp.blksz/ssd->sp.pgsz *ssd->sp.pls_per_lun*ssd->sp.luns_per_ch *ssd->sp.nchs;

	zns_ftl->wfc.write_credits = ssd->sp.pgs_per_line;
	zns_ftl->wfc.credits_to_refill = ssd->sp.pgs_per_line;

	zns_ftl->pslc_wfc.write_credits = ssd->sp.pgs_per_line;
	zns_ftl->pslc_wfc.credits_to_refill = ssd->sp.pgs_per_line;

	zns_ftl->host_w_pgs = 0;
	zns_ftl->device_w_pgs = 0;
	zns_ftl->l2p_hits = 0;
	zns_ftl->l2p_misses = 0;
}

static void zns_init_ftl(struct zns_ftl *zns_ftl, struct znsparams *zpp, struct ssd *ssd,
			 void *mapped_addr)
{
	*zns_ftl = (struct zns_ftl){
		.zp = *zpp, /*copy znsparams*/

		.ssd = ssd,
		.storage_base_addr = mapped_addr,
	};

	__init_descriptor(zns_ftl);
	__init_resource(zns_ftl);

	//#if (BASE_SSD == ZMS_PROTOTYPE)
	__init_zms(zns_ftl,zpp,ssd);
	//#endif
}

void zns_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
			uint32_t cpu_nr_dispatcher)
{
	NVMEV_ZMS_DEBUG("---------------new zms ssd--------------\n");
	struct ssd *ssd;
	struct zns_ftl *zns_ftl;
	uint64_t user_size = size - (BASE_SSD==ZMS_PROTOTYPE?RSV_SIZE:0);;

	struct ssdparams spp;
	struct znsparams zpp;

	const uint32_t nr_parts = 1; /* Not support multi partitions for zns*/
	NVMEV_ASSERT(nr_parts == 1);

	ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
	ssd_init_params(&spp, size, nr_parts);
	ssd_init(ssd, &spp, cpu_nr_dispatcher);

	zns_ftl = kmalloc(sizeof(struct zns_ftl) * nr_parts, GFP_KERNEL);
	zns_init_params(&zpp, &spp, user_size, id);
	zns_init_ftl(zns_ftl, &zpp, ssd, mapped_addr);

	*ns = (struct nvmev_ns){
		.id = id,
		.csi = NVME_CSI_ZNS,
		.nr_parts = nr_parts,
		.ftls = (void *)zns_ftl,
		.size = user_size,
		.mapped = mapped_addr,

		/*register io command handler*/
		.proc_io_cmd = zns_proc_nvme_io_cmd,
	};
	return;
}

void zns_remove_namespace(struct nvmev_ns *ns)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	
	NVMEV_INFO("WAF = (%lld/%lld)\n",zns_ftl->device_w_pgs,zns_ftl->host_w_pgs);
	NVMEV_INFO("%s map, L2P miss rate = (%lld/%lld)\n",L2P_HYBRID_MAP?"hybrid":"page",zns_ftl->l2p_misses,zns_ftl->l2p_misses+zns_ftl->l2p_hits);
	ssd_remove(zns_ftl->ssd);

	__remove_descriptor(zns_ftl);
	//#if(BASE_SSD == ZMS_PROTOTYPE)
	zms_remove_lines(zns_ftl,false);
	zms_remove_lines(zns_ftl,true);
	kfree(zns_ftl->maptbl);
	kfree(zns_ftl->rmap);
	for(int i = 0;i < zns_ftl->l2pcache.num_slots;i++)
	{
		kfree(zns_ftl->l2pcache.mapping[i]);
	}
	kfree(zns_ftl->l2pcache.tail);
	kfree(zns_ftl->l2pcache.head);
	kfree(zns_ftl->l2pcache.slot_len);
	kfree(zns_ftl->l2pcache.mapping);
	//#endif
	kfree(zns_ftl->ssd);
	kfree(zns_ftl);

	ns->ftls = NULL;
}

static void zns_flush(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	uint64_t start, latest;
	uint32_t i;
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(zns_ftl[i].ssd));
	}

	NVMEV_DEBUG("%s latency=%llu\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
	return;
}

bool zns_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;
	NVMEV_ASSERT(ns->csi == NVME_CSI_ZNS);
	/*still not support multi partitions ...*/
	NVMEV_ASSERT(ns->nr_parts == 1);

	switch (cmd->common.opcode) {
	case nvme_cmd_write:
	case nvme_cmd_zone_append:
		if (!zns_write(ns, req, ret))
			return false;
		break;
	case nvme_cmd_read:
		if (!zns_read(ns, req, ret))
			return false;
		break;
	case nvme_cmd_flush:
		zns_flush(ns, req, ret);
		break;
	case nvme_cmd_zone_mgmt_send:
		zns_zmgmt_send(ns, req, ret);
		break;
	case nvme_cmd_zone_mgmt_recv:
		zns_zmgmt_recv(ns, req, ret);
		break;
	default:
		NVMEV_ERROR("%s: unimplemented command: %s(%d)\n", __func__,
			    nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
		break;
	}

	return true;
}

//#if(BASE_SSD == ZMS_PROTOTYPE)
//only operated in dispatcher!!
//At least O(logn) search!
static inline int l2p_slot_match(struct l2pcache* cache, uint64_t slot, uint64_t la, int gran)
{
	int i;
	for(i = 0;i<cache->slot_len[slot];i++)
	{
		if(cache->mapping[slot][i].la == la && cache->mapping[slot][i].gran==gran) return i;
	}
	return -1;
}

int l2p_search(struct zns_ftl *zns_ftl, uint64_t lpn,uint64_t* rla)
{
	int i,gran,ret;
	uint64_t slot,la;
	struct l2pcache* cache = &zns_ftl->l2pcache;
	int sidx = L2P_HYBRID_MAP?0:NUM_MAP-1;

	for( i = sidx;i < NUM_MAP;i++){
		gran = MAP_GRAN(i);
		la = AU_to_slpn(zns_ftl,gran,lpn);
		slot = la%cache->num_slots;
		ret = l2p_slot_match(cache,slot,la,gran);
		if(ret!=-1) {
			zns_ftl->l2p_hits++;
			*rla = la;
			NVMEV_ZMS_L2P_DEBUG_VERBOSE("%s L2P Cache HIT for lpn %lld (la %lld), gran %s\n",__func__,lpn,la,gran==ZONE_MAP?"zone":(gran==CHUNK_MAP?"chunk":"page"));
			return ret;
		}
	}

	zns_ftl->l2p_misses++;
	*rla = INVALID_LPN;
	return -1;
}

//O(1) access & replace
//only len==size
int l2p_replace(struct zns_ftl *zns_ftl, uint64_t la, int gran, int res)
{
	int evict_idx;
	struct l2pcache* cache = &zns_ftl->l2pcache;
	uint64_t slot = la%cache->num_slots;
	switch (cache->evict_policy)
	{
	case L2P_EVICTION_POLICY_NONE:
	case L2P_EVICTION_POLICY_LRU:
		evict_idx = cache->head[slot];
		while(evict_idx!=cache->tail[slot] && cache->mapping[slot][evict_idx].resident)
		{
			evict_idx = cache->mapping[slot][evict_idx].next;
		}

		if(evict_idx==cache->tail[slot] && cache->mapping[slot][evict_idx].resident) 
		{	
			return -1; //no free space to evict
		}
		if(evict_idx==cache->tail[slot]) break;

		if(evict_idx == cache->head[slot])
		{
			cache->head[slot] = cache->mapping[slot][evict_idx].next;
			cache->mapping[slot][cache->head[slot]].last = -1;
		}
		else
		{
			int last = cache->mapping[slot][evict_idx].last;
			int next = cache->mapping[slot][evict_idx].next;
			cache->mapping[slot][last].next = next;
			cache->mapping[slot][next].last = last;
		}

		cache->mapping[slot][evict_idx].last = cache->tail[slot];
		cache->mapping[slot][evict_idx].next = -1;	

		cache->mapping[slot][cache->tail[slot]].next = evict_idx;
		cache->tail[slot] = evict_idx;
		break;
	default:
		NVMEV_ERROR("Invalid L2P Cache Evict Policy %d\n",cache->evict_policy);
		return -1;
	}

	cache->mapping[slot][evict_idx].la = la;
	cache->mapping[slot][evict_idx].gran = gran;
	cache->mapping[slot][evict_idx].resident = res;
	return evict_idx;
}

void l2p_access(struct zns_ftl *zns_ftl, uint64_t la, int idx)
{
	struct l2pcache* cache = &zns_ftl->l2pcache;
	uint64_t slot = la%cache->num_slots;
	if(idx==-1)
	{
		NVMEV_ERROR("idx == -1?:lpn 0x%llx slot %lld slot head %d slot tail %d\n",la,slot,cache->head[slot],cache->tail[slot]);
		return;
	}
	if(idx!=cache->tail[slot])
	{
		if(idx==cache->head[slot])
		{
			cache->head[slot] = cache->mapping[slot][idx].next;
			cache->mapping[slot][cache->head[slot]].last = -1;
		}
		else
		{
			int last = cache->mapping[slot][idx].last;
			int next = cache->mapping[slot][idx].next;
			cache->mapping[slot][last].next = next;
			cache->mapping[slot][next].last = last;
		}

		cache->mapping[slot][idx].last = cache->tail[slot];
		cache->mapping[slot][idx].next = -1;

		cache->mapping[slot][cache->tail[slot]].next = idx;
		cache->tail[slot] = idx;
	}
	return;
}

int l2p_insert(struct zns_ftl *zns_ftl, uint64_t la,int gran,int res)
{
	struct l2pcache* cache = &zns_ftl->l2pcache;
	uint64_t slot = la%cache->num_slots;
	if(cache->slot_len[slot]==cache->slot_size) return l2p_replace(zns_ftl,la,gran,res);
	int idx = cache->slot_len[slot];
	int tail = cache->tail[slot];

	cache->mapping[slot][idx].la = la;
	cache->mapping[slot][idx].gran = gran;
	cache->mapping[slot][idx].resident = res;

	cache->mapping[slot][idx].next = -1;
	if(idx == 0)
	{
		cache->mapping[slot][idx].last = -1;
	}
	else
	{
		cache->mapping[slot][idx].last = tail;

		cache->mapping[slot][tail].next = idx;
		cache->tail[slot] = idx;
	}
	cache->slot_len[slot]++;
	return idx;
}
//#endif