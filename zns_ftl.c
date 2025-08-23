// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/sched/clock.h>
#include <linux/vmalloc.h>

#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

static void __init_buffer(struct zns_ftl *zns_ftl)
{
	uint32_t zrwa_buffer_size = zns_ftl->zp.zrwa_buffer_size;
	uint32_t nr_zones = zns_ftl->zp.nr_zones;
	uint32_t zone_wb_size = zns_ftl->zp.zone_wb_size;
	uint32_t nr_zone_wb = zns_ftl->zp.ns_type == SSD_TYPE_ZNS ? nr_zones : zns_ftl->zp.nr_wb;
	if (zrwa_buffer_size) {
		zns_ftl->zrwa_buffer = kmalloc(sizeof(struct buffer) * nr_zones, GFP_KERNEL);
		for (int i = 0; i < nr_zones; i++)
			buffer_init(&(zns_ftl->zrwa_buffer[i]), zrwa_buffer_size);
	}

	if (zone_wb_size) {
		uint32_t wb_size = zone_wb_size;
#if (BASE_SSD == ZMS_PROTOTYPE)
		if (zns_ftl->zp.ns_type == SSD_TYPE_ZMS_ZONED) {
			switch (WB_MGNT) {
			case WB_STATIC:
			case WB_MOD:
			default:
				wb_size = zone_wb_size / nr_zone_wb;
				break;
			}
		}
#endif
		zns_ftl->zone_write_buffer = kmalloc(sizeof(struct buffer) * nr_zone_wb, GFP_KERNEL);
		for (int i = 0; i < nr_zone_wb; i++) {
			buffer_init(&(zns_ftl->zone_write_buffer[i]), wb_size);
		}

		NVMEV_INFO("[Size of Each Write Buffer] %d KiB [LPNs per Write Buffer] %llu\n",
				   BYTE_TO_KB(wb_size), zns_ftl->zone_write_buffer[0].tt_lpns);
	}
}

static void __init_descriptor(struct zns_ftl *zns_ftl)
{
	struct zone_descriptor *zone_descs;
	uint32_t zone_size = zns_ftl->zp.zone_size;
	uint32_t nr_zones = zns_ftl->zp.nr_zones;
	uint64_t zslba = 0;
	uint32_t i = 0;
	__init_buffer(zns_ftl);

	int ns_type = BASE_SSD == ZMS_PROTOTYPE ? zns_ftl->zp.ns_type : SSD_TYPE_ZNS;
	if (ns_type != SSD_TYPE_ZMS_ZONED && ns_type != SSD_TYPE_ZNS)
		return;

	uint32_t zone_capacity = zns_ftl->zp.zone_capacity;

	zns_ftl->zone_descs = kzalloc(sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);
	zns_ftl->report_buffer =
		kmalloc(sizeof(struct zone_report) + sizeof(struct zone_descriptor) * nr_zones, GFP_KERNEL);
	zone_descs = zns_ftl->zone_descs;

	for (i = 0; i < nr_zones; i++) {
		zone_descs[i].state = ZONE_STATE_EMPTY;
		zone_descs[i].type = ZONE_TYPE_SEQ_WRITE_REQUIRED;

		zone_descs[i].zslba = zslba;
		zone_descs[i].wp = zslba;
		zslba += BYTE_TO_LBA(zone_size);
		zone_descs[i].zone_capacity = BYTE_TO_LBA(zone_capacity);

		NVMEV_ZNS_DEBUG("[%d] zslba 0x%llx zone capacity 0x%llx, wp 0x%llx\n", i,
						zone_descs[i].zslba, zone_descs[i].zone_capacity, zone_descs[i].wp);
	}
}

static void __remove_descriptor(struct zns_ftl *zns_ftl)
{
	if (zns_ftl->zp.zrwa_buffer_size) {
		buffer_remove(zns_ftl->zrwa_buffer);
		kfree(zns_ftl->zrwa_buffer);
	}

	if (zns_ftl->zp.zone_wb_size) {
		buffer_remove(zns_ftl->zone_write_buffer);
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

static void zns_init_params(struct znsparams *zpp, struct ssdparams *spp, uint64_t capacity,
							struct nvmev_ns *ns, int ns_type)
{
	*zpp = (struct znsparams){
		.ns_type = ns_type,
		.physical_size = capacity,
		.logical_size = capacity,
		.zone_size = ZONE_SIZE,
		.nr_zones = capacity / ZONE_SIZE,
		.dies_per_zone = DIES_PER_ZONE,
		.nr_active_zones = capacity / ZONE_SIZE, // max
		.nr_open_zones = capacity / ZONE_SIZE,	 // max
		.nr_zrwa_zones = MAX_ZRWA_ZONES,
		.zone_wb_size = ZONE_WB_SIZE,
		.zrwa_size = ZRWA_SIZE,
		.zrwafg_size = ZRWAFG_SIZE,
		.zrwa_buffer_size = ZRWA_BUFFER_SIZE,
		.lbas_per_zrwa = ZRWA_SIZE / spp->secsz,
		.lbas_per_zrwafg = ZRWAFG_SIZE / spp->secsz,
	};

#if (BASE_SSD == ZNS_PROTOTYPE)
	zpp->zone_capacity = ZONE_CAPACITY;
#endif
	if (zpp->logical_size % zpp->zone_size != 0) {
		NVMEV_INFO("Invalid logical size (%llu MiB) zone size (%u MiB) nr zones %d\n",
				   BYTE_TO_MB(zpp->logical_size), BYTE_TO_MB(zpp->zone_size), zpp->nr_zones);
		zpp->logical_size = ((uint64_t)zpp->nr_zones) * ((uint64_t)zpp->zone_size);
		NVMEV_INFO("New logical size %llu MiB\n", BYTE_TO_MB(zpp->logical_size));
	}
	/* It should be 4KB aligned, according to lpn size */
	if ((zpp->zone_size % spp->pgsz) != 0) {
		NVMEV_ERROR("invalid zone_size %u (MB) zone size %u (KB)\n", BYTE_TO_MB(zpp->zone_size),
					BYTE_TO_KB(spp->pgsz));
	}

	NVMEV_INFO("zone_size=%u(Byte),%u(MB), # zones=%d # die/zone=%d \n", zpp->zone_size,
			   BYTE_TO_MB(zpp->zone_size), zpp->nr_zones, zpp->dies_per_zone);
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
}

void zns_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
						uint32_t cpu_nr_dispatcher)
{
	struct ssd *ssd;
	struct zns_ftl *zns_ftl;

	struct ssdparams spp;
	struct znsparams zpp;

	const uint32_t nr_parts = 1; /* Not support multi partitions for zns*/
	NVMEV_ASSERT(nr_parts == 1);

	ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
	ssd_init_params(&spp, size, nr_parts);
	ssd_init(ssd, &spp, cpu_nr_dispatcher);

	zns_ftl = kmalloc(sizeof(struct zns_ftl) * nr_parts, GFP_KERNEL);
	zns_init_params(&zpp, &spp, size, ns, NS_SSD_TYPE(id));
	zns_init_ftl(zns_ftl, &zpp, ssd, mapped_addr);

	*ns = (struct nvmev_ns){
		.id = id,
		.csi = NVME_CSI_ZNS,
		.nr_parts = nr_parts,
		.ftls = (void *)zns_ftl,
		.size = zpp.logical_size,
		.mapped = mapped_addr,

		/*register io command handler*/
		.proc_io_cmd = zns_proc_nvme_io_cmd,
	};
	return;
}

void zns_remove_namespace(struct nvmev_ns *ns)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;

	ssd_remove(zns_ftl->ssd);

	__remove_descriptor(zns_ftl);
	kfree(zns_ftl->ssd);
	kfree(zns_ftl);

	ns->ftls = NULL;
}

static void zns_flush(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	uint64_t start, latest;
	uint32_t i;
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;

#if (BASE_SSD == ZMS_PROTOTYPE)
	// flush write buffer
	struct zms_ftl *zms_ftl = (struct zms_ftl *)(&(*zns_ftl));

	for (int i = 0; i < zms_ftl->zp.nr_wb; i++) {
		zms_ftl->write_buffer[i].sqid = req->sq_id;
		if (zms_ftl->write_buffer[i].flush_data) {
			buffer_flush(zms_ftl, &(zms_ftl->write_buffer[i]), 0);
		}
	}
#endif

	start = local_clock();
	latest = start;
	for (i = 0; i < ns->nr_parts; i++) {
		latest = max(latest, ssd_next_idle_time(zns_ftl[i].ssd));
	}

	NVMEV_DEBUG("%s latency=%llu\n", __func__, latest - start);

	ret->status = NVME_SC_SUCCESS;
	ret->nsecs_target = latest;
#if (BASE_SSD == ZMS_PROTOTYPE)
	zms_ftl->host_flush_cnt++;
#endif
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

#if (BASE_SSD == ZMS_PROTOTYPE)
bool zms_zoned_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
								struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;
	NVMEV_ASSERT(ns->csi == NVME_CSI_ZNS);
	/*still not support multi partitions ...*/
	NVMEV_ASSERT(ns->nr_parts == 1);

	switch (cmd->common.opcode) {
	case nvme_cmd_write:
	case nvme_cmd_zone_append:
		if (!zoned_write(ns, req, ret))
			return false;
		break;
	case nvme_cmd_read:
		if (!zoned_read(ns, req, ret))
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

bool zms_block_proc_nvme_io_cmd(struct nvmev_ns *ns, struct nvmev_request *req,
								struct nvmev_result *ret)
{
	struct nvme_command *cmd = req->cmd;

	NVMEV_ASSERT(ns->csi == NVME_CSI_NVM);

	switch (cmd->common.opcode) {
	case nvme_cmd_write:
		if (!block_write(ns, req, ret))
			return false;
		break;
	case nvme_cmd_read:
		if (!block_read(ns, req, ret))
			return false;
		break;
	case nvme_cmd_flush:
		zns_flush(ns, req, ret);
		break;
	default:
		NVMEV_ERROR("%s: command not implemented: %s (0x%x)\n", __func__,
					nvme_opcode_string(cmd->common.opcode), cmd->common.opcode);
		break;
	}

	return true;
}

static uint64_t Log2(uint64_t num)
{
	long long int orig = num;
	long long int ret = 0;
	while (num > 1) {
		num /= 2;
		ret++;
	}
	if (orig != 1 << ret)
		ret++;
	return ret;
}

static void zms_init_params(struct znsparams *zpp, uint64_t physical_size, struct nvmev_ns *ns,
							int ns_type)
{
	*zpp = (struct znsparams){
		.ns = ns,
		.ns_type = ns_type,
		.physical_size = physical_size,
		.gc_thres_lines_high = 2,
		.migrate_thres_lines_low = 2,
		.enable_gc_delay = 1,
	};
	if (ns_type == SSD_TYPE_ZMS_META) {
		zpp->logical_size = LOGICAL_META_SIZE;
		zpp->nr_wb = NR_META_WB;
		zpp->zone_wb_size = META_WB_SIZE;
		zpp->pslc_blks = META_pSLC_INIT_BLKS;
		zpp->pre_read = L2P_PREREAD;
		NVMEV_INFO("-------------META PARAMS----------------\n");
		NVMEV_INFO("[Logical Space] %llu MiB [Physical Space] %llu MiB\n",
				   BYTE_TO_MB(zpp->logical_size), BYTE_TO_MB(zpp->physical_size));
		NVMEV_INFO("[Write Buffer Size] %u KiB [# of Write Buffer] %u\n",
				   BYTE_TO_KB(zpp->zone_wb_size), zpp->nr_wb);
		NVMEV_INFO("[# of pSLC Superblocks] %d \n", zpp->pslc_blks);
		return;
	} else if (ns_type == SSD_TYPE_ZMS_BLOCK) {
		zpp->pba_pcent = (int)((1 + OP_AREA_PERCENT) * 100);
		zpp->logical_size =
			(uint64_t)(((zpp->physical_size - DATA_pSLC_RSV_SIZE) * 100) / zpp->pba_pcent);
		zpp->nr_wb = 1;
		zpp->zone_wb_size = ZONE_WB_SIZE;
		zpp->pslc_blks = DATA_pSLC_INIT_BLKS;
		zpp->pre_read = L2P_PREREAD;
		NVMEV_INFO("-------------DATA PARAMS (BLOCK)----------------\n");
		NVMEV_INFO("[Logical Space] %llu MiB [pSLC Reserved Size] %llu MiB [Physical Space] %llu "
				   "MiB\n",
				   BYTE_TO_MB(zpp->logical_size), BYTE_TO_MB(DATA_pSLC_RSV_SIZE),
				   BYTE_TO_MB(zpp->physical_size));
		NVMEV_INFO("[Write Buffer Size] %u KiB [# of Write Buffer] %u\n",
				   BYTE_TO_KB(zpp->zone_wb_size), zpp->nr_wb);
		NVMEV_INFO("[# of pSLC Superblocks] %d \n", zpp->pslc_blks);
		return;
	} else if (ns_type == SSD_TYPE_ZMS_ZONED) {
		uint32_t nr_zones;
		zpp->logical_size = zpp->physical_size - DATA_pSLC_RSV_SIZE;
		zpp->nr_wb = SLC_BYPASS ? ZONE_WB_SIZE / (ONESHOT_PAGE_SIZE * NAND_CHANNELS *
												  LUNS_PER_NAND_CH * PLNS_PER_LUN)
								: ZONE_WB_SIZE / (pSLC_ONESHOT_PAGE_SIZE * NAND_CHANNELS *
												  LUNS_PER_NAND_CH * PLNS_PER_LUN);
		zpp->zone_wb_size = ZONE_WB_SIZE;
		zpp->pslc_blks = DATA_pSLC_INIT_BLKS;

		zpp->zone_capacity = ZONE_SIZE;
		zpp->zone_size = (1 << (Log2(zpp->zone_capacity)));
		zpp->nr_zones = zpp->logical_size / zpp->zone_size;
		zpp->nr_active_zones = zpp->nr_zones; // max
		zpp->nr_open_zones = zpp->nr_zones;	  // max
		zpp->dies_per_zone = DIES_PER_ZONE;
		zpp->chunk_size = CHUNK_SIZE;
		zpp->pgs_per_chunk = CHUNK_SIZE / PG_SIZE;
		zpp->pgs_per_zone = zpp->zone_capacity / PG_SIZE;
		zpp->pre_read = L2P_PREREAD;

		if (zpp->logical_size % zpp->zone_size) {
			NVMEV_INFO("Invalid logical size (%llu MiB) zone size (%u MiB) nr zones %d\n",
					   BYTE_TO_MB(zpp->logical_size), BYTE_TO_MB(zpp->zone_size), zpp->nr_zones);
			zpp->logical_size = ((uint64_t)zpp->nr_zones) * ((uint64_t)zpp->zone_size);
			NVMEV_INFO("New logical size %llu MiB\n", BYTE_TO_MB(zpp->logical_size));
		}

		/* It should be 4KB aligned, according to lpn size */
		if (zpp->zone_size % PG_SIZE) {
			NVMEV_ERROR("%s Invalid zone size (%u KiB) pgsz (%llu KiB)\n", __func__,
						BYTE_TO_KB(zpp->zone_size), BYTE_TO_KB(PG_SIZE));
		}

		NVMEV_INFO("-------------DATA PARAMS (ZONED)----------------\n");
		NVMEV_INFO("[Logical Space] %llu MiB [pSLC Reserved Size] %llu MiB [Physical Space] %llu "
				   "MiB\n",
				   BYTE_TO_MB(zpp->logical_size), BYTE_TO_MB(DATA_pSLC_RSV_SIZE),
				   BYTE_TO_MB(zpp->physical_size));
		NVMEV_INFO("[Write Buffer Size] %u KiB [# of Write Buffer] %u\n",
				   BYTE_TO_KB(zpp->zone_wb_size), zpp->nr_wb);
		NVMEV_INFO("[# of pSLC Superblocks] %d \n", zpp->pslc_blks);
		NVMEV_INFO("[Zone Size] %u MiB [Zone Capacity] %u MiB [# of Zones] %d\n",
				   BYTE_TO_MB(zpp->zone_size), BYTE_TO_MB(zpp->zone_capacity), zpp->nr_zones);
		NVMEV_INFO("[Chunk Size] %u MiB [Logical Pages per Zone] %d [Logical Pages per Chunk] %d\n",
				   BYTE_TO_MB(zpp->chunk_size), zpp->pgs_per_zone, zpp->pgs_per_chunk);
		return;
	}
	NVMEV_ERROR(" %s Invalid SSD Type (%d)\n", __func__, ns_type);

	return;
}

static void zms_init_ftl(struct zms_ftl *zms_ftl, struct znsparams *zpp, void *mapped_addr)
{
	*zms_ftl = (struct zms_ftl){
		.zp = *zpp, /*copy znsparams*/

		.ssd = NULL,
		.storage_base_addr = mapped_addr,
		.last_gc_ppa.ppa = UNMAPPED_PPA,
	};

	__init_descriptor((struct zns_ftl *)(&(*zms_ftl)));
	if (zpp->ns_type == SSD_TYPE_ZMS_ZONED) {
		__init_resource((struct zns_ftl *)(&(*zms_ftl)));
	}
}

static void __init_l2p(struct zms_ftl *zms_ftl)
{
	int i, j;
	struct znsparams *zpp = &zms_ftl->zp;
	struct ssd *ssd = zms_ftl->ssd;
	if (!ssd) {
		NVMEV_ERROR("%s no ssd?\n", __func__);
		return;
	}

	zpp->tt_lpns = DIV_ROUND_UP(zpp->logical_size, ssd->sp.pgsz);
	zms_ftl->maptbl = vmalloc(sizeof(struct ppa) * (zpp->tt_lpns));
	zms_ftl->l2pcache_idx = vmalloc(sizeof(struct ppa) * (zpp->tt_lpns));
	for (i = 0; i < zpp->tt_lpns; i++) {
		zms_ftl->maptbl[i].ppa = UNMAPPED_PPA;
		zms_ftl->l2pcache_idx[i] = -1;
	}

	NVMEV_INFO("[# of L2P Entries (cached/all)] %d / %lld [Evict Policy] %d [Pre Read Pages] %d\n",
			   ssd->l2pcache.size, zpp->tt_lpns, ssd->l2pcache.evict_policy, zms_ftl->zp.pre_read);
}

static void __remove_l2p(struct zms_ftl *zms_ftl)
{
	vfree(zms_ftl->maptbl);
	vfree(zms_ftl->l2pcache_idx);
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
	return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a) { return ((struct zms_line *)a)->vpc; }

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct zms_line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a) { return ((struct zms_line *)a)->pos; }

static inline void victim_line_set_pos(void *a, size_t pos) { ((struct zms_line *)a)->pos = pos; }

static inline int migrating_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
	return (next > curr);
}

static inline pqueue_pri_t migrating_line_get_pri(void *a)
{
	return ((struct migrating_lineid *)a)->write_order;
}

static inline void migrating_line_set_pri(void *a, pqueue_pri_t pri)
{
	((struct migrating_lineid *)a)->write_order = pri;
}

static inline size_t migrating_line_get_pos(void *a) { return ((struct migrating_lineid *)a)->pos; }

static inline void migrating_line_set_pos(void *a, size_t pos)
{
	((struct migrating_lineid *)a)->pos = pos;
}

static void __init_sublines(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	line->sub_lines = vmalloc(sizeof(struct zms_line) * spp->blks_per_line);
	for (int i = 0; i < spp->blks_per_line; i++) {
		line->sub_lines[i] = (struct zms_line){
			.id = i,
			.ipc = 0,
			.vpc = 0,
			.pos = 0,
			.rpc = 0,
			.entry = LIST_HEAD_INIT(line->sub_lines[i].entry),
			.mid.parent_id = line->id,
			.mid.id = i,
			.mid.entry = LIST_HEAD_INIT(line->sub_lines[i].mid.entry),
			.mid.write_order = -1,
			.mid.pos = 0,
			.parent_id = line->id,
			.sub_lines = NULL,
			.rsv_nextline = NULL,
		};
		line->sub_lines[i].blkid = lmid_2_blkid(zms_ftl, &line->sub_lines[i]);
	}
}

static void __remoev_sublines(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	if (line->sub_lines) {
		vfree(line->sub_lines);
	}
}

static void __init_lines(struct zms_ftl *zms_ftl, int pSLC_eline, int interleave_sline)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct znsparams *zpp = &zms_ftl->zp;
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct zms_line *line;
	int i;

	lm->tt_lines = zpp->tt_lines;
	lm->pslc_tt_lines = zpp->pslc_lines;

	lm->lines = vmalloc(sizeof(struct zms_line) * lm->tt_lines);

	INIT_LIST_HEAD(&lm->free_line_list);
	INIT_LIST_HEAD(&lm->full_line_list);

	lm->victim_line_pq = pqueue_init(lm->tt_lines, victim_line_cmp_pri, victim_line_get_pri,
									 victim_line_set_pri, victim_line_get_pos, victim_line_set_pos);
	INIT_LIST_HEAD(&lm->pslc_free_line_list);
	INIT_LIST_HEAD(&lm->pslc_full_line_list);

	lm->pslc_victim_line_pq =
		pqueue_init(lm->tt_lines, victim_line_cmp_pri, victim_line_get_pri, victim_line_set_pri,
					victim_line_get_pos, victim_line_set_pos);

	lm->free_line_cnt = 0;
	lm->pslc_free_line_cnt = 0;

	for (i = 0; i < lm->tt_lines; i++) {
		lm->lines[i] = (struct zms_line){
			.id = i,
			.ipc = 0,
			.vpc = 0,
			.pos = 0,
			.rpc = 0,
			.entry = LIST_HEAD_INIT(lm->lines[i].entry),
			.mid.parent_id = -1,
			.mid.id = i,
			.mid.entry = LIST_HEAD_INIT(lm->lines[i].mid.entry),
			.mid.write_order = 0,
			.mid.pos = 0,
			.parent_id = -1,
			.sub_lines = NULL,
			.rsv_nextline = NULL,
		};

		lm->lines[i].blkid = lmid_2_blkid(zms_ftl, &lm->lines[i]);
		struct nand_block *blk = line_2_blk(zms_ftl, &lm->lines[i]);
		NVMEV_INFO("Line id %d -> Block id %d nand type %d\n", lm->lines[i].id, lm->lines[i].blkid,
				   blk->nand_type);

		if (i < interleave_sline) {
			__init_sublines(zms_ftl, &lm->lines[i]);
		}

		if (i < pSLC_eline) {
			/* initialize all the lines as free lines */
			if (i < interleave_sline) {
				for (int j = 0; j < spp->blks_per_line; j++) {
					list_add_tail(&lm->lines[i].sub_lines[j].entry, &lm->pslc_free_line_list);
					lm->pslc_free_line_cnt++;
					lm->lines[i].sub_lines[j].pgs_per_line = spp->pslc_pgs_per_blk;
				}
			} else {
				list_add_tail(&lm->lines[i].entry, &lm->pslc_free_line_list);
				lm->pslc_free_line_cnt++;
			}
			lm->lines[i].pgs_per_line = zpp->pslc_pgs_per_line;
		} else {
			/* initialize all the lines as free lines */
			if (i < interleave_sline) {
				for (int j = 0; j < spp->blks_per_line; j++) {
					list_add_tail(&lm->lines[i].sub_lines[j].entry, &lm->free_line_list);
					lm->free_line_cnt++;
					lm->lines[i].sub_lines[j].pgs_per_line = spp->pgs_per_blk;
				}
			} else {
				list_add_tail(&lm->lines[i].entry, &lm->free_line_list);
				lm->free_line_cnt++;
			}
			lm->lines[i].pgs_per_line = zpp->pgs_per_line;
		}

		// NVMEV_INFO("misao: %s add %d curr line
		// %p\n",pSLC?"slc":"qlc",i,list_first_entry_or_null(&lm->free_line_list, struct zms_line,
		// entry));
	}

	lm->victim_line_cnt = 0;
	lm->full_line_cnt = 0;

	lm->pslc_victim_line_cnt = 0;
	lm->pslc_full_line_cnt = 0;

	NVMEV_INFO("Line Management: [# of Top Level Lines] %d [Interleave Lines] %u [pSLC Lines] %u\n",
			   lm->tt_lines, lm->tt_lines - interleave_sline, pSLC_eline);
	NVMEV_INFO("Line Management: [Free Normal Lines] %u [Free pSLC Lines] %u\n", lm->free_line_cnt,
			   lm->pslc_free_line_cnt);
}

static void __remove_lines(struct zms_ftl *zms_ftl)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	for (int i = 0; i < lm->tt_lines; i++) {
		if (lm->lines[i].sub_lines) {
			__remoev_sublines(zms_ftl, &lm->lines[i]);
		}
	}
	pqueue_free(lm->victim_line_pq);
	pqueue_free(lm->pslc_victim_line_pq);
	vfree(lm->lines);
}

struct zms_write_pointer *zms_get_wp(struct zms_ftl *zms_ftl, uint32_t io_type, int location)
{
	if (location == LOC_NORMAL) {
		// TLC:
		// 	BLOCK: USER_I/O  + GC I/O
		//	PAGE-SLC: USER_I/O
		//  ZONED-SLC: USER_I/O
		// QLC:
		// 	BLOCK:  Migrate I/O + GC I/O
		// 	PAGE-SLC: Migrate I/O(FORCE)
		// 	ZONED-SLC: Migrate I/O(FORCE)
		return io_type == GC_IO ? &zms_ftl->gc_wp : &zms_ftl->wp;
	} else {
		// TLC:
		//	BLOCK: USER I/O + GC I/O
		//	PAGE-SLC:  USER I/O
		//	ZONED-SLC: USER I/O
		// QLC:
		// 	BLOCK: USER I/O + Migrate I/O + GC I/O
		// 	PAGE-SLC:  USER I/O + Migrate I/O(FORCE)
		// 	ZONED-SLC: USER I/O
		return io_type == USER_IO ? &zms_ftl->pslc_wp : &zms_ftl->pslc_gc_wp;
	}
}

struct zms_line *get_next_free_line(struct zms_ftl *zms_ftl, int location)
{
	struct list_head *free_line_list = zms_get_free_list(zms_ftl, location);
	struct zms_line *curline = list_first_entry_or_null(free_line_list, struct zms_line, entry);

	if (!curline) {
		NVMEV_ERROR("ns %d No free line left in %s VIRT !!!!\n", zms_ftl->zp.ns->id,
					location == LOC_PSLC ? "pslc" : "normal");
		print_lines(zms_ftl);
		if (location == LOC_PSLC) {
			if (zms_ftl->zp.ns_type == SSD_TYPE_ZMS_ZONED) {
				int *agg_len = zms_ftl->zone_agg_pgs;
				uint64_t **agg = zms_ftl->zone_agg_lpns;
				for (int i = 0; i < zms_ftl->zp.nr_zones; i++) {
					print_agg(zms_ftl, agg_len[i], agg[i]);
				}
			}

			struct ssd *ssd = zms_ftl->ssd;
			struct ssdparams *spp = &ssd->sp;
			struct ppa ppa;
			struct zms_line *sblk_line;
			int lun, ch;
			for (lun = 0; lun < spp->luns_per_ch; lun++) {
				for (ch = 0; ch < spp->nchs; ch++) {
					struct nand_lun *lunp;

					ppa.g.ch = ch;
					ppa.g.lun = lun;
					ppa.g.pl = 0;
					lunp = get_lun(zms_ftl->ssd, &ppa);
					if (lunp->migrating && lunp->migrating_etime > zms_ftl->current_time)
						NVMEV_INFO("ch %d lun %d migrating... next free %lld us\n", ch, lun,
								   (lunp->next_lun_avail_time - zms_ftl->current_time) / 1000);
					else
						NVMEV_INFO("ch %d lun %d idle \n", ch, lun);
				}
			}

			zms_ftl->pslc_full = 1;
		} else {
			zms_ftl->device_full = 1;
		}
		return NULL;
	}

	list_del_init(&curline->entry);
	dec_free_cnt(zms_ftl, location);
	return curline;
}

static void prepare_write_pointer(struct zms_ftl *zms_ftl, uint32_t io_type, int location)
{
	struct zms_write_pointer *wp = zms_get_wp(zms_ftl, io_type, location);
	struct zms_line *curline = get_next_free_line(zms_ftl, location);
	struct ssdparams *spp = &zms_ftl->ssd->sp;

	NVMEV_ASSERT(wp);
	NVMEV_ASSERT(curline);

	/* wp->curline is always our next-to-write super-block */
	*wp = (struct zms_write_pointer){
		.curline = curline,
		.loc = location,
	};
	struct ppa ppa = get_first_page(zms_ftl, curline);
	update_write_pointer(wp, ppa);

	NVMEV_INFO("prepared %s wp for %s IO, line id %d blk id %d\n",
			   location == LOC_PSLC ? "slc" : "normal", io_type == GC_IO ? "GC" : "USER",
			   curline->id, wp->blk);
}

static void __init_rmap(struct zms_ftl *zms_ftl)
{
	int i;
	struct znsparams *zpp = &zms_ftl->zp;
	struct ssd *ssd = zms_ftl->ssd;
	if (!ssd) {
		NVMEV_ERROR("%s no ssd??\n", __func__);
		return;
	}

	zpp->tt_ppns = zpp->tt_lines * zpp->pgs_per_line;
	// DIV_ROUND_UP(zpp->physical_size, ssd->sp.pgsz);

	zms_ftl->rmap = vmalloc(sizeof(struct ppa) * (zpp->tt_ppns));
	for (i = 0; i < zpp->tt_ppns; i++) {
		zms_ftl->rmap[i] = INVALID_LPN;
	}
	NVMEV_INFO("[# of RMap Entries] %llu\n", zpp->tt_ppns);
}

static void __remove_rmap(struct zms_ftl *zms_ftl) { vfree(zms_ftl->rmap); }

static void zms_realize_ftl(struct zms_ftl *zms_ftl)
{
	int i, j;
	struct ssd *ssd = zms_ftl->ssd;
	int pslc_lines, normal_lines;
	bool interleave = true;
	if (!ssd) {
		NVMEV_ERROR(" ssd is null!\n");
		return;
	}

	struct znsparams *zpp = &zms_ftl->zp;
	// for debug
	zms_ftl->nopg_last_lpn = 0;
	zms_ftl->last_slba = 0;
	zms_ftl->last_nlb = 0;
	zms_ftl->device_full = 0;
	zms_ftl->pslc_full = 0;

	__init_l2p(zms_ftl);

	zpp->tt_lines =
		DIV_ROUND_UP(zpp->physical_size, (ssd->sp.blksz * ssd->sp.luns_per_ch * ssd->sp.nchs));
	zpp->pslc_lines = zpp->pslc_blks;
	zpp->pgs_per_line = ssd->sp.pgs_per_line;
	zpp->pslc_pgs_per_line = ssd->sp.pslc_pgs_per_line;

	int interleave_sline = 0;
	int pslc_eline = zpp->pslc_lines;

	if ((zpp->ns_type == SSD_TYPE_ZMS_ZONED && zpp->pgs_per_zone < ssd->sp.pgs_per_line) ||
		zpp->tt_lines < 4) {
		interleave_sline = zpp->tt_lines;
	} else if (zpp->pslc_lines < 4) {
		interleave_sline = zpp->pslc_lines;
	}

	__init_lines(zms_ftl, pslc_eline, interleave_sline);
	// init normal area
	if (zpp->tt_lines - zpp->pslc_lines > 0) {
		if (zpp->ns_type == SSD_TYPE_ZMS_META) {
			NVMEV_ERROR("The meta data is only written to the SLC! tt lines %lu pslc lines %lu\n",
						zpp->tt_lines, zpp->pslc_lines);
			NVMEV_ASSERT(0);
		}
		prepare_write_pointer(zms_ftl, USER_IO, LOC_NORMAL);
		if (zpp->ns_type != SSD_TYPE_ZMS_ZONED)
			prepare_write_pointer(zms_ftl, GC_IO, LOC_NORMAL);
	}

	// init pSLC area
	if (zpp->pslc_lines > 0) {
		prepare_write_pointer(zms_ftl, USER_IO, LOC_PSLC);
		if (!ZONED_SLC) {
			prepare_write_pointer(zms_ftl, GC_IO, LOC_PSLC);
		}
	}

	NVMEV_INFO("[Total Lines] %lu [pSLC Lines] %lu [Normal lines] %lu\n", zpp->tt_lines,
			   zpp->pslc_lines, zpp->tt_lines - zpp->pslc_lines);

	zms_ftl->wfc.write_credits = zpp->pgs_per_line;
	zms_ftl->wfc.credits_to_refill = zpp->pgs_per_line;

	zms_ftl->pslc_wfc.write_credits = zpp->pslc_pgs_per_line;
	zms_ftl->pslc_wfc.credits_to_refill = zpp->pslc_pgs_per_line;

	__init_rmap(zms_ftl);

	// for pSLC->QLC migration in zoned device
	zms_ftl->num_aggs = 1;
	if (zms_ftl->zp.ns_type == SSD_TYPE_ZMS_ZONED) {
		zms_ftl->num_aggs = zms_ftl->zp.nr_zones;
	}
	zms_ftl->zone_agg_pgs = kzalloc(sizeof(int) * zms_ftl->num_aggs, GFP_KERNEL);
	zms_ftl->zone_agg_lpns = kmalloc(sizeof(uint64_t *) * zms_ftl->num_aggs, GFP_KERNEL);
	zms_ftl->zone_write_unit = ssd->sp.pgs_per_oneshotpg;
	for (int i = 0; i < zms_ftl->num_aggs; i++) {
		zms_ftl->zone_agg_pgs[i] = 0;
		zms_ftl->zone_agg_lpns[i] =
			kmalloc(sizeof(uint64_t) * (zms_ftl->zone_write_unit), GFP_KERNEL);
	}

	NVMEV_INFO("[Num Aggs] %d\n", zms_ftl->num_aggs);
	zms_ftl->migrating_line_pq =
		pqueue_init(zpp->tt_lines, migrating_line_cmp_pri, migrating_line_get_pri,
					migrating_line_set_pri, migrating_line_get_pos, migrating_line_set_pos);
}

void zms_init_namespace(struct nvmev_ns *ns, uint32_t id, uint64_t size, void *mapped_addr,
						uint32_t cpu_nr_dispatcher)
{
	struct zms_ftl *zms_ftl;
	struct znsparams zpp;
	const uint32_t nr_parts = 1; /* Not support multi partitions for zms*/

	zms_ftl = kmalloc(sizeof(struct zms_ftl) * nr_parts, GFP_KERNEL);
	memset(&zpp, 0, sizeof(struct znsparams));
	memset(zms_ftl, 0, sizeof(struct zms_ftl));
	zms_init_params(&zpp, size, ns, NS_SSD_TYPE(id));
	zms_init_ftl(zms_ftl, &zpp, mapped_addr);

	*ns = (struct nvmev_ns){
		.id = id,
		.csi = zpp.ns_type == SSD_TYPE_ZMS_ZONED ? NVME_CSI_ZNS : NVME_CSI_NVM,
		.nr_parts = nr_parts,
		.ftls = (void *)zms_ftl,
		.size = zpp.logical_size,
		.mapped = mapped_addr,

		/*register io command handler*/
		.proc_io_cmd = zpp.ns_type == SSD_TYPE_ZMS_ZONED ? zms_zoned_proc_nvme_io_cmd
														 : zms_block_proc_nvme_io_cmd,
	};
	NVMEV_INFO("---------zms init %s namespace id %d csi %d ftl %p--------------\n",
			   zpp.ns_type == SSD_TYPE_ZMS_ZONED ? "zoned" : "block", id, ns->csi, zms_ftl);
	return;
}

void zms_print_statistic_info(struct zms_ftl *zms_ftl)
{
	NVMEV_INFO("------------MISAO--device %d statistic info-----------\n", zms_ftl->zp.ns->id);
	NVMEV_INFO("[# of Zone Resets] %lld [# of Zone Writes] %lld\n", zms_ftl->zone_reset_cnt,
			   zms_ftl->zone_write_cnt);
	NVMEV_INFO("[# of Host Read Requests] %lld\n", zms_ftl->host_rrequest_cnt);
	NVMEV_INFO("[# of Host Write Requests] %lld\n", zms_ftl->host_wrequest_cnt);
	NVMEV_INFO("[# of Host Flush Requests] %lld\n", zms_ftl->host_flush_cnt);
	NVMEV_INFO("[Host Read Pages] %lld\n", zms_ftl->host_r_pgs);
	NVMEV_INFO("[L2P Miss Rate] (%lld/%lld) [WB Hits] %lld [Unmapped Read Cnt] %lld\n",
			   zms_ftl->l2p_misses, zms_ftl->l2p_misses + zms_ftl->l2p_hits, zms_ftl->read_wb_hits,
			   zms_ftl->unmapped_read_cnt);
	NVMEV_INFO("[WAF] (%lld/%lld) [Migrated Logical Pages] %lld\n", zms_ftl->device_w_pgs,
			   zms_ftl->host_w_pgs, zms_ftl->migration_pgs);
	NVMEV_INFO("[# of Normal Line Earse] %d [# of pSLC Line Erase] %d\n", zms_ftl->normal_erase_cnt,
			   zms_ftl->slc_erase_cnt);
	NVMEV_INFO("[# of Garbage Collection] %d\n", zms_ftl->gc_count);
	NVMEV_INFO("[# of Early Flush] %d\n", zms_ftl->early_flush_cnt);
	NVMEV_INFO("[pSLC lines] %d/%d/%d/%d [normal lines] %d/%d/%d/%d "
			   "(free/full/victim/all)\n",
			   zms_ftl->lm.pslc_free_line_cnt, zms_ftl->lm.pslc_full_line_cnt,
			   zms_ftl->lm.pslc_victim_line_cnt, zms_ftl->lm.pslc_tt_lines,
			   zms_ftl->lm.free_line_cnt, zms_ftl->lm.full_line_cnt, zms_ftl->lm.victim_line_cnt,
			   zms_ftl->lm.tt_lines - zms_ftl->lm.pslc_tt_lines);
	NVMEV_INFO("[# of inplace update] %d\n", zms_ftl->inplace_update);
}

void zms_remove_namespace(struct nvmev_ns *ns)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;

	zms_print_statistic_info(zms_ftl);

	__remove_descriptor((struct zns_ftl *)(&(*zms_ftl)));
	__remove_rmap(zms_ftl);
	__remove_lines(zms_ftl);
	__remove_l2p(zms_ftl);

	kfree(zms_ftl->zone_agg_pgs);
	for (int i = 0; i < zms_ftl->num_aggs; i++) {
		kfree(zms_ftl->zone_agg_lpns[i]);
	}
	kfree(zms_ftl->zone_agg_lpns);
	pqueue_free(zms_ftl->migrating_line_pq);
	kfree(zms_ftl);

	ns->ftls = NULL;
}

void zms_remove_ssd(struct nvmev_ns *ns)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;

	ssd_remove(zms_ftl->ssd);
	kfree(zms_ftl->ssd);
}

void zms_realize_namespaces(struct nvmev_ns *ns, int nr_ns, uint64_t size,
							uint32_t cpu_nr_dispatcher)
{
	struct ssd *ssd = NULL;
	struct ssdparams spp;
	int i;
	const uint32_t nr_parts = 1; /* Not support multi partitions for zns*/

	ssd = kmalloc(sizeof(struct ssd), GFP_KERNEL);
	memset(&spp, 0, sizeof(struct ssdparams));
	ssd_init_params(&spp, size, nr_parts);
	ssd_init(ssd, &spp, cpu_nr_dispatcher);

	for (i = 0; i < nr_ns; i++) {
		if (!ns[i].ftls) {
			NVMEV_ERROR(" ftl in ns %d has not been inited!\n", i);
			continue;
		}
		struct zms_ftl *zms_ftl = (struct zms_ftl *)ns[i].ftls;
		zms_ftl->ssd = ssd;
		zms_realize_ftl(zms_ftl);
		NVMEV_INFO("--------------- realize %s namespace %d ssd %p--------------\n",
				   zms_ftl->zp.ns_type == SSD_TYPE_ZMS_ZONED ? "zoned" : "block", i, zms_ftl->ssd);
	}

	return;
}

#endif