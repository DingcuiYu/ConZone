// SPDX-License-Identifier: GPL-2.0-only

#include <linux/time.h>
#include <linux/vmalloc.h>

#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"
#if (BASE_SSD == CONZONE_PROTOTYPE)
static inline bool check_resident(struct zms_ftl *zms_ftl, int gran)
{
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && L2P_HYBRID_MAP_RESIDENT)
		return gran != PAGE_MAP;
	return 0;
}

static inline void check_addr(int a, int max)
{
	// NVMEV_ASSERT(a >= 0 && a < max);
	if (a >= 0 && a < max)
		return;
	NVMEV_ERROR("%s target : %d ,max : %d\n", __func__, a, max);
}

static inline bool mapped_ppa(struct ppa *ppa) { return !(ppa->ppa == UNMAPPED_PPA); }

static inline bool valid_lpn(struct zms_ftl *zms_ftl, uint64_t lpn)
{
	return (lpn < zms_ftl->zp.tt_lpns);
}

static int check_location_type(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	if (!mapped_ppa(ppa)) {
		NVMEV_ERROR("ppa unmapped! return location type -1\n");
		return -1;
	}
	struct ssd *ssd = zms_ftl->ssd;
	struct nand_block *blk = get_blk(ssd, ppa);
	switch (blk->nand_type) {
	case CELL_MODE_SLC:
		return LOC_PSLC;
	default:
		return LOC_NORMAL;
	}
	// return (ppa->zms.blk < ssd->sp.pslc_blks);
}

static int get_line_location(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct nand_block *blk = line_2_blk(zms_ftl, line);
	switch (blk->nand_type) {
	case CELL_MODE_SLC:
		return LOC_PSLC;
	default:
		return LOC_NORMAL;
	}
}

static struct zms_line *get_line(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	int lmid;
	struct zms_line *line;
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META) {
		lmid = ppa->zms.blk;
	} else {
		lmid = ppa->zms.blk - (spp->meta_pslc_blks + spp->meta_normal_blks);
	}
	if (lmid >= lm->tt_lines) {
		NVMEV_ERROR(
			"lmid >= lm->tt_line! ppa blk %d meta pslc blk %d meta normal blk %d lmid %d tt "
			"lines %d\n",
			ppa->zms.blk, spp->meta_pslc_blks, spp->meta_normal_blks, lmid, lm->tt_lines);
		NVMEV_ASSERT(0);
	}

	line = &lm->lines[lmid];
	if (lm->lines[lmid].sub_lines) {
		int sublmid = ppa->zms.ch * spp->nchs + ppa->zms.lun;
		if (sublmid >= spp->blks_per_line) {
			NVMEV_ERROR(
				"sublmid >= spp->blks_per_line! ppa (ch %d lun %d) sublm id %d blks per line "
				"%ld \n",
				ppa->zms.ch, ppa->zms.lun, sublmid, spp->blks_per_line);
			NVMEV_ASSERT(0);
		}
		line = &lm->lines[lmid].sub_lines[sublmid];
	}
	return line;
}

static uint64_t ppa_2_pgidx(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct zms_line *line = get_line(zms_ftl, ppa);
	int lineid = (line->parent_id == -1) ? line->id : line->parent_id;
	uint64_t pgidx;

	// Mapped as interleave, but does not represent the actual address iterations

	pgidx = lineid * spp->pgs_per_blk * spp->nchs * spp->luns_per_ch * spp->pls_per_lun;
	pgidx += (ppa->zms.pg / spp->pgs_per_oneshotpg) *
			 (spp->pgs_per_oneshotpg * spp->nchs * spp->luns_per_ch);
	// chip offset
	pgidx += (ppa->zms.lun * spp->nchs + ppa->zms.ch) * spp->pgs_per_oneshotpg;

	// pg offset
	pgidx += ppa->zms.pg % spp->pgs_per_oneshotpg;
	return pgidx;
}

static bool ppa_same(struct ppa pa, struct ppa pb)
{
	pa.g.rsv = 0;
	pb.g.rsv = 0;
	return pa.ppa == pb.ppa;
}

static bool flashpage_same(struct zms_ftl *zms_ftl, struct ppa ppa1, struct ppa ppa2)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	uint32_t ppa1_page = ppa1.g.pg / spp->pgs_per_flashpg;
	uint32_t ppa2_page = ppa2.g.pg / spp->pgs_per_flashpg;

	return (ppa1.h.blk_in_ssd == ppa2.h.blk_in_ssd) && (ppa1_page == ppa2_page);
}

static int get_pages_per_granularity(struct zms_ftl *zms_ftl, int granularity)
{
	int pgs = 1;
	switch (granularity) {
	case PAGE_MAP:
		pgs = 1;
		break;
	case CHUNK_MAP:
		pgs = zms_ftl->zp.pgs_per_chunk;
		break;
	case ZONE_MAP:
		pgs = zms_ftl->zp.pgs_per_zone;
		break;
	case SUB_ZONE_MAP:
		pgs = zms_ftl->zp.pslc_pgs_per_line;
		break;
	default:
		NVMEV_ERROR("%s Invalid gran:%d\n", __func__, granularity);
		// NVMEV_ASSERT(0);
		break;
	}
	return pgs;
}

static uint64_t get_granularity_start_lpn(struct zms_ftl *zms_ftl, uint64_t lpn, int granularity)
{
	uint64_t offset = lpn;
	uint64_t slpn = 0;
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
		slpn = zone_to_slpn((struct zns_ftl *)(&(*zms_ftl)),
							lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), lpn));
		offset = lpn - slpn;
	}
	int pgs = get_pages_per_granularity(zms_ftl, granularity);
	if (pgs == 0) {
		NVMEV_ERROR("pgs == 0? nsid %d granu %d \n", zms_ftl->zp.ns->id, granularity);
		return lpn;
	}
	uint64_t au_slpn = offset / pgs * pgs + slpn;
	return au_slpn;
}

struct list_head *zms_get_free_list(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct list_head *line_list;
	switch (location) {
	case LOC_PSLC:
		line_list = &lm->pslc_free_line_list;
		break;
	default:
		line_list = &lm->free_line_list;
		break;
	}
	return line_list;
}

struct list_head *zms_get_full_list(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct list_head *line_list;
	switch (location) {
	case LOC_PSLC:
		line_list = &lm->pslc_full_line_list;
		break;
	default:
		line_list = &lm->full_line_list;
		break;
	}
	return line_list;
}

struct pqueue_t *zms_get_victim_pq(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct pqueue_t *victim_pq = NULL;
	switch (location) {
	case LOC_PSLC:
		victim_pq = lm->pslc_victim_line_pq;
		break;
	default:
		victim_pq = lm->victim_line_pq;
		break;
	}
	return victim_pq;
}

void dec_free_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_free_line_cnt--;
		break;
	default:
		lm->free_line_cnt--;
		break;
	}
}

void inc_free_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_free_line_cnt++;
		break;
	default:
		lm->free_line_cnt++;
		break;
	}
}

void dec_victim_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_victim_line_cnt--;
		break;
	default:
		lm->victim_line_cnt--;
		break;
	}
}

void inc_victim_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_victim_line_cnt++;
		break;
	default:
		lm->victim_line_cnt++;
		break;
	}
}

void dec_full_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_full_line_cnt--;
		break;
	default:
		lm->full_line_cnt--;
		break;
	}
}

void inc_full_cnt(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	switch (location) {
	case LOC_PSLC:
		lm->pslc_full_line_cnt++;
		break;
	default:
		lm->full_line_cnt++;
		break;
	}
}

void dec_line_rpc(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct zms_line *line;
	line = get_line(zms_ftl, ppa);
	line->rpc--;
	// if(check_location_type(zms_ftl,ppa)==LOC_PSLC)
	// 	NVMEV_INFO("%s line %d rpc %d\n",__func__,line->id,line->rpc);
}

struct ppa get_first_page(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct ppa ppa;
	ppa.ppa = 0;
	ppa.zms.blk = line->blkid;
	if (line->parent_id != -1) {
		ppa.zms.ch = line->id % spp->nchs;
		ppa.zms.lun = (line->id / spp->nchs) % spp->luns_per_ch;
		ppa.zms.pl = (line->id / (spp->nchs * spp->luns_per_ch)) % spp->pls_per_lun;
	}
	return ppa;
}

int lmid_2_blkid(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	int lmblk = line->parent_id == -1 ? line->id : line->parent_id;
	int blkid;

	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META) {
		blkid = lmblk;
	} else {
		blkid = lmblk + spp->meta_normal_blks + spp->meta_pslc_blks;
	}
	return blkid;
}

struct nand_block *line_2_blk(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct ppa ppa = get_first_page(zms_ftl, line);
	return get_blk(zms_ftl->ssd, &ppa);
}

void print_ppa(struct ppa ppa)
{
	NVMEV_INFO("ppa ch %d lun %d pl %d blk %d pg %d\n", ppa.zms.ch, ppa.zms.lun, ppa.zms.pl,
			   ppa.zms.blk, ppa.zms.pg);
}

void print_agg(struct zms_ftl *zms_ftl, int agg_len, uint64_t *agg_lpns)
{
	NVMEV_INFO("------------%s--------------\n", __func__);
	for (int i = 0; i < agg_len; i++) {
		uint64_t lpn = agg_lpns[i];
		struct ppa ppa = get_maptbl_ent(zms_ftl, lpn);
		if (mapped_ppa(&ppa)) {
			NVMEV_INFO("lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n", lpn, ppa.zms.ch,
					   ppa.zms.lun, ppa.zms.pl, ppa.zms.blk, ppa.zms.pg);
		} else {
			NVMEV_INFO("lpn %lld -> UNMAPPED PPA\n", lpn);
		}
	}
	NVMEV_INFO("------------%s--------------\n", __func__);
}

void print_lines(struct zms_ftl *zms_ftl)
{
	NVMEV_INFO("------------%s--------------\n", __func__);
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	int pslc_active_line = 0, active_line = 0;
	if (zms_ftl->pslc_wp.curline) {
		NVMEV_INFO("[pSLC] current line id (%d,%d)\n", zms_ftl->pslc_wp.curline->parent_id,
				   zms_ftl->pslc_wp.curline->id);
		pslc_active_line++;
	}
	if (zms_ftl->pslc_gc_wp.curline) {
		NVMEV_INFO("[pSLC] current gc line id (%d,%d)\n", zms_ftl->pslc_gc_wp.curline->parent_id,
				   zms_ftl->pslc_gc_wp.curline->id);
		pslc_active_line++;
	}
	if (zms_ftl->wp.curline) {
		NVMEV_INFO("[NORMAL] current line id (%d,%d)\n", zms_ftl->wp.curline->parent_id,
				   zms_ftl->wp.curline->id);
		active_line++;
	}
	if (zms_ftl->gc_wp.curline) {
		NVMEV_INFO("[NORMAL] current gc line id (%d,%d)\n", zms_ftl->gc_wp.curline->parent_id,
				   zms_ftl->gc_wp.curline->id);
		active_line++;
	}

	NVMEV_INFO("[pSLC] free line cnt %d victim line cnt %d full line cnt %d active line cnt %d\n",
			   lm->pslc_free_line_cnt, lm->pslc_victim_line_cnt, lm->pslc_full_line_cnt,
			   pslc_active_line);
	NVMEV_INFO(
		"[NORMAL] free line cnt %d victim line cnt %d full line cnt %d active line cnt %d \n",
		lm->free_line_cnt, lm->victim_line_cnt, lm->full_line_cnt, active_line);
	for (int i = 0; i < lm->tt_lines; i++) {
		struct nand_block *blk = line_2_blk(zms_ftl, &lm->lines[i]);
		NVMEV_INFO("line id: %d vpc: %d ipc: %d rpc :%d blkid %d nand type %d\n", i,
				   lm->lines[i].vpc, lm->lines[i].ipc, lm->lines[i].rpc, lm->lines[i].blkid,
				   blk->nand_type);
		if (lm->lines[i].sub_lines) {
			for (int j = 0; j < spp->blks_per_line; j++) {
				struct nand_block *blk = line_2_blk(zms_ftl, &lm->lines[i].sub_lines[j]);
				NVMEV_INFO("subline id: %d vpc: %d ipc: %d rpc :%d blkid %d nand type %d\n", j,
						   lm->lines[i].sub_lines[j].vpc, lm->lines[i].sub_lines[j].ipc,
						   lm->lines[i].sub_lines[j].rpc, lm->lines[i].sub_lines[j].blkid,
						   blk->nand_type);
			}
		}
	}
	NVMEV_INFO("------------%s--------------\n", __func__);
}

void print_zone_mapping(struct zms_ftl *zms_ftl, uint32_t zid)
{
	uint64_t slpn = zone_to_slpn((struct zns_ftl *)(&(*zms_ftl)), zid);
	uint64_t elpn = zone_to_elpn((struct zns_ftl *)(&(*zms_ftl)), zid);
	NVMEV_INFO("------------%s--------------\n", __func__);
	for (uint64_t lpn = slpn; lpn < elpn; lpn++) {
		struct ppa ppa = get_maptbl_ent(zms_ftl, lpn);
		if (mapped_ppa(&ppa)) {
			NVMEV_INFO("LPN %llu -> PPA %llu (ch %d lun %d pl %d blk %d pg %d)\n", lpn,
					   ppa_2_pgidx(zms_ftl, &ppa), ppa.zms.ch, ppa.zms.lun, ppa.zms.pl, ppa.zms.blk,
					   ppa.zms.pg);
		} else {
			NVMEV_INFO("LPN %llu -> UNMAPPED\n", lpn);
		}
	}
	NVMEV_INFO("------------%s--------------\n", __func__);
}

static void print_writebuffer_info(struct zms_ftl *zms_ftl)
{
	NVMEV_INFO("------------%s--------------\n", __func__);
	for (int i = 0; i < zms_ftl->zp.nr_wb; i++) {
		NVMEV_INFO("write buffer %d zid %d flushing %d flush data %ld pgs %lld slpn "
				   "%lld \n",
				   i, zms_ftl->write_buffer[i].zid, zms_ftl->write_buffer[i].flushing,
				   zms_ftl->write_buffer[i].flush_data, zms_ftl->write_buffer[i].pgs,
				   zms_ftl->write_buffer[i].lpns[0]);
	}
	NVMEV_INFO("------------%s--------------\n", __func__);
}

static uint64_t submit_nand_cmd(struct ssd *ssd, struct nand_cmd *command)
{
	struct nand_cmd *cmd = kmalloc(sizeof(struct nand_cmd), GFP_KERNEL);
	cmd->type = command->type;
	cmd->cmd = command->cmd;
	cmd->stime = command->stime;
	cmd->xfer_size = command->xfer_size;
	cmd->ppa = command->ppa;
	cmd->interleave_pci_dma = command->interleave_pci_dma;
	cmd->ctime = -1;
	INIT_LIST_HEAD(&cmd->entry);
	return ssd_advance_nand(ssd, cmd);
}

// O(1) access & replace
// only len==size
static int l2p_replace(struct zms_ftl *zms_ftl, uint64_t la, int gran, int res)
{
	int evict_idx;
	struct l2pcache *cache = &zms_ftl->ssd->l2pcache;
	uint64_t slot = la % cache->num_slots;
	switch (cache->evict_policy) {
	case L2P_EVICTION_POLICY_NONE:
	case L2P_EVICTION_POLICY_LRU:
		evict_idx = cache->head[slot];
		while (evict_idx != cache->tail[slot] && cache->mapping[slot][evict_idx].resident) {
			evict_idx = cache->mapping[slot][evict_idx].next;
		}

		if (evict_idx == cache->tail[slot] && cache->mapping[slot][evict_idx].resident) {
			return -1; // no free space to evict
		}
		if (evict_idx == cache->tail[slot])
			break;

		if (evict_idx == cache->head[slot]) {
			cache->head[slot] = cache->mapping[slot][evict_idx].next;
			cache->mapping[slot][cache->head[slot]].last = -1;
		} else {
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
		NVMEV_ERROR("Invalid L2P Cache Evict Policy %d\n", cache->evict_policy);
		return -1;
	}

	cache->mapping[slot][evict_idx].lpn = la;
	cache->mapping[slot][evict_idx].granularity = gran;
	cache->mapping[slot][evict_idx].resident = res;
	return evict_idx;
}

static void l2p_access(struct zms_ftl *zms_ftl, uint64_t la, int idx)
{
	struct l2pcache *cache = &zms_ftl->ssd->l2pcache;
	uint64_t slot = la % cache->num_slots;
	if (idx == -1) {
		NVMEV_ERROR("idx == -1?:lpn 0x%llx slot %lld slot head %d slot tail %d\n", la, slot,
					cache->head[slot], cache->tail[slot]);
		return;
	}
	if (idx != cache->tail[slot]) {
		if (idx == cache->head[slot]) {
			cache->head[slot] = cache->mapping[slot][idx].next;
			cache->mapping[slot][cache->head[slot]].last = -1;
		} else {
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

static int l2p_insert(struct zms_ftl *zms_ftl, uint64_t la, int gran, int res)
{
	struct l2pcache *cache = &zms_ftl->ssd->l2pcache;
	uint64_t slot = la % cache->num_slots;
	if (cache->slot_len[slot] == cache->slot_size)
		return l2p_replace(zms_ftl, la, gran, res);
	int idx = cache->slot_len[slot];
	int tail = cache->tail[slot];

	cache->mapping[slot][idx].lpn = la;
	cache->mapping[slot][idx].granularity = gran;
	cache->mapping[slot][idx].resident = res;

	cache->mapping[slot][idx].next = -1;
	if (idx == 0) {
		cache->mapping[slot][idx].last = -1;
	} else {
		cache->mapping[slot][idx].last = tail;

		cache->mapping[slot][tail].next = idx;
		cache->tail[slot] = idx;
	}
	cache->slot_len[slot]++;
	return idx;
}

// for write
static inline bool is_last_pg_in_wordline(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	int pgs_per_oneshotpg = check_location_type(zms_ftl, ppa) == LOC_PSLC
								? spp->pslc_pgs_per_oneshotpg
								: spp->pgs_per_oneshotpg;

	return (ppa->g.pg % pgs_per_oneshotpg) == (pgs_per_oneshotpg - 1);
}

static inline uint64_t get_rmap_ent(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	uint64_t pgidx = ppa_2_pgidx(zms_ftl, ppa);
	if (pgidx >= zms_ftl->zp.tt_ppns) {
		NVMEV_ERROR("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__, ppa->zms.ch,
					ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);
		NVMEV_ERROR("ERROR wpidx %lld\n", pgidx);
		return INVALID_LPN;
	}
	return zms_ftl->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct zms_ftl *zms_ftl, uint64_t lpn, struct ppa *ppa)
{
	uint64_t pgidx = ppa_2_pgidx(zms_ftl, ppa);
	if (pgidx >= zms_ftl->zp.tt_ppns) {
		NVMEV_ERROR("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d --> lpn %lld\n", __func__, ppa->zms.ch,
					ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg, lpn);
		NVMEV_ERROR("ERROR wpidx %lld\n", pgidx);
		return;
	}
	zms_ftl->rmap[pgidx] = lpn;
}

static inline void set_maptbl_ent(struct zms_ftl *zms_ftl, uint64_t lpn, struct ppa *ppa)
{
	if (lpn >= zms_ftl->zp.tt_lpns) {
		NVMEV_ERROR("%s lpn too large %llu / %llu\n", __func__, lpn, zms_ftl->zp.tt_lpns);
		return;
	}
	zms_ftl->maptbl[lpn] = *ppa;
}

struct ppa get_maptbl_ent(struct zms_ftl *zms_ftl, uint64_t lpn)
{
	if (lpn >= zms_ftl->zp.tt_lpns) {
		NVMEV_ERROR("%s lpn too large %llu / %llu\n", __func__, lpn, zms_ftl->zp.tt_lpns);
		struct ppa ppa;
		ppa.ppa = UNMAPPED_PPA;
		return ppa;
	}
	return zms_ftl->maptbl[lpn];
}

static inline void set_l2pcacheidx(struct zms_ftl *zms_ftl, uint64_t lpn, int idx)
{
	if (lpn >= zms_ftl->zp.tt_lpns) {
		NVMEV_ERROR("%s lpn too large %llu / %llu\n", __func__, lpn, zms_ftl->zp.tt_lpns);
		return;
	}
	zms_ftl->l2pcache_idx[lpn] = idx;
}

static int get_l2pcacheidx(struct zms_ftl *zms_ftl, uint64_t lpn)
{
	if (lpn >= zms_ftl->zp.tt_lpns) {
		NVMEV_ERROR("%s lpn too large %llu / %llu\n", __func__, lpn, zms_ftl->zp.tt_lpns);
		return -1;
	}
	return zms_ftl->l2pcache_idx[lpn];
}

static struct ppa get_prev_ppa(struct zms_ftl *zms_ftl, uint64_t lpn, int granularity)
{
	uint64_t mapping_slpn = get_granularity_start_lpn(zms_ftl, lpn, granularity);
	uint64_t last_lpn = lpn == mapping_slpn ? lpn : lpn - 1;
	struct ppa ppa = get_maptbl_ent(zms_ftl, last_lpn);
	return ppa;
}

static bool check_reserved(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct nand_page *pg = get_pg(zms_ftl->ssd, ppa);
	uint64_t lpn = get_rmap_ent(zms_ftl, ppa);
	if (pg->status == PG_VALID && !valid_lpn(zms_ftl, lpn))
		return true;
	return false;
}

static bool check_migrating(struct zms_ftl *zms_ftl)
{
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
				return 1;
		}
	}
	return 0;
}

static uint64_t check_maxfreetime(struct zms_ftl *zms_ftl)
{
	struct ssd *ssd = zms_ftl->ssd;
	struct ssdparams *spp = &ssd->sp;
	struct ppa ppa;
	ppa.ppa = 0;

	uint64_t max_freetime = 0;
	int lun, ch;
	for (lun = 0; lun < spp->luns_per_ch; lun++) {
		for (ch = 0; ch < spp->nchs; ch++) {
			ppa.g.ch = ch;
			ppa.g.lun = lun;
			ppa.g.pl = 0;

			struct nand_lun *lunp = get_lun(ssd, &ppa);
			struct nand_cmd gcw = {
				.type = GC_IO,
				.cmd = NAND_NOP,
				.stime = 0,
				.interleave_pci_dma = false,
				.ppa = ppa,
			};
			uint64_t end_time = submit_nand_cmd(zms_ftl->ssd, &gcw);
			max_freetime = max(max_freetime, (end_time - zms_ftl->current_time));
		}
	}
	return max_freetime;
}

static int get_aggidx(struct zms_ftl *zms_ftl, uint64_t lpn)
{
	return zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED
			   ? lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), lpn)
			   : 0;
}

struct ppa get_current_page(struct zms_ftl *zms_ftl, struct zms_write_pointer *wp)
{
	struct ppa ppa;
	ppa.ppa = 0;
	ppa.zms.map = PAGE_MAP; // default
	ppa.zms.ch = wp->ch;
	ppa.zms.lun = wp->lun;
	ppa.zms.pl = wp->pl;
	ppa.zms.blk = wp->blk;
	ppa.zms.pg = wp->pg;

	int loc = check_location_type(zms_ftl, &ppa);
	if (loc != wp->loc) {
		NVMEV_ERROR("bad get current page!\n");
		NVMEV_ASSERT(0);
	}

	if (ppa_2_pgidx(zms_ftl, &ppa) >= zms_ftl->zp.tt_ppns) {
		NVMEV_ERROR("%s ppa_2_pgidx(zms_ftl,&ppa) >= zms_ftl->zp.tt_ppns\n", __func__);
		if (loc == LOC_PSLC)
			zms_ftl->pslc_full = 1;
		else
			zms_ftl->device_full = 1;
	}
	// NVMEV_ASSERT(ppa.g.pl == 0);
	return ppa;
}

void update_write_pointer(struct zms_write_pointer *wp, struct ppa ppa)
{
	wp->ch = ppa.zms.ch;
	wp->lun = ppa.zms.lun;
	wp->pl = ppa.zms.pl;
	wp->blk = ppa.zms.blk;
	wp->pg = ppa.zms.pg;
}

static void get_new_wp(struct zms_ftl *zms_ftl, struct zms_write_pointer *wpp)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	/* move current line to {victim,full} line list */
	if (wpp->curline->vpc == wpp->curline->pgs_per_line) {
		/* all pgs are still valid, move to full line list */
		// NVMEV_ASSERT(wpp->curline->ipc == 0);
		if (wpp->curline->ipc) {
			NVMEV_ERROR("%s full line ipc should be 0\n", __func__);
		}
		struct list_head *full_line_list = zms_get_full_list(zms_ftl, wpp->loc);
		list_add_tail(&wpp->curline->entry, full_line_list);
		inc_full_cnt(zms_ftl, wpp->loc);
		// NVMEV_CONZONE_GC_DEBUG_VERBOSE("wpp: move line to full_line_list\n");
	} else {
		// NVMEV_ASSERT(wpp->curline->vpc >= 0 && wpp->curline->vpc < lm->pgs_per_line);
		if (!(wpp->curline->vpc >= 0 && wpp->curline->vpc < wpp->curline->pgs_per_line)) {
			NVMEV_ERROR("%s line vpc %d pgs_per_line %lu\n", __func__, wpp->curline->vpc,
						wpp->curline->pgs_per_line);
		}
		/* there must be some invalid pages in this line */
		// NVMEV_ASSERT(wpp->curline->ipc > 0);
		if (wpp->curline->ipc == 0) {
			NVMEV_ERROR("[%d] %s wpp ipc should > 0\n", zms_ftl->zp.ns->id,
						wpp == &zms_ftl->pslc_wp ? "user" : "gc");
			NVMEV_ERROR("vpc %d ipc %d rpc %d\n", wpp->curline->vpc, wpp->curline->ipc,
						wpp->curline->rpc);
		}

		pqueue_t *victim_pq = zms_get_victim_pq(zms_ftl, wpp->loc);
		pqueue_insert(victim_pq, wpp->curline);
		inc_victim_cnt(zms_ftl, wpp->loc);
	}

	/* current line is used up, pick another empty line */
	wpp->curline = get_next_free_line(zms_ftl, wpp->loc);

	if (!wpp->curline) {
		NVMEV_ERROR("stack info: %s\n", __func__);
		return;
	}

	struct ppa first_pg = get_first_page(zms_ftl, wpp->curline);
	update_write_pointer(wpp, first_pg);

	check_addr(wpp->blk, spp->blks_per_pl);
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && wpp == &zms_ftl->wp) {
		zms_ftl->zone_write_cnt++;
	}
}

static void nextpage_interleave(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	if (!mapped_ppa(ppa))
		return;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	bool location = check_location_type(zms_ftl, ppa);
	struct nand_block *blk = get_blk(zms_ftl->ssd, ppa);
	int pgs_per_oneshotpg =
		location == LOC_PSLC ? spp->pslc_pgs_per_oneshotpg : spp->pgs_per_oneshotpg;

	check_addr(ppa->zms.pg, blk->used_pgs);
	ppa->zms.pg++;
	if ((ppa->zms.pg % pgs_per_oneshotpg) != 0)
		goto out;

	ppa->zms.pg -= pgs_per_oneshotpg;
	check_addr(ppa->zms.ch, spp->nchs);
	ppa->zms.ch++;
	if (ppa->zms.ch != spp->nchs)
		goto out;

	ppa->zms.ch = 0;
	check_addr(ppa->zms.lun, spp->luns_per_ch);
	ppa->zms.lun++;
	/* in this case, we should go to next lun */
	if (ppa->zms.lun != spp->luns_per_ch)
		goto out;

	ppa->zms.lun = 0;
	/* go to next wordline in the block */

	ppa->zms.pg += pgs_per_oneshotpg;
	if (ppa->zms.pg != blk->used_pgs)
		goto out;

	ppa->ppa = UNMAPPED_PPA;
out:
	return;
}

static void nextpage_normal(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct nand_block *blk = get_blk(zms_ftl->ssd, ppa);

	check_addr(ppa->zms.pg, blk->used_pgs);
	ppa->zms.pg++;
	if (ppa->zms.pg != blk->used_pgs)
		goto out;
	ppa->ppa = UNMAPPED_PPA;
out:
	return;
}

static void advance_write_pointer(struct zms_ftl *zms_ftl, uint32_t io_type, bool loc)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct zms_write_pointer *wpp = zms_get_wp(zms_ftl, io_type, loc);
	if (!wpp) {
		NVMEV_ERROR("wpp is NULL!");
		return;
	}
	if (!wpp->curline)
		return;

	struct ppa ppa = get_current_page(zms_ftl, wpp);
	if (wpp->curline->parent_id == -1) {
		nextpage_interleave(zms_ftl, &ppa);
	} else {
		nextpage_normal(zms_ftl, &ppa);
	}

	if (mapped_ppa(&ppa)) {
		update_write_pointer(wpp, ppa);
	} else {
		get_new_wp(zms_ftl, wpp);
	}

	if (!wpp->curline) {
		NVMEV_ERROR("stack info: %s io type %d loc %d \n", __func__, io_type, loc);
	}

	// NVMEV_CONZONE_GC_DEBUG_VERBOSE("advanced %s wpp(parent %d): ch:%d, lun:%d, pl:%d, blk:%d, "
	// 							   "pg:%d (curline %d ipc %d vpc %d)\n",
	// 							   wpp->loc ? "pslc" : "normal", wpp->curline->parent_id, wpp->ch,
	// 							   wpp->lun, wpp->pl, wpp->blk, wpp->pg, wpp->curline->id,
	// 							   wpp->curline->ipc, wpp->curline->vpc);
}

static struct ppa get_new_page(struct zms_ftl *zms_ftl, uint32_t io_type, int location)
{
	struct ppa ppa;
	struct zms_write_pointer *wp = zms_get_wp(zms_ftl, io_type, location);
	struct ssd *ssd = zms_ftl->ssd;

	if (!wp->curline) {
		ppa.ppa = UNMAPPED_PPA;
		return ppa;
	}
	return get_current_page(zms_ftl, wp);
}

static void mark_page_valid(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct zms_line *line = NULL;

	NVMEV_DEBUG_VERBOSE("mark ppa ch %d lun %d pl %d blk %d pg %d valid\n", ppa->zms.ch,
						ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);

	line = get_line(zms_ftl, ppa);
	blk = get_blk(zms_ftl->ssd, ppa);
	if (ppa->zms.pg >= blk->used_pgs) {
		NVMEV_ERROR(" ppa->zms.pg %d blk->used_pgs %d\n", ppa->zms.pg, blk->used_pgs);
		return;
	}

	/* update page status */
	pg = get_pg(zms_ftl->ssd, ppa);
	if (pg->status != PG_FREE) {
		NVMEV_ERROR("nsid %d page %lld is valid!\n", zms_ftl->zp.ns->id, ppa_2_pgidx(zms_ftl, ppa));
		NVMEV_ERROR("valid page: ppa ch %d lun %d pl %d blk %d pg %d\n", ppa->zms.ch, ppa->zms.lun,
					ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);
		return;
	}
	// NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;

	/* update corresponding block status */
	if (!(blk->vpc >= 0 && blk->vpc < blk->used_pgs)) {
		NVMEV_ERROR(" blk vpc %d blk->used_pgs %d\n", blk->vpc, blk->used_pgs);
		return;
	}
	blk->vpc++;

	/* update corresponding line status */
	if (!(line->vpc >= 0 && line->vpc < line->pgs_per_line)) {
		NVMEV_ERROR(" line id %d vpc %d pgs_per_line %lu\n", line->id, line->vpc,
					line->pgs_per_line);
		return;
	}
	line->vpc++;
}

// RESERVED: VALID but ppa -> invalid lpn
static void zms_mark_page_reserved(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct zms_line *line;
	mark_page_valid(zms_ftl, ppa);
	set_rmap_ent(zms_ftl, INVALID_LPN, ppa);
	line = get_line(zms_ftl, ppa);
	line->rpc++;
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	if (!mapped_ppa(ppa))
		return;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	int location = check_location_type(zms_ftl, ppa);
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct zms_line *line = NULL;

	NVMEV_CONZONE_DEBUG("mark ppa ch %d lun %d pl %d blk %d pg %d invalid\n", ppa->zms.ch,
						ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);
	// NVMEV_INFO("mark page %lld invalid!\n",ppa_2_pgidx(zms_ftl,ppa));

	line = get_line(zms_ftl, ppa);

	/* update corresponding page status */
	pg = get_pg(zms_ftl->ssd, ppa);
	if (pg->status != PG_VALID) {
		NVMEV_ERROR(" ns %d %s but pg status (%d)\n", zms_ftl->zp.ns->id, __func__, pg->status);
		NVMEV_ERROR("ppa ch %d lun %d pl %d blk %d pg %d\n", ppa->zms.ch, ppa->zms.lun, ppa->zms.pl,
					ppa->zms.blk, ppa->zms.pg);
		NVMEV_ASSERT(0);
	}

	// NVMEV_CONZONE_DEBUG("mark ppa ch %d lun %d pl %d blk %d pg %d
	// invalid\n",ppa->zms.ch,ppa->zms.lun,ppa->zms.pl,ppa->zms.blk,ppa->zms.pg);

	pg->status = PG_INVALID;

	/* update corresponding block status */
	blk = get_blk(zms_ftl->ssd, ppa);
	if (!(blk->ipc >= 0 && blk->ipc < blk->used_pgs)) {
		NVMEV_ERROR(" blk ipc %d blk->used_pgs %d)\n", blk->ipc, blk->used_pgs);
		NVMEV_ASSERT(0);
	}
	blk->ipc++;
	if (!(blk->vpc > 0 && blk->vpc <= blk->used_pgs)) {
		NVMEV_ERROR(" blk vpc %d blk->used_pgs %d\n", blk->vpc, blk->used_pgs);
		NVMEV_ASSERT(0);
	}
	blk->vpc--;

	/* update corresponding line status */
	if (!(line->ipc >= 0 && line->ipc < line->pgs_per_line)) {
		NVMEV_ERROR(" line id %d ipc %d pgs_per_line %lu\n", line->id, line->ipc,
					line->pgs_per_line);
		NVMEV_ASSERT(0);
	}
	if (line->vpc == line->pgs_per_line) {
		if (line->ipc) {
			NVMEV_ERROR(" full line ipc should be 0!\n");
			NVMEV_ASSERT(0);
		}
		was_full_line = true;
	}
	line->ipc++;
	if (!(line->vpc > 0 && line->vpc <= line->pgs_per_line)) {
		NVMEV_ERROR("line id %d vpc %d pgs_per_line %lu\n", line->id, line->vpc,
					line->pgs_per_line);
		NVMEV_ASSERT(0);
	}

	pqueue_t *victim_pq = zms_get_victim_pq(zms_ftl, location);

	/* Adjust the position of the victime line in the pq under over-writes */
	if (line->pos) {
		/* Note that line->vpc will be updated by this call */
		pqueue_change_priority(victim_pq, line->vpc - 1, line);
	} else {
		line->vpc--;
	}

	if (was_full_line) {
		/* move line: "full" -> "victim" */
		list_del_init(&line->entry);
		dec_full_cnt(zms_ftl, location);
		pqueue_insert(victim_pq, line);
		inc_victim_cnt(zms_ftl, location);
	}
}

static void mark_block_free(struct zms_ftl *zms_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct nand_block *blk = get_blk(zms_ftl->ssd, ppa);
	struct nand_page *pg = NULL;
	int i;

	for (i = 0; i < blk->npgs; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		if (!(pg->nsecs == spp->secs_per_pg)) {
			NVMEV_ERROR("%s ERROR pg nsecs (%d / %d)\n", __func__, pg->nsecs, spp->secs_per_pg);
			return;
		}
		// NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
		pg->status = PG_FREE;
		// NVMEV_CONZONE_GC_DEBUG_VERBOSE("mark ppa(ch %d lun %d pl %d blk %d pg %d)
		// free\n",ppa->zms.ch,ppa->zms.lun,ppa->zms.pl,ppa->zms.blk,i);
	}

	/* reset block status */
	blk->ipc = 0;
	blk->vpc = 0;
	blk->erase_cnt++;
}

static void nextpage(struct zms_ftl *zms_ftl, struct ppa *ppa, int rsv)
{
	struct zms_line *line = get_line(zms_ftl, ppa);
	if (line->parent_id == -1)
		nextpage_interleave(zms_ftl, ppa);
	else
		nextpage_normal(zms_ftl, ppa);
	if (rsv && !mapped_ppa(ppa) && line->rsv_nextline != NULL) {
		*ppa = get_first_page(zms_ftl, line->rsv_nextline);
	}
}

static void set_map_gran(struct zms_ftl *zms_ftl, int gran, uint64_t map_slpn)
{
	int is_resident = check_resident(zms_ftl, gran);
	struct ppa ppa = get_maptbl_ent(zms_ftl, map_slpn);
	int cache_idx = get_l2pcacheidx(zms_ftl, map_slpn);

	if (cache_idx == -1) {
		cache_idx = l2p_insert(zms_ftl, map_slpn, gran, is_resident);
		set_l2pcacheidx(zms_ftl, map_slpn, cache_idx);
	}

	ppa.zms.map = gran;
	set_maptbl_ent(zms_ftl, map_slpn, &ppa);
	NVMEV_CONZONE_L2P_DEBUG("set map [slpn %lld, map gran %d] \n", map_slpn, gran);
}

static int get_mapping_granularity(struct zms_ftl *zms_ftl, int loc)
{
	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_ZONED)
		return PAGE_MAP;
	if (loc == LOC_NORMAL)
		return ZONE_MAP;
	if (ZONED_SLC && !SLC_BYPASS)
		return SUB_ZONE_MAP;
	return PAGE_MAP;
}

static inline bool should_gc_high(struct zms_ftl *zms_ftl, int location)
{
	int free_line_cnt =
		location == LOC_PSLC ? zms_ftl->lm.pslc_free_line_cnt : zms_ftl->lm.free_line_cnt;
	return free_line_cnt <= zms_ftl->zp.gc_thres_lines_high;
}

static inline bool should_migrate_low(struct zms_ftl *zms_ftl)
{
	return zms_ftl->lm.pslc_free_line_cnt <= zms_ftl->zp.migrate_thres_lines_low;
}

static struct zms_line *select_victim_line(struct zms_ftl *zms_ftl, bool force, int location)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct znsparams *zpp = &zms_ftl->zp;
	struct zms_line *victim_line = NULL;
	pqueue_t *victim_pq = zms_get_victim_pq(zms_ftl, location);
	victim_line = pqueue_peek(victim_pq);
	if (!victim_line) {
		return NULL;
	}

	if (!force && (victim_line->vpc > (spp->pgs_per_line / 8))) {
		return NULL;
	}

	pqueue_pop(victim_pq);
	victim_line->pos = 0;
	dec_victim_cnt(zms_ftl, location);
	/* victim_line is a danggling node now */
	return victim_line;
}

static bool is_active_line(struct zms_ftl *zms_ftl, struct zms_line *line)
{
	struct zms_write_pointer *pslc_gc_wp = zms_get_wp(zms_ftl, GC_IO, LOC_PSLC);
	struct zms_write_pointer *pslc_user_wp = zms_get_wp(zms_ftl, USER_IO, LOC_PSLC);
	struct zms_write_pointer *gc_wp = zms_get_wp(zms_ftl, GC_IO, LOC_NORMAL);
	struct zms_write_pointer *user_wp = zms_get_wp(zms_ftl, USER_IO, LOC_NORMAL);
	if (line != gc_wp->curline && line != user_wp->curline && line != pslc_gc_wp->curline &&
		line != pslc_user_wp->curline) {
		return false;
	} else {
		return true;
	}
}

static void mark_line_free(struct zms_ftl *zms_ftl, struct zms_line *line, int io_type)
{
	int location = get_line_location(zms_ftl, line);
	bool is_active = is_active_line(zms_ftl, line);

	if (io_type != GC_IO && !is_active) {
		if (line->vpc != line->pgs_per_line) {
			pqueue_t *victim_pq = zms_get_victim_pq(zms_ftl, location);
			pqueue_remove(victim_pq, line);
			line->pos = 0;
			dec_victim_cnt(zms_ftl, location);
		} else {
			list_del_init(&line->entry);
			dec_full_cnt(zms_ftl, location);
		}
	}

	line->ipc = 0;
	line->vpc = 0;
	line->rpc = 0;
	line->rsv_nextline = NULL;

	/* move this line to free line list */
	if (!is_active) {
		struct list_head *free_line_list = zms_get_free_list(zms_ftl, location);
		list_add_tail(&line->entry, free_line_list);
		inc_free_cnt(zms_ftl, location);
	} else {
		struct zms_write_pointer *pslc_gc_wp = zms_get_wp(zms_ftl, GC_IO, LOC_PSLC);
		struct zms_write_pointer *pslc_user_wp = zms_get_wp(zms_ftl, USER_IO, LOC_PSLC);
		struct zms_write_pointer *gc_wp = zms_get_wp(zms_ftl, GC_IO, LOC_NORMAL);
		struct zms_write_pointer *user_wp = zms_get_wp(zms_ftl, USER_IO, LOC_NORMAL);
		struct zms_write_pointer *wp;
		// reset write pointer
		if (line == gc_wp->curline) {
			wp = gc_wp;
			NVMEV_INFO("Reset curline of gc wp. PPA loc %d IO TYPE %d\n", location, io_type);
		} else if (line == user_wp->curline) {
			wp = user_wp;
			NVMEV_INFO("Reset curline of user wp. PPA loc %d IO TYPE %d\n", location, io_type);
		} else if (line == pslc_gc_wp->curline) {
			wp = pslc_gc_wp;
			NVMEV_INFO("Reset curline of pslc gc wp. PPA loc %d IO TYPE %d\n", location, io_type);
		} else {
			wp = pslc_user_wp;
			NVMEV_INFO("Reset curline of pslc user wp. PPA loc %d IO TYPE %d\n", location, io_type);
		}

		wp->ch = 0;
		wp->lun = 0;
		wp->pl = 0;
		wp->pg = 0;
	}

	if (location == LOC_PSLC) {
		zms_ftl->slc_erase_cnt++;

		if (io_type != MIGRATE_IO && !is_active) {
			if (line->mid.pos == 0) {
				NVMEV_ERROR("try to remove migrating line but line->mid.pos == 0\n");
			} else {
				pqueue_remove(zms_ftl->migrating_line_pq, &line->mid);
				line->mid.pos = 0;
			}
		}
	} else {
		zms_ftl->normal_erase_cnt++;
	}
}

static void foreground_gc(struct zms_ftl *zms_ftl, int location);
static void try_migrate(struct zms_ftl *zms_ftl);

static inline void check_and_refill_write_credit(struct zms_ftl *zms_ftl, int location)
{
	struct zms_write_flow_control *wfc = location ? &(zms_ftl->pslc_wfc) : &(zms_ftl->wfc);

	if (wfc->write_credits <= 0) {
		// NVMEV_INFO("should refill write credit %ld\n", wfc->write_credits);
		foreground_gc(zms_ftl, location);

		wfc->write_credits += wfc->credits_to_refill;
		// NVMEV_INFO("new write credits %ld\n", wfc->write_credits);
	}
}

static inline void consume_write_credit(struct zms_ftl *zms_ftl, bool pSLC)
{
	if (pSLC == LOC_PSLC) {
		zms_ftl->pslc_wfc.write_credits--;
	} else {
		zms_ftl->wfc.write_credits--;
	}
}

static int update_mapping_if_reserved(struct zms_ftl *zms_ftl, uint64_t lpn, int loc, int io_type)
{
	int granularity = get_mapping_granularity(zms_ftl, loc);
	struct ppa ppa = get_prev_ppa(zms_ftl, lpn, granularity);
	if (!mapped_ppa(&ppa)) {
		return FAILURE;
	}

	if (check_location_type(zms_ftl, &ppa) != loc) {
		// maybe last page is in qlc and the new page should be written to slc
		NVMEV_INFO("check location type should be %d\n", loc);
		print_ppa(ppa);
		uint64_t badlpn = get_rmap_ent(zms_ftl, &ppa);
		NVMEV_INFO("rmap: badlpn %lld current lpn %lld\n", badlpn, lpn);
		int agg_idx = get_aggidx(zms_ftl, lpn);
		print_agg(zms_ftl, zms_ftl->zone_agg_pgs[agg_idx], zms_ftl->zone_agg_lpns[agg_idx]);
		return FAILURE;
	}

	nextpage(zms_ftl, &ppa, 1);

	if (!mapped_ppa(&ppa)) {
		// no free page in this line
		NVMEV_ERROR("[ERROR] lpn %lld exceed the boundary of the granularity(%d) "
					"unit (loc %d)!\n",
					lpn, granularity, loc);
		return FAILURE;
	}

	if (!(check_reserved(zms_ftl, &ppa))) {
		// may be this page is in GC
		NVMEV_ERROR("[WARN] page (ch %d lun %d pl %d blk %d page %d) is not reserved!\n",
					ppa.zms.ch, ppa.zms.lun, ppa.zms.pl, ppa.zms.blk, ppa.zms.pg);
		return FAILURE;
	}

	set_maptbl_ent(zms_ftl, lpn, &ppa);
	set_rmap_ent(zms_ftl, lpn, &ppa);
	NVMEV_CONZONE_MAPPING_DEBUG("[r] map zid %u lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n",
								lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), lpn), lpn, ppa.zms.ch,
								ppa.zms.lun, ppa.zms.pl, ppa.zms.blk, ppa.zms.pg);
	dec_line_rpc(zms_ftl, &ppa);

	struct zms_line *current_line = get_line(zms_ftl, &ppa);
	if (current_line->vpc + current_line->ipc == current_line->pgs_per_line &&
		current_line->rpc == 0 && loc == LOC_PSLC && io_type == USER_IO) {
		zms_ftl->line_write_cnt++;
		current_line->mid.write_order = zms_ftl->line_write_cnt;
		pqueue_insert(zms_ftl->migrating_line_pq, &current_line->mid);
	}

	int sidx = 0;
	if (!(zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED)) {
		sidx = NUM_MAP - 1;
	}

	for (int i = sidx; i < NUM_MAP; i++) {
		uint64_t map_slpn = get_granularity_start_lpn(zms_ftl, lpn, MAP_GRAN(i));
		struct ppa sppa = get_maptbl_ent(zms_ftl, map_slpn);
		int pgs = get_pages_per_granularity(zms_ftl, MAP_GRAN(i));

		if (mapped_ppa(&sppa) && check_location_type(zms_ftl, &sppa) == loc &&
			(lpn - map_slpn + 1) % pgs == 0) {
			set_map_gran(zms_ftl, MAP_GRAN(i), map_slpn);
		}
	}
	return SUCCESS;
}

static void update_or_reserve_mapping(struct zms_ftl *zms_ftl, uint64_t lpn, int loc, int io_type)
{
	if (update_mapping_if_reserved(zms_ftl, lpn, loc, io_type) != SUCCESS) {
		int granularity = get_mapping_granularity(zms_ftl, loc);
		int pgs = get_pages_per_granularity(zms_ftl, granularity);
		uint64_t slpn = get_granularity_start_lpn(zms_ftl, lpn, granularity);
		struct ppa ppa;
		if (lpn != slpn) {
			NVMEV_ERROR("BAD RESERVED!! lpn %lld slpn %lld granularity %d\n", lpn, slpn,
						granularity);
			// NVMEV_ASSERT(0);
		}
		struct zms_line *last_line = NULL;
		for (int i = 0; i < pgs; i++) {
			if ((loc == LOC_NORMAL && zms_ftl->device_full) ||
				(loc == LOC_PSLC && zms_ftl->pslc_full)) {
				// NVMEV_ERROR("%s no free page! io_type %d dest location %d lpn %lld\n", __func__,
				// 			io_type, loc, lpn);
				break;
			}

			ppa = get_new_page(zms_ftl, io_type, loc);
			struct zms_line *current_line = get_line(zms_ftl, &ppa);

			if (last_line && last_line != current_line && last_line->rsv_nextline == NULL) {
				last_line->rsv_nextline = current_line;
				// if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
				// 	NVMEV_INFO("line [%d,%d] -next line-> [%d,%d]\n", last_line->parent_id,
				// 			   last_line->id, current_line->parent_id, current_line->id);
				// }
			}

			if (last_line == NULL || last_line != current_line) {
				last_line = current_line;
				// if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
				// 	NVMEV_INFO("new last line [%d,%d] \n", last_line->parent_id, last_line->id);
				// 	if (last_line->rsv_nextline) {
				// 		NVMEV_ERROR("last line next ->[%d,%d]\n",
				// 					last_line->rsv_nextline->parent_id,
				// 					last_line->rsv_nextline->id);
				// 	}
				// }
			}

			if (!mapped_ppa(&ppa)) {
				NVMEV_ERROR("Can not get new page. I/O type: %d loc %d lpn %lld pgs %d\n", io_type,
							loc, lpn, pgs);
				break;
			}

			if (slpn + i <= lpn) {
				set_maptbl_ent(zms_ftl, slpn + i, &ppa);
				mark_page_valid(zms_ftl, &ppa);
				set_rmap_ent(zms_ftl, slpn + i, &ppa);
				NVMEV_CONZONE_MAPPING_DEBUG("[n] map lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n",
											slpn + i, ppa.zms.ch, ppa.zms.lun, ppa.zms.pl,
											ppa.zms.blk, ppa.zms.pg);
				set_map_gran(zms_ftl, PAGE_MAP, slpn + i);

				if (current_line->vpc + current_line->ipc == current_line->pgs_per_line &&
					current_line->rpc == 0 && loc == LOC_PSLC && io_type == USER_IO) {
					zms_ftl->line_write_cnt++;
					current_line->mid.write_order = zms_ftl->line_write_cnt;
					pqueue_insert(zms_ftl->migrating_line_pq, &current_line->mid);
				}
			} else {
				zms_mark_page_reserved(zms_ftl, &ppa);
			}

			advance_write_pointer(zms_ftl, io_type, loc);
		}

		// if (pgs > 1) {
		// 	NVMEV_INFO("Reserved pgs [%lld - %lld)\n", lpn, lpn + pgs);
		// 	NVMEV_INFO("Start ppa\n");
		// 	print_ppa(get_maptbl_ent(zms_ftl, lpn));
		// 	NVMEV_INFO("End ppa\n");
		// 	print_ppa(ppa);
		// }
	}
}

static uint64_t nand_write(struct zms_ftl *zms_ftl, uint64_t nsecs_start, uint64_t lpn,
						   uint64_t location, int io_type, int to_write_pgs, uint64_t elpn)
{
	struct ppa ppa;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	uint64_t nsecs_latest = nsecs_start;
	update_or_reserve_mapping(zms_ftl, lpn, location, io_type);
	ppa = get_maptbl_ent(zms_ftl, lpn);

	if (!mapped_ppa(&ppa)) {
		return nsecs_start;
	}

	if ((location == LOC_PSLC && zms_ftl->pslc_full) ||
		(location == LOC_NORMAL && zms_ftl->device_full)) {
		// NVMEV_ERROR("%s: no free pages! io_type %d dest %s lpn %lld\n", __func__, io_type,
		// 			location == LOC_NORMAL ? "normal" : "slc", lpn);
		// NVMEV_ERROR("%s: device write pgs %lld host write pgs %lld\n", __func__,
		// 			zms_ftl->device_w_pgs, zms_ftl->host_w_pgs);
		return nsecs_start;
	}

	if (is_last_pg_in_wordline(zms_ftl, &ppa) || lpn == elpn) {

		int pgs = location == LOC_PSLC ? spp->pslc_pgs_per_oneshotpg : spp->pgs_per_oneshotpg;
		if (to_write_pgs < pgs) {
			pgs = to_write_pgs;
		}

		uint64_t flush_nsecs_completed = nsecs_start;

		if (io_type != GC_IO || zms_ftl->zp.enable_gc_delay) {
			struct nand_cmd swr = {
				.type = io_type,
				.cmd = NAND_WRITE,
				.stime = nsecs_start,
				.xfer_size = pgs * spp->pgsz,
				.interleave_pci_dma = false,
				.ppa = ppa,
			};
			flush_nsecs_completed = submit_nand_cmd(zms_ftl->ssd, &swr);
			nsecs_latest = max(flush_nsecs_completed, nsecs_latest);
		}

		zms_ftl->device_w_pgs += pgs;
		if (io_type == MIGRATE_IO)
			zms_ftl->migration_pgs += pgs;
		if (io_type == GC_IO)
			zms_ftl->gc_pgs += pgs;
	}

	// NVMEV_INFO("%s flushed %d pgs( idx %d loc %d) ppa ch %d lun %d pl %d blk %d pg %d
	// \n",__func__,pgs,idx,loc,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);
	if (io_type == USER_IO) {
		if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META && location == LOC_PSLC) {
			if (SLC_BYPASS) {
				consume_write_credit(zms_ftl, location);
				check_and_refill_write_credit(zms_ftl, location);
			} else {
				try_migrate(zms_ftl);
			}
		}
	}

	if (io_type != GC_IO) {
		if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META ||
			(zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_BLOCK && location == LOC_NORMAL)) {
			consume_write_credit(zms_ftl, location);
			check_and_refill_write_credit(zms_ftl, location);
		}
	}
	return nsecs_latest;
}

static struct zms_line *get_migrate_line(struct zms_ftl *zms_ftl)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct migrating_lineid *m_id = NULL;
	struct zms_line *sblk_line = NULL;
	int i;

	// choose a line based on FIFO
	m_id = pqueue_peek(zms_ftl->migrating_line_pq);

	if (m_id) {
		if (m_id->parent_id != -1) {
			sblk_line = &lm->lines[m_id->parent_id].sub_lines[m_id->id];
		} else {
			sblk_line = &lm->lines[m_id->id];
		}
		pqueue_pop(zms_ftl->migrating_line_pq);
		m_id->pos = 0;
	}
	return sblk_line;
}

static uint64_t nand_read(struct zms_ftl *zms_ftl, uint64_t *read_lpns, int sidx, int eidx,
						  int io_type, uint64_t nsecs_start)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	int read_agg_len = spp->nchs * spp->luns_per_ch * spp->pls_per_lun;
	uint64_t latest_time = nsecs_start;
	struct ppa *prev_ppas = kmalloc(sizeof(struct ppa) * read_agg_len, GFP_KERNEL);
	uint64_t *read_agg_size = kmalloc(sizeof(uint64_t) * read_agg_len, GFP_KERNEL);
	if (!prev_ppas || !read_agg_size) {
		NVMEV_ERROR("nand read failed because of kmalloc failure!\n");
		return nsecs_start;
	}

	for (int i = 0; i < read_agg_len; i++) {
		prev_ppas[i].ppa = UNMAPPED_PPA;
		read_agg_size[i] = 0;
	}

	for (int i = sidx; i < eidx; i++) {
		uint64_t lpn = read_lpns[i];
		struct ppa ppa = get_maptbl_ent(zms_ftl, lpn);
		if (mapped_ppa(&ppa)) {
			int loc_idx = (ppa.zms.ch * (spp->luns_per_ch * spp->pls_per_lun) +
						   ppa.zms.lun * spp->pls_per_lun + ppa.zms.pl) %
						  read_agg_len;
			// aggregate read io in same flash page
			if (mapped_ppa(&prev_ppas[loc_idx]) &&
				flashpage_same(zms_ftl, ppa, prev_ppas[loc_idx])) {
				read_agg_size[loc_idx] += spp->pgsz;
			} else {
				if (read_agg_size[loc_idx] > 0) {
					if (io_type != GC_IO || zms_ftl->zp.enable_gc_delay) {
						struct nand_cmd rcmd = {
							.type = io_type,
							.cmd = NAND_READ,
							.stime = nsecs_start,
							.xfer_size = read_agg_size[loc_idx],
							.interleave_pci_dma = false,
							.ppa = prev_ppas[loc_idx],
						};

						uint64_t complete_time = submit_nand_cmd(zms_ftl->ssd, &rcmd);
						latest_time = max(latest_time, complete_time);
					}
					read_agg_size[loc_idx] = 0;
					prev_ppas[loc_idx].ppa = UNMAPPED_PPA;
				}
				read_agg_size[loc_idx] = spp->pgsz;
				prev_ppas[loc_idx] = ppa;
			}
		}
	}

	for (int i = 0; i < read_agg_len; i++) {
		if (read_agg_size[i] > 0 && mapped_ppa(&prev_ppas[i])) {
			if (io_type != GC_IO || zms_ftl->zp.enable_gc_delay) {
				struct nand_cmd rcmd = {
					.type = io_type,
					.cmd = NAND_READ,
					.stime = nsecs_start,
					.xfer_size = read_agg_size[i],
					.interleave_pci_dma = false,
					.ppa = prev_ppas[i],
				};
				uint64_t complete_time = submit_nand_cmd(zms_ftl->ssd, &rcmd);
				latest_time = max(latest_time, complete_time);
			}
		}
	}
	kfree(prev_ppas);
	kfree(read_agg_size);
	return latest_time;
}

static uint64_t internal_write(struct zms_ftl *zms_ftl, uint64_t *write_lpns, int sidx, int eidx,
							   int io_type, int dest_loc, uint64_t nsecs_start)
{
	// read migrated pages
	uint64_t write_start_time = nand_read(zms_ftl, write_lpns, sidx, eidx, io_type, nsecs_start);
	uint64_t latest_time = write_start_time;
	struct ssdparams *spp = &zms_ftl->ssd->sp;

	// write those pages to normal area
	int to_write_pgs = 0;
	for (int i = sidx; i < eidx; i++) {
		uint64_t lpn;
		struct ppa ppa;
		lpn = write_lpns[i];
		ppa = get_maptbl_ent(zms_ftl, lpn);

		if (mapped_ppa(&ppa)) {
			/* update old page information first */
			if (io_type == MIGRATE_IO && check_location_type(zms_ftl, &ppa) != LOC_PSLC) {
				NVMEV_ERROR("Migrated data should be in pSLC!! lpn %lld  slpn %lld elpn %lld\n",
							lpn, write_lpns[sidx], write_lpns[eidx - 1]);
				print_agg(zms_ftl, eidx, write_lpns);
				NVMEV_ASSERT(0);
			}

			if (ZONED_SLC && io_type == MIGRATE_IO && dest_loc == LOC_PSLC) {
				NVMEV_ERROR("shoudl not go here...\n");
				NVMEV_ASSERT(0);
			}

			mark_page_invalid(zms_ftl, &ppa);
			set_rmap_ent(zms_ftl, INVALID_LPN, &ppa);
			zms_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;

		} else {
			if (io_type != USER_IO && !(io_type == GC_IO && dest_loc == LOC_NORMAL)) {
				NVMEV_ERROR("Migrating unmapped lpn %lld slpn %lld elpn %lld\n", lpn,
							write_lpns[sidx], write_lpns[eidx - 1]);
				print_agg(zms_ftl, eidx, write_lpns);
				print_lines(zms_ftl);
				NVMEV_ASSERT(0);
			}
		}

		to_write_pgs++;
		uint64_t complete_time = nand_write(zms_ftl, write_start_time, lpn, dest_loc, io_type,
											to_write_pgs, write_lpns[eidx - 1]);
		if (complete_time != write_start_time) {
			to_write_pgs = 0;
		}

		latest_time = max(latest_time, complete_time);
	}
	NVMEV_CONZONE_PRINT_TIME("%s: latest %llu start %llu lat %llu us\n", __func__, latest_time,
							 nsecs_start, (latest_time - nsecs_start) / 1000);
	// NVMEV_INFO("[internal write] io type %d dest loc %d pgs %d lat %lld us\n", io_type, dest_loc,
	// 		   eidx - sidx, (latest_time - write_start_time) / 1000);
	return latest_time;
}

// Handle writes caused by data migration or garbage collection
// For MIGRATE_IO: migrate (line->vpc + agg_pgs) pgs
static void submit_internal_write(struct zms_ftl *zms_ftl, struct zms_line *line, int io_type)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct ppa ppa;
	struct nand_block *blk;
	ppa = get_first_page(zms_ftl, line);
	blk = get_blk(zms_ftl->ssd, &ppa);
	uint64_t *agg_lpns = NULL;
	int agg_len = 0;
	int pgs_per_oneshotpg;
	int dest_loc;
	if (io_type == MIGRATE_IO) {
		pgs_per_oneshotpg = zms_ftl->zone_write_unit;
		dest_loc = LOC_NORMAL;
	} else {
		pgs_per_oneshotpg =
			blk->nand_type == CELL_MODE_SLC ? spp->pslc_pgs_per_oneshotpg : spp->pgs_per_oneshotpg;
		dest_loc = blk->nand_type == CELL_MODE_SLC ? LOC_PSLC : LOC_NORMAL;
		agg_lpns = vmalloc(sizeof(uint64_t) * pgs_per_oneshotpg);

		if (dest_loc == LOC_NORMAL) {
			for (int i = 0; i < zms_ftl->gc_agg_len; i++) {
				agg_lpns[i] = zms_ftl->gc_agg_lpns[i];
			}
			agg_len = zms_ftl->gc_agg_len;
			zms_ftl->gc_agg_len = 0;
		}
	}

	while (mapped_ppa(&ppa)) {
		struct nand_page *pg_iter = get_pg(zms_ftl->ssd, &ppa);
		// Due to zone reset, some data in pSLC block may be invalidated
		if (pg_iter->status == PG_VALID) {
			if (check_reserved(zms_ftl, &ppa)) {
				mark_page_invalid(zms_ftl, &ppa);
				dec_line_rpc(zms_ftl, &ppa);
			} else {
				uint64_t lpn = get_rmap_ent(zms_ftl, &ppa);
				if (lpn == INVALID_LPN) {
					NVMEV_ERROR("migrating invalid lpn!\n");
					NVMEV_ASSERT(0);
				}
				int agg_idx = get_aggidx(zms_ftl, lpn);

				if (io_type == MIGRATE_IO) {
					agg_len = zms_ftl->zone_agg_pgs[agg_idx];
					agg_lpns = zms_ftl->zone_agg_lpns[agg_idx];

					if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && agg_len > 0 &&
						lpn != agg_lpns[agg_len - 1] + 1) {
						NVMEV_ERROR(
							"Migrated LPNs should be continuous!! lpn %lld slpn %lld elpn %lld\n",
							lpn, agg_lpns[0], agg_lpns[agg_len - 1]);
						print_agg(zms_ftl, agg_len, agg_lpns);
						if (lpn > agg_lpns[agg_len - 1] + 1) {
							NVMEV_INFO("----gap----\n");
							for (uint64_t idx = agg_lpns[agg_len - 1] + 1; idx < lpn; idx++) {
								struct ppa ippa = get_maptbl_ent(zms_ftl, idx);
								if (mapped_ppa(&ippa)) {
									NVMEV_INFO("lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n", idx,
											   ippa.zms.ch, ippa.zms.lun, ippa.zms.pl, ippa.zms.blk,
											   ippa.zms.pg);
								} else {
									NVMEV_INFO("lpn %lld -> UNMAPPED PPA\n", idx);
								}
							}
						}
						NVMEV_INFO("lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n", lpn, ppa.zms.ch,
								   ppa.zms.lun, ppa.zms.pl, ppa.zms.blk, ppa.zms.pg);
						NVMEV_ASSERT(0);
					}

					agg_lpns[agg_len] = lpn;
					zms_ftl->zone_agg_pgs[agg_idx]++;
					agg_len++;

				} else {
					agg_lpns[agg_len] = lpn;
					agg_len++;
				}

				if (agg_len == pgs_per_oneshotpg) {
					internal_write(zms_ftl, agg_lpns, 0, agg_len, io_type, dest_loc, 0);
					if (io_type == MIGRATE_IO) {
						zms_ftl->zone_agg_pgs[agg_idx] = 0;
					}
					agg_len = 0;
				}
				zms_ftl->gc_copy_pgs++;
			}
		}
		nextpage(zms_ftl, &ppa, 0);
	}

	// Gather misaligned data in SLC that does not align with QLC programming units
	// During the upcoming SLC to QLC migration, this data will be written to the QLC along with the
	// rest.
	if (io_type == MIGRATE_IO) {
		agg_lpns = vmalloc(sizeof(uint64_t) * spp->pslc_pgs_per_oneshotpg);
		agg_len = 0;
		struct nand_block *blk = line_2_blk(zms_ftl, line);

		int agg_pgs = 0;
		for (int i = 0; i < zms_ftl->num_aggs; i++) {
			if (zms_ftl->zone_agg_pgs[i] > 0) {
				if (zms_ftl->pslc_full) {
					NVMEV_ERROR("Agg small writes failed because slc full!\n");
					break;
				}

				for (int j = 0; j < zms_ftl->zone_agg_pgs[i]; j++) {
					uint64_t lpn = zms_ftl->zone_agg_lpns[i][j];
					struct ppa ppa = get_maptbl_ent(zms_ftl, lpn);
					if (mapped_ppa(&ppa)) {
						if ((line->parent_id == -1 && ppa.zms.blk == line->blkid) ||
							(line->parent_id != -1 && get_blk(zms_ftl->ssd, &ppa) == blk)) {
							agg_lpns[agg_len] = lpn;
							agg_len++;
						}
					} else {
						NVMEV_ERROR("migrate invalid ppa??\n");
					}

					if (agg_len == spp->pslc_pgs_per_oneshotpg) {
						internal_write(zms_ftl, agg_lpns, 0, agg_len, io_type, LOC_PSLC, 0);
						agg_len = 0;
					}
				}
			}
		}
	}

	// Handle data not aligned to programming units
	if (agg_len > 0) {
		if (io_type == GC_IO && dest_loc == LOC_NORMAL) {
			// We need to place misaligned GC writes from LOC_NORMAL into the write buffer to
			// prevent this data from occupying the SLC region.
			if (zms_ftl->num_aggs > 1) {
				NVMEV_ERROR("BAD NUM AGGS!(%d)\n", zms_ftl->num_aggs);
				NVMEV_ASSERT(0);
			}

			if (agg_len > zms_ftl->gc_agg_ttlpns) {
				NVMEV_ERROR("BAD AGG IN GC!\n");
				NVMEV_ASSERT(0);
			}

			for (int i = 0; i < agg_len; i++) {
				uint64_t lpn = agg_lpns[i];
				int len = zms_ftl->gc_agg_len;
				zms_ftl->gc_agg_lpns[len] = lpn;
				zms_ftl->gc_agg_len++;

				ppa = get_maptbl_ent(zms_ftl, lpn);

				if (mapped_ppa(&ppa)) {
					mark_page_invalid(zms_ftl, &ppa);
					set_rmap_ent(zms_ftl, INVALID_LPN, &ppa);
					zms_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;
				} else {
					NVMEV_ERROR("BAD AGG IN GC!!: ppa unmapped!\n");
					NVMEV_ASSERT(0);
				}
			}

		} else {
			internal_write(zms_ftl, agg_lpns, 0, agg_len, io_type, LOC_PSLC, 0);
		}
	}
	vfree(agg_lpns);
}

static void erase_line(struct zms_ftl *zms_ftl, struct zms_line *line, int io_type)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct znsparams *zpp = &zms_ftl->zp;

	struct ppa e_ppa = get_first_page(zms_ftl, line);

	if (line->parent_id == -1) {
		int ch, lun;
		for (ch = 0; ch < spp->nchs; ch++) {
			for (lun = 0; lun < spp->luns_per_ch; lun++) {
				e_ppa.zms.ch = ch;
				e_ppa.zms.lun = lun;
				mark_block_free(zms_ftl, &e_ppa);

				if (io_type != GC_IO || zms_ftl->zp.enable_gc_delay) {
					struct nand_cmd ecmd = {
						.type = io_type,
						.cmd = NAND_ERASE,
						.stime = 0,
						.interleave_pci_dma = false,
						.ppa = e_ppa,
					};
					submit_nand_cmd(zms_ftl->ssd, &ecmd);
				}
			}
		}
	} else {
		mark_block_free(zms_ftl, &e_ppa);

		if (io_type != GC_IO || zms_ftl->zp.enable_gc_delay) {
			struct nand_cmd ecmd = {
				.type = io_type,
				.cmd = NAND_ERASE,
				.stime = 0,
				.interleave_pci_dma = false,
				.ppa = e_ppa,
			};
			submit_nand_cmd(zms_ftl->ssd, &ecmd);
		}
	}

	NVMEV_CONZONE_GC_DEBUG("Erase line id = %d, ipc = %d, vpc = %d, rpc = %d all = %lu!\n",
						   line->id, line->ipc, line->vpc, line->rpc, line->pgs_per_line);

	mark_line_free(zms_ftl, line, io_type);
}

static struct zms_line *do_migrate(struct zms_ftl *zms_ftl, int io_type)
{
	struct zms_line *sblk_line = NULL;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct ppa ppa;
	uint64_t lpn;
	uint32_t zid;

	// choose a pslc sblk line to migrate
	sblk_line = get_migrate_line(zms_ftl);
	if (sblk_line == NULL) {
		return NULL;
	}
	NVMEV_CONZONE_GC_DEBUG("get migrate line id: %d vpc: %d ipc: %d rpc :%d\n", sblk_line->id,
						   sblk_line->vpc, sblk_line->ipc, sblk_line->rpc);

	// copy flashpages: pSLC -> normal or pSLC -> pSLC
	if (zms_ftl->device_full || zms_ftl->pslc_full) {
		NVMEV_ERROR("%s failed because device (%d) or pslc (%d) full!\n", __func__,
					zms_ftl->device_full, zms_ftl->pslc_full);
		return NULL;
	}

	submit_internal_write(zms_ftl, sblk_line, io_type);
	erase_line(zms_ftl, sblk_line, io_type);
	NVMEV_CONZONE_GC_DEBUG("migrate end line id: %d  vpc: %d ipc: %d rpc:%d\n", sblk_line->id,
						   sblk_line->vpc, sblk_line->ipc, sblk_line->rpc);
	return sblk_line;
}

static void try_migrate(struct zms_ftl *zms_ftl)
{
	if (!SLC_BYPASS && should_migrate_low(zms_ftl)) {
		struct ppa ppa;
		struct zms_line *sblk_line = NULL;
		// check if now we are migrating
		if (check_migrating(zms_ftl)) {
			return;
		}

		int io_type = MIGRATE_IO;
		sblk_line = do_migrate(zms_ftl, io_type);
		if (!sblk_line)
			return;

		// release pSLC lines
		struct zms_line_mgmt *lm = &zms_ftl->lm;
		for (int i = 0; i < lm->tt_lines; i++) {
			if (lm->lines[i].sub_lines) {
				for (int j = 0; j < zms_ftl->ssd->sp.blks_per_line; j++) {
					if (get_line_location(zms_ftl, &lm->lines[i].sub_lines[j]) == LOC_PSLC &&
						lm->lines[i].sub_lines[j].ipc == lm->lines[i].sub_lines[j].pgs_per_line &&
						sblk_line != &lm->lines[i].sub_lines[j]) {
						NVMEV_INFO("try direct earse line %d parent id %d pgs per line %ld\n", j, i,
								   lm->lines[i].pgs_per_line);
						erase_line(zms_ftl, &lm->lines[i].sub_lines[j], io_type);
					}
				}
			} else {
				if (get_line_location(zms_ftl, &lm->lines[i]) == LOC_PSLC &&
					lm->lines[i].pgs_per_line == zms_ftl->ssd->sp.pslc_pgs_per_line &&
					lm->lines[i].ipc == lm->lines[i].pgs_per_line && sblk_line != &lm->lines[i]) {
					NVMEV_INFO("try direct earse line %d pgs per line %ld\n", i,
							   lm->lines[i].pgs_per_line);
					erase_line(zms_ftl, &lm->lines[i], io_type);
				}
			}
		}
	}
}

static int do_gc(struct zms_ftl *zms_ftl, bool force, int location)
{
	struct zms_line *victim_line = NULL;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct znsparams *zpp = &zms_ftl->zp;
	struct zms_write_flow_control *wfc = location ? &zms_ftl->pslc_wfc : &zms_ftl->wfc;
	struct ppa ppa;
	int flashpg;
	int flashpgs_per_blk = location ? spp->pslc_flashpgs_per_blk : spp->flashpgs_per_blk;
	int pgs_per_flashpg = location ? spp->pslc_pgs_per_flashpg : spp->pgs_per_flashpg;

	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
		NVMEV_CONZONE_GC_DEBUG_VERBOSE(
			"GC choose lines %s [%s] victim= %d/%d ,full= %d/%d ,free= %d/%d, all = %d/%d\n",
			location ? "pslc" : "normal",
			zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META ? "meta" : "data",
			zms_ftl->lm.victim_line_cnt, zms_ftl->lm.pslc_victim_line_cnt,
			zms_ftl->lm.full_line_cnt, zms_ftl->lm.pslc_full_line_cnt, zms_ftl->lm.free_line_cnt,
			zms_ftl->lm.pslc_free_line_cnt, zms_ftl->lm.tt_lines - zms_ftl->lm.pslc_tt_lines,
			zms_ftl->lm.pslc_tt_lines);
	}

	victim_line = select_victim_line(zms_ftl, force, location);
	if (!victim_line) {
		return -1;
	}
	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
		NVMEV_CONZONE_GC_DEBUG("GC-ing %s [%s] line:%d(ipc=%d,vpc=%d,rpc=%d) gc count %d\n",
							   location ? "pslc" : "normal",
							   zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META ? "meta" : "data",
							   victim_line->id, victim_line->ipc, victim_line->vpc,
							   victim_line->rpc, zms_ftl->gc_count);
	}

	wfc->credits_to_refill = victim_line->ipc + victim_line->rpc;

	submit_internal_write(zms_ftl, victim_line, GC_IO);
	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
		NVMEV_CONZONE_GC_DEBUG("earse line %d,%d after GC [pgs per line %ld]\n",
							   victim_line->parent_id, victim_line->id, victim_line->pgs_per_line);
	}
	erase_line(zms_ftl, victim_line, GC_IO);
	zms_ftl->gc_count++;

	// if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
	// 	NVMEV_CONZONE_GC_DEBUG("After erase %s [%s] line:%d(ipc=%d,vpc=%d)\n",
	// 						   location ? "pslc" : "normal",
	// 						   zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META ? "meta" : "data",
	// 						   victim_line->id, victim_line->ipc, victim_line->vpc);
	// }
	// print_lines(zms_ftl);

	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
		NVMEV_CONZONE_GC_DEBUG_VERBOSE(
			"[GC END] %s [%s] victim= %d/%d ,full= %d/%d ,free= %d/%d, all = %d/%d \n",
			location ? "pslc" : "normal",
			zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META ? "meta" : "data",
			zms_ftl->lm.victim_line_cnt, zms_ftl->lm.pslc_victim_line_cnt,
			zms_ftl->lm.full_line_cnt, zms_ftl->lm.pslc_full_line_cnt, zms_ftl->lm.free_line_cnt,
			zms_ftl->lm.pslc_free_line_cnt, zms_ftl->lm.tt_lines - zms_ftl->lm.pslc_tt_lines,
			zms_ftl->lm.pslc_tt_lines);
	}
	return 0;
}

static void foreground_gc(struct zms_ftl *zms_ftl, int location)
{
	struct zms_line_mgmt *lm = &zms_ftl->lm;
	struct zms_write_flow_control *wfc = location == LOC_PSLC ? &zms_ftl->pslc_wfc : &zms_ftl->wfc;
	if (should_gc_high(zms_ftl, location)) {
		wfc->credits_to_refill = 0;
		if (zms_ftl->device_full || zms_ftl->pslc_full) {
			NVMEV_ERROR("[%d] %s device full %d, pslc full %d\n", zms_ftl->zp.ns->id, __func__,
						zms_ftl->device_full, zms_ftl->pslc_full);
			return;
		}

		do_gc(zms_ftl, true, location);
		if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META) {
			NVMEV_INFO("credits_to_refill after GC for %ld: %ld\n", wfc->credits_to_refill,
					   location);
		}
	} else {
		wfc->credits_to_refill =
			location == LOC_PSLC ? zms_ftl->zp.pslc_pgs_per_line : zms_ftl->zp.pgs_per_line;
	}
}

static uint32_t zns_write_check(struct zms_ftl *zms_ftl, struct nvme_rw_command *cmd)
{
	if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_ZONED)
		return NVME_SC_SUCCESS;
	struct zone_descriptor *zone_descs = zms_ftl->zone_descs;
	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);
	uint32_t zid = lba_to_zone((struct zns_ftl *)(&(*zms_ftl)), slba);
	enum zone_state state = zone_descs[zid].state;

	if (__check_boundary_error((struct zns_ftl *)(&(*zms_ftl)), slba, nr_lba) == false) {
		// return boundary error
		return NVME_SC_ZNS_ERR_BOUNDARY;
	}

	// check if slba == current write pointer
	if (slba != zone_descs[zid].wp) {
		NVMEV_ERROR("%s WP error slba %lld nr_lba %lld zone_id %d wp %lld cap %lld state %d opcode "
					"0x%x\n",
					__func__, slba, nr_lba, zid, zms_ftl->zone_descs[zid].wp,
					zone_descs[zid].zone_capacity, state, cmd->opcode);
		return NVME_SC_ZNS_INVALID_WRITE;
	}

	if (zone_descs[zid].wp + nr_lba >
		(zone_to_slba((struct zns_ftl *)(&(*zms_ftl)), zid) + zone_descs[zid].zone_capacity)) {
		NVMEV_ERROR("[%s] Trying to write invalid area!!\n", __func__);
		return NVME_SC_ZNS_INVALID_WRITE;
	}

	// NVMEV_INFO("%s zid %d slba %lld wp %lld cap
	// %lld\n",__func__,zid,slba,zms_ftl->zone_descs[zid].wp,zone_descs[zid].zone_capacity);
	switch (state) {
	case ZONE_STATE_EMPTY: {
		// check if slba == start lba in zone
		if (slba != zone_descs[zid].zslba) {
			return NVME_SC_ZNS_INVALID_WRITE;
		}

		if (is_zone_resource_full((struct zns_ftl *)(&(*zms_ftl)), ACTIVE_ZONE)) {
			return NVME_SC_ZNS_NO_ACTIVE_ZONE;
		}
		if (is_zone_resource_full((struct zns_ftl *)(&(*zms_ftl)), OPEN_ZONE)) {
			return NVME_SC_ZNS_NO_OPEN_ZONE;
		}
		acquire_zone_resource((struct zns_ftl *)(&(*zms_ftl)), ACTIVE_ZONE);
		// go through
	}
	case ZONE_STATE_CLOSED: {
		if (acquire_zone_resource((struct zns_ftl *)(&(*zms_ftl)), OPEN_ZONE) == false) {
			return NVME_SC_ZNS_NO_OPEN_ZONE;
		}

		// change to ZSIO
		change_zone_state((struct zns_ftl *)(&(*zms_ftl)), zid, ZONE_STATE_OPENED_IMPL);
		break;
	}
	case ZONE_STATE_OPENED_IMPL:
	case ZONE_STATE_OPENED_EXPL: {
		break;
	}
	case ZONE_STATE_FULL:
		return NVME_SC_ZNS_ERR_FULL;
	case ZONE_STATE_READ_ONLY:
		return NVME_SC_ZNS_ERR_READ_ONLY;
	case ZONE_STATE_OFFLINE:
		return NVME_SC_ZNS_ERR_OFFLINE;
	}

	__increase_write_ptr((struct zns_ftl *)(&(*zms_ftl)), zid, nr_lba);
	return NVME_SC_SUCCESS;
}

static struct buffer *__zms_wb_get(struct zms_ftl *zms_ftl, uint64_t slpn)
{
	struct buffer *write_buffer = NULL;
	if (zms_ftl->ssd->sp.write_buffer_size) {
		write_buffer = zms_ftl->ssd->write_buffer;
	} else {
		if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
			uint32_t zid = lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), slpn);
			if (WB_MGNT == WB_STATIC) {
				for (int i = 0; i < zms_ftl->zp.nr_wb; i++) {
					if (zms_ftl->write_buffer[i].zid == zid) {
						write_buffer = &(zms_ftl->write_buffer[i]);
						break;
					}
				}
				if (!write_buffer) {
					bool some_one_flushing = false;
					for (int i = 0; i < zms_ftl->zp.nr_wb; i++) {
						if (!zms_ftl->write_buffer[i].flush_data &&
							!zms_ftl->write_buffer[i].flushing) {
							write_buffer = &(zms_ftl->write_buffer[i]);
							zms_ftl->write_buffer[i].zid = zid; // assign // TODO delete
							break;
						}
						if (zms_ftl->write_buffer[i].flushing)
							some_one_flushing = true;
					}

					if (!write_buffer && !some_one_flushing) {
						size_t max_flush_data = zms_ftl->write_buffer[0].flush_data;
						int wid = 0;

						// if (!SLC_BYPASS) {
						// 	NVMEV_INFO("MAX OPEN ZONES >= %d? zid = %d slpn = %lld\n",
						// 			   zms_ftl->zp.nr_wb, zid, slpn);
						// }
						for (int i = 0; i < zms_ftl->zp.nr_wb; i++) {
							// if (!SLC_BYPASS) {
							// 	NVMEV_INFO("write buffer %d zid %d flushing %d flush data %ld pgs "
							// 			   "%lld slpn "
							// 			   "%lld \n",
							// 			   i, zms_ftl->write_buffer[i].zid,
							// 			   zms_ftl->write_buffer[i].flushing,
							// 			   zms_ftl->write_buffer[i].flush_data,
							// 			   zms_ftl->write_buffer[i].pgs,
							// 			   zms_ftl->write_buffer[i].lpns[0]);
							// }
							if (max_flush_data < zms_ftl->write_buffer[i].flush_data) {
								max_flush_data = zms_ftl->write_buffer[i].flush_data;
								wid = i;
							}
						}
						// if (!SLC_BYPASS) {
						// 	NVMEV_INFO("Choose write buffer %d \n", wid);
						// }
						write_buffer = &zms_ftl->write_buffer[wid];
					}
				}
			} else if (WB_MGNT == WB_MOD) {
				write_buffer = &(zms_ftl->write_buffer[zid % zms_ftl->zp.nr_wb]);
			} else {
				NVMEV_ERROR("Undefined WB_MGNT!\n");
			}
		} else {
			if (zms_ftl->zp.nr_wb != 1) {
				NVMEV_ERROR("too many write buffer : %d, return write_buffer[0] \n",
							zms_ftl->zp.nr_wb);
			}
			write_buffer = &(zms_ftl->write_buffer[0]);
		}
	}
	return write_buffer;
}

static int __zms_wb_check(struct zms_ftl *zms_ftl, struct buffer *write_buffer, uint64_t slpn)
{
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
		uint32_t zid = lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), slpn);
		if (write_buffer->zid != -1 && write_buffer->zid != zid)
			return FAILURE;
		return SUCCESS;
	} else {
		if (write_buffer->zid != -1)
			return FAILURE;
		return SUCCESS;
	}
}

static bool __zms_wb_hit(struct zms_ftl *zms_ftl, struct buffer *write_buffer, uint64_t lpn)
{
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
		if (write_buffer->zid != -1 && write_buffer->lpns[0] != INVALID_LPN &&
			lpn >= write_buffer->lpns[0] && lpn < write_buffer->lpns[0] + write_buffer->pgs) {
			return true;
		}
		return false;
	} else {
		for (int i = 0; i < write_buffer->pgs; i++) {
			if (write_buffer->lpns[i] == lpn)
				return true;
		}
		return false;
	}
}

// Return the location where the write buffer is flushed to flash.
static int get_flush_target_location(struct zms_ftl *zms_ftl, struct buffer *write_buffer,
									 int written_pgs)
{
	int loc = LOC_PSLC;
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_META)
		loc = LOC_PSLC;
	else if (!SLC_BYPASS)
		loc = LOC_PSLC;
	else if (NORMAL_ONLY)
		loc = LOC_NORMAL;
	else {
		// check alignment for data
		uint64_t lpn = write_buffer->lpns[written_pgs];
		int agg_idx = get_aggidx(zms_ftl, lpn);
		uint64_t to_write_pgs = write_buffer->pgs - written_pgs;
		if (zms_ftl->zone_agg_pgs[agg_idx] + to_write_pgs < zms_ftl->ssd->sp.pgs_per_oneshotpg) {
			loc = LOC_PSLC;
			zms_ftl->flush_to_slc++;
		} else {
			loc = LOC_NORMAL;
			zms_ftl->flush_to_regular++;
		}
	}
	return loc;
}

uint64_t buffer_flush(struct zms_ftl *zms_ftl, struct buffer *write_buffer, uint64_t nsecs_start)
{
	if (write_buffer->flushing)
		return nsecs_start;
	struct ppa ppa;
	uint64_t lpn;
	uint64_t slpn = write_buffer->lpns[0];
	uint64_t elpn = write_buffer->lpns[write_buffer->pgs - 1];
	uint64_t pgs = 0;
	uint64_t nsecs_latest = nsecs_start;
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	bool loc = LOC_NORMAL;
	int agg_idx = 0, agg_len = 0;
	uint64_t *agg_lpns = NULL;

	/***
	 * For the zone interface, each write buffer serves a different zone, so each write buffer
	 * corresponds to one aggregation array. For the block interface, regardless of whether it's
	 * used for data or metadata, the write buffers all correspond to the same aggregation
	 * array. Therefore, there is a one-to-one relationship between the write buffers and the
	 * aggregation arrays.
	 */
	agg_idx = get_aggidx(zms_ftl, slpn);
	agg_len = zms_ftl->zone_agg_pgs[agg_idx];
	agg_lpns = zms_ftl->zone_agg_lpns[agg_idx];

	// Overwriting the updated LPNs in the aggregation array, eliminating duplicate LPNs.
	// Note that there are no duplicate LPNs in write_buffer->lpns
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_BLOCK) {
		if (agg_len > 0) {
			for (int i = 0; i < write_buffer->pgs; i++) {
				for (int j = 0; j < agg_len; j++) {
					if (agg_lpns[j] == write_buffer->lpns[i]) {
						for (int k = j + 1; k < agg_len; k++) {
							agg_lpns[k - 1] = agg_lpns[k];
						}
						agg_len--;
						break;
					}
				}
			}
			zms_ftl->zone_agg_pgs[agg_idx] = agg_len;
		}

		// If the user updates misaligned GC data, we first need to delete it from gc_agg_lpn.
		if (zms_ftl->gc_agg_len > 0) {
			for (int i = 0; i < write_buffer->pgs; i++) {
				for (int j = 0; j < zms_ftl->gc_agg_len; j++) {
					if (zms_ftl->gc_agg_lpns[j] == write_buffer->lpns[i]) {
						for (int k = j + 1; k < zms_ftl->gc_agg_len; k++) {
							zms_ftl->gc_agg_lpns[k - 1] = zms_ftl->gc_agg_lpns[k];
						}
						zms_ftl->gc_agg_len--;
						break;
					}
				}
			}
		}
	}

	int to_write_pgs = 0;
	for (int i = 0; i < write_buffer->pgs; i += pgs) {
		lpn = write_buffer->lpns[i];
		loc = get_flush_target_location(zms_ftl, write_buffer, i);
		agg_len = zms_ftl->zone_agg_pgs[agg_idx];

		// NVMEV_INFO("loc %d lpn %lld agg idx %d agg_len %d\n", loc, lpn, agg_idx, agg_len);
		if (loc == LOC_NORMAL && agg_len > 0) {
			// Do Migrate for SLC_BYPASS && USER_IO
			pgs = spp->pgs_per_oneshotpg - agg_len;
			for (int j = 0; j < pgs; j++) {
				agg_lpns[agg_len] = write_buffer->lpns[i + j];
				agg_len++;
				ppa = get_maptbl_ent(zms_ftl, write_buffer->lpns[i + j]);
				if (mapped_ppa(&ppa)) {
					if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_ZONED) {
						// update the metadata of expired page in internal_write
						zms_ftl->inplace_update++;
					} else {
						NVMEV_ERROR("update in zoned storage?? lpn %lld loc %d ppa loc %d ppaidx "
									"%lld\n",
									write_buffer->lpns[i + j], loc,
									check_location_type(zms_ftl, &ppa), ppa_2_pgidx(zms_ftl, &ppa));
					}
				}
			}
			uint64_t complete_time =
				internal_write(zms_ftl, agg_lpns, 0, agg_len, USER_IO, LOC_NORMAL, nsecs_start);
			nsecs_latest = max(nsecs_latest, complete_time);
			NVMEV_CONZONE_PRINT_TIME("%s latest %llu complete %llu lat %llu us\n", __func__,
									 nsecs_latest, complete_time,
									 (nsecs_latest - nsecs_start) / 1000);
			zms_ftl->zone_agg_pgs[agg_idx] = 0;
		} else {
			uint64_t pgs_per_oneshotpg =
				(loc == LOC_PSLC) ? spp->pslc_pgs_per_oneshotpg : spp->pgs_per_oneshotpg;
			pgs = min(pgs_per_oneshotpg, write_buffer->pgs - i);

			for (int j = 0; j < pgs; j++) {
				lpn = write_buffer->lpns[i + j];
				ppa = get_maptbl_ent(zms_ftl, lpn);
				if (mapped_ppa(&ppa)) {
					if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_ZONED) {
						/* update old page information first */
						mark_page_invalid(zms_ftl, &ppa);
						set_rmap_ent(zms_ftl, INVALID_LPN, &ppa);
						zms_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;

						zms_ftl->inplace_update++;

					} else {
						NVMEV_ERROR("update in zoned storage?? lpn %lld loc %d ppa loc %d ppaidx "
									"%lld\n",
									lpn, loc, check_location_type(zms_ftl, &ppa),
									ppa_2_pgidx(zms_ftl, &ppa));
					}
				}
				if (SLC_BYPASS && zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META && loc == LOC_PSLC) {
					zms_ftl->zone_agg_lpns[agg_idx][agg_len] = lpn;
					agg_len++;
				}
				to_write_pgs++;
				uint64_t complete_time =
					nand_write(zms_ftl, nsecs_start, lpn, loc, USER_IO, to_write_pgs, elpn);
				if (complete_time != nsecs_start) {
					to_write_pgs = 0;
				}
				nsecs_latest = max(nsecs_latest, complete_time);
			}
			if (SLC_BYPASS && zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_META && loc == LOC_PSLC) {
				zms_ftl->zone_agg_pgs[agg_idx] = agg_len;
			}
		}

		if (zms_ftl->device_full || zms_ftl->pslc_full) {
			NVMEV_ERROR("[%d] %s: device full %d, pslc full %d\n", zms_ftl->zp.ns->id, __func__,
						zms_ftl->device_full, zms_ftl->pslc_full);
			break;
		}
		NVMEV_CONZONE_PRINT_TIME("[%d] Flush: slpn %lld pgs %lld zid %d lat %lld us\n",
								 zms_ftl->zp.ns->id, lpn, pgs, write_buffer->zid,
								 (nsecs_latest - nsecs_start) / 1000);
	}
	NVMEV_CONZONE_PRINT_TIME("[END] [%d] flush slpn %lld pgs %lld zid %d lat %lld us\n",
							 zms_ftl->zp.ns->id, write_buffer->lpns[0], write_buffer->pgs,
							 write_buffer->zid, (nsecs_latest - nsecs_start) / 1000);

	NVMEV_CONZONE_PRINT_BW("BW: %ld/%lld KiB/us\n", BYTE_TO_KB(write_buffer->flush_data),
						   (nsecs_latest - write_buffer->time) / 1000);

	zms_ftl->host_w_pgs += write_buffer->pgs;
	if (write_buffer->flush_data < write_buffer->capacity) {
		zms_ftl->early_flush_cnt++;
	}
	write_buffer->flushing = true;
	size_t bufs_to_release = write_buffer->flush_data;
	write_buffer->flush_data = 0;
	for (int i = 0; i < write_buffer->pgs; i++) {
		write_buffer->lpns[i] = INVALID_LPN;
	}
	write_buffer->pgs = 0;
	write_buffer->zid = -1;
	write_buffer->time = nsecs_latest;
	schedule_internal_operation(write_buffer->sqid, nsecs_latest, write_buffer, bufs_to_release);
	return nsecs_latest;
}

static void print_writebuffer(struct zms_ftl *zms_ftl, struct buffer *write_buffer)
{
	int pgs = 1;
	uint64_t slpn = write_buffer->lpns[0];
	NVMEV_INFO("---------print write buffer-----------\n");
	for (int i = 1; i < write_buffer->pgs; i++) {
		if (write_buffer->lpns[i] != write_buffer->lpns[i - 1] + 1) {
			NVMEV_INFO("slpn %lld pgs %d zid %u\n", slpn, pgs,
					   lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), slpn));
			pgs = 1;
			slpn = write_buffer->lpns[i];
		} else
			pgs++;
	}
	NVMEV_INFO("slpn %lld pgs %d zid %u\n", slpn, pgs,
			   lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), slpn));
	NVMEV_INFO("---------print write buffer end-----------\n");
}

static bool handle_write_request(struct zms_ftl *zms_ftl, struct nvmev_request *req,
								 struct nvmev_result *ret)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct nvme_rw_command *cmd = &(req->cmd->rw);

	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);
	uint64_t slpn, elpn, lpn;

	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_xfer_completed = nsecs_start;
	uint64_t nsecs_latest = nsecs_start;
	uint32_t status = NVME_SC_SUCCESS;
	zms_ftl->current_time = nsecs_start;

	if (zms_ftl->device_full || zms_ftl->pslc_full) {
		status = NVME_SC_CAP_EXCEEDED;
		goto out;
	}

	uint64_t pgs = 0;
	uint32_t allocated_buf_size = 0;

	struct buffer *write_buffer;
	int i, j;
	uint32_t zid = zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED
					   ? lba_to_zone((struct zns_ftl *)(&(*zms_ftl)), slba)
					   : -1;

	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && cmd->opcode == nvme_cmd_zone_append) {
		slba = zms_ftl->zone_descs[zid].wp;
		cmd->slba = slba;
	}

	slpn = lba_to_lpn((struct zns_ftl *)(&(*zms_ftl)), slba);
	elpn = lba_to_lpn((struct zns_ftl *)(&(*zms_ftl)), slba + nr_lba - 1);

	if (nr_lba < spp->secs_per_pg) {
		NVMEV_CONZONE_RW_DEBUG("TODO: nsid %d not a page writing!\n", zms_ftl->zp.ns->id);
		if (zms_ftl->nopg_last_lpn == slpn)
			goto out;
		else
			zms_ftl->nopg_last_lpn = slpn;
	}

	write_buffer = __zms_wb_get(zms_ftl, slpn);
	if (!write_buffer) // wait for flushing
		return false;

	if (zms_ftl->last_nlb != nr_lba || zms_ftl->last_slba != slba) {
		NVMEV_CONZONE_RW_DEBUG_VERBOSE(
			"[w] nsid %d slba %lld nlb %lld write buffer flushing? %d maxfree %lld us\n",
			zms_ftl->zp.ns->id, slba, nr_lba, write_buffer->flushing,
			check_maxfreetime(zms_ftl) / 1000);

		zms_ftl->last_nlb = nr_lba;
		zms_ftl->last_slba = slba;
		zms_ftl->last_stime = nsecs_start;
		NVMEV_INFO("write to zid %d\n", zid);
		print_writebuffer_info(zms_ftl);
	}

	if (__zms_wb_check(zms_ftl, write_buffer, slpn) != SUCCESS) {
		if (!write_buffer->flushing) {
			// should flush
			NVMEV_INFO("[w] ZONE SWITCHING %d -> %d\n", write_buffer->zid, zid);
			// print_writebuffer(zms_ftl, write_buffer);
			// print_writebuffer_info(zms_ftl);
			nsecs_latest = buffer_flush(zms_ftl, write_buffer, nsecs_start);
		}
		return false;
	}

	if ((LBA_TO_BYTE(nr_lba) % spp->write_unit_size) != 0) {
		status = NVME_SC_ZNS_INVALID_WRITE;
		goto out;
	}

	status = zns_write_check(zms_ftl, cmd);
	if (status != NVME_SC_SUCCESS)
		goto out;

	write_buffer->sqid = req->sq_id;
	for (lpn = slpn; lpn <= elpn; lpn += pgs) {
		pgs = min(elpn - lpn + 1, (uint64_t)(write_buffer->capacity / spp->pgsz));

		uint64_t *lpns = NULL;
		uint64_t wpgs = pgs;
		// check update write
		if (zms_ftl->zp.ns_type != SSD_TYPE_CONZONE_ZONED) {
			lpns = kmalloc(sizeof(uint64_t) * pgs, GFP_KERNEL);
			for (i = 0, j = 0; i < pgs; i++) {
				if (!__zms_wb_hit(zms_ftl, write_buffer, slpn + i)) {
					lpns[j++] = slpn + i;
				}
			}
			wpgs = j;
		}

		while (wpgs > 0 && allocated_buf_size == 0) {
			// lock contention
			allocated_buf_size = buffer_allocate(write_buffer, wpgs * spp->pgsz);
		}

		if (allocated_buf_size < wpgs * spp->pgsz) {
			wpgs = (uint64_t)(allocated_buf_size / spp->pgsz);
			pgs = wpgs;
			allocated_buf_size = 0;
		} else
			allocated_buf_size -= spp->pgsz * wpgs;

		if (wpgs == 0)
			continue;

		for (i = write_buffer->pgs, j = 0; i < write_buffer->pgs + wpgs && j < wpgs; i++, j++) {
			write_buffer->lpns[i] = lpns != NULL ? lpns[j] : lpn + j;
		}
		write_buffer->pgs += wpgs;
		write_buffer->flush_data += spp->pgsz * wpgs;
		if (lpns) {
			kfree(lpns);
			lpns = NULL;
		}

		NVMEV_CONZONE_RW_DEBUG_VERBOSE(
			"write buffer %d data(KiB): %ld / %ld,remaining %ld, slpn %lld pgs %lld\n",
			write_buffer->zid, BYTE_TO_KB(write_buffer->flush_data),
			BYTE_TO_KB(write_buffer->capacity), BYTE_TO_KB(write_buffer->remaining),
			write_buffer->lpns[0], write_buffer->pgs);

		// NVMEV_INFO("write buffer %d lpn %lld wpgs %lld, slpn %lld elpn %lld data(KiB): %ld /
		// %ld\n",write_buffer->zid,lpn,pgs,slpn,elpn,BYTE_TO_KB(write_buffer->flush_data),BYTE_TO_KB(write_buffer->capacity));

		// get delay from nand model
		nsecs_latest = ssd_advance_write_buffer(zms_ftl->ssd, nsecs_latest, wpgs * spp->pgsz);
		nsecs_xfer_completed = nsecs_latest;

		if (write_buffer->zid == -1) {
			write_buffer->zid = zid;
			if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
				// NVMEV_INFO("write buffer is ready!\n");
				// print_writebuffer_info(zms_ftl);
			}
		}

		write_buffer->time = nsecs_xfer_completed;

		/* Aggregate write io or unaligned io*/
		//|| lpn + pgs >= elpn
		if (write_buffer->flush_data == write_buffer->capacity) {
			uint64_t nsecs_completed = buffer_flush(zms_ftl, write_buffer, nsecs_xfer_completed);
			nsecs_latest = max(nsecs_completed, nsecs_latest);
		}
		if (zms_ftl->device_full || zms_ftl->pslc_full)
			break;
	}

	if (zms_ftl->device_full || zms_ftl->pslc_full) {
		status = NVME_SC_CAP_EXCEEDED;
		goto out;
	}
out:
	// if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED) {
	// 	NVMEV_INFO("handle write request end\n");
	// 	print_writebuffer_info(zms_ftl);
	// }
	ret->status = status;
	if ((cmd->control & NVME_RW_FUA) ||
		(spp->write_early_completion == 0)) /*Wait all flash operations*/
	{
		ret->nsecs_target = nsecs_latest;
	} else /*Early completion*/
		ret->nsecs_target = nsecs_xfer_completed;

	zms_ftl->host_wrequest_cnt++;
	NVMEV_CONZONE_RW_DEBUG("w nsid %d zid %d slpn %lld pgs %lld lat %lld us\n", zms_ftl->zp.ns->id,
						   zid, slpn, (elpn - slpn + 1),
						   (ret->nsecs_target - zms_ftl->last_stime) / 1000);
	return true;
}

static uint64_t map_read(struct zms_ftl *zms_ftl, uint64_t lpn, uint64_t nsecs_start)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	uint64_t nsecs_map_latest = nsecs_start;
	int cache_idx;
	int sidx;
	if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && L2P_HYBRID_MAP)
		sidx = 0;
	else
		sidx = NUM_MAP - 1;

	for (int i = sidx; i < NUM_MAP; i++) {
		uint64_t map_slpn = get_granularity_start_lpn(zms_ftl, lpn, MAP_GRAN(i));
		nsecs_map_latest = nand_read(zms_ftl, &map_slpn, 0, 1, MAP_READ_IO, nsecs_map_latest);
		struct ppa ppa = get_maptbl_ent(zms_ftl, map_slpn);
		if (mapped_ppa(&ppa) && ppa.zms.map == MAP_GRAN(i)) {
			int is_resident = check_resident(zms_ftl, MAP_GRAN(i));
			cache_idx = l2p_insert(zms_ftl, map_slpn, MAP_GRAN(i), is_resident);
			set_l2pcacheidx(zms_ftl, map_slpn, cache_idx);
			if (cache_idx == -1) {
				NVMEV_INFO("l2p insert failed?? map slpn %lld mapgran %d isresident %d\n", map_slpn,
						   MAP_GRAN(i), is_resident);
			} else {
				l2p_access(zms_ftl, map_slpn, cache_idx);
			}

			if (MAP_GRAN(i) == PAGE_MAP) {
				// pre read for page map
				for (int j = 0; j < zms_ftl->zp.pre_read; j++) {
					if (map_slpn + j >= zms_ftl->zp.tt_lpns) {
						break;
					}
					struct ppa prer_ppa = get_maptbl_ent(zms_ftl, map_slpn + j);
					if (mapped_ppa(&prer_ppa)) {
						cache_idx = l2p_insert(zms_ftl, map_slpn + j, prer_ppa.zms.map, 0);
						set_l2pcacheidx(zms_ftl, map_slpn + j, cache_idx);
					}
				}
			}
			zms_ftl->l2p_misses++;
			break;
		}
	}

	return nsecs_map_latest;
}

static bool handle_read_request(struct zms_ftl *zms_ftl, struct nvmev_request *req,
								struct nvmev_result *ret)
{
	struct ssdparams *spp = &zms_ftl->ssd->sp;
	struct nvme_rw_command *cmd = &(req->cmd->rw);

	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);

	uint64_t slpn = lba_to_lpn((struct zns_ftl *)(&(*zms_ftl)), slba);
	uint64_t elpn = lba_to_lpn((struct zns_ftl *)(&(*zms_ftl)), slba + nr_lba - 1);
	uint64_t lpn;

	uint32_t status = NVME_SC_SUCCESS;
	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed = nsecs_start, nsecs_latest = 0;

	uint64_t wb_read_pgs = 0;

	zms_ftl->current_time = nsecs_start;

	// get delay from nand model
	nsecs_latest = nsecs_start;
	if (LBA_TO_BYTE(nr_lba) <= KB(4))
		nsecs_latest += spp->fw_4kb_rd_lat;
	else
		nsecs_latest += spp->fw_rd_lat;

	bool interleave_pci_dma = false;
	uint64_t nsecs_read_begin = nsecs_latest;
	uint64_t nsecs_wb_read_begin = nsecs_latest;
	uint64_t *to_read_lpns = kmalloc(sizeof(uint64_t) * (elpn - slpn + 1), GFP_KERNEL);
	int to_read_pgs = 0;

	for (lpn = slpn; lpn <= elpn; lpn++) {
		zms_ftl->host_r_pgs++;
		// check if this page in write buffer
		struct buffer *write_buffer = __zms_wb_get(zms_ftl, slpn);
		if (write_buffer && __zms_wb_check(zms_ftl, write_buffer, slpn) == SUCCESS &&
			__zms_wb_hit(zms_ftl, write_buffer, lpn)) {
			wb_read_pgs++;
			zms_ftl->read_wb_hits++;
			continue;
		}

		// check if this page in gc buffer
		if (zms_ftl->gc_agg_len > 0) {
			for (int i = 0; i < zms_ftl->gc_agg_len; i++) {
				if (zms_ftl->gc_agg_lpns[i] == lpn) {
					wb_read_pgs++;
					continue;
				}
			}
		}

		// L2P Search
		int sidx, cache_idx = -1;
		if (zms_ftl->zp.ns_type == SSD_TYPE_CONZONE_ZONED && L2P_HYBRID_MAP)
			sidx = 0;
		else
			sidx = NUM_MAP - 1;

		for (int i = sidx; i < NUM_MAP; i++) {
			uint64_t map_slpn = get_granularity_start_lpn(zms_ftl, lpn, MAP_GRAN(i));
			cache_idx = get_l2pcacheidx(zms_ftl, lpn);
			if (cache_idx != -1) {
				NVMEV_CONZONE_L2P_DEBUG_VERBOSE("L2P Cache HIT for lpn %lld, gran %d\n", lpn,
												MAP_GRAN(i));
				zms_ftl->l2p_hits++;
				l2p_access(zms_ftl, map_slpn, cache_idx);
				break;
			}
		}

		// L2P Miss
		if (cache_idx == -1) {
			nsecs_completed =
				nand_read(zms_ftl, to_read_lpns, 0, to_read_pgs, USER_IO, nsecs_read_begin);
			nsecs_latest = max(nsecs_latest, nsecs_completed);
			to_read_pgs = 0;
			nsecs_read_begin = map_read(zms_ftl, lpn, nsecs_read_begin);
		}

		struct ppa ppa = get_maptbl_ent(zms_ftl, lpn);
		if (!mapped_ppa(&ppa)) {
			zms_ftl->unmapped_read_cnt++;
		}

		to_read_lpns[to_read_pgs] = lpn;
		to_read_pgs++;
		// NVMEV_CONZONE_DEBUG("%s ppa ch %d lun %d pl %d blk %d pg
		// %d\n",__func__,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);
	}

	if (to_read_pgs) {
		nsecs_completed =
			nand_read(zms_ftl, to_read_lpns, 0, to_read_pgs, USER_IO, nsecs_read_begin);
		nsecs_latest = max(nsecs_latest, nsecs_completed);
	}

	if (wb_read_pgs) {
		nsecs_completed =
			ssd_advance_write_buffer(zms_ftl->ssd, nsecs_wb_read_begin, wb_read_pgs * spp->pgsz);
		nsecs_latest = max(nsecs_latest, nsecs_completed);
		NVMEV_CONZONE_RW_DEBUG("r [write buffer] read  pgs %lld lat %lld us\n", wb_read_pgs,
							   (nsecs_completed - nsecs_read_begin) / 1000);
	}

	if (interleave_pci_dma == false) {
		nsecs_completed = ssd_advance_pcie(zms_ftl->ssd, nsecs_latest, nr_lba * spp->secsz);
		nsecs_latest = max(nsecs_latest, nsecs_completed);
	}

	kfree(to_read_lpns);
	zms_ftl->host_rrequest_cnt++;
	ret->status = status;
	ret->nsecs_target = nsecs_latest;
	// NVMEV_CONZONE_DEBUG("%s slba 0x%llx (slpn 0x%llx) nr_lba 0x%llx zone_id %d lat %lld us\n",
	// __func__, slba,slpn,nr_lba, zid, (ret->nsecs_target-nsecs_start)/1000);
	return true;
}

bool zoned_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;
	struct zone_descriptor *zone_descs = zms_ftl->zone_descs;
	struct nvme_rw_command *cmd = &(req->cmd->rw);
	uint64_t slpn = lba_to_lpn((struct zns_ftl *)(&(*zms_ftl)), cmd->slba);

	// get zone from start_lba
	uint32_t zid = lpn_to_zone((struct zns_ftl *)(&(*zms_ftl)), slpn);

	if (zone_descs[zid].zrwav != 0)
		NVMEV_ERROR("NOT SUPPORT ZRWA!\n");
	return handle_write_request(zms_ftl, req, ret);
}

bool zoned_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;
	struct zone_descriptor *zone_descs = zms_ftl->zone_descs;
	struct nvme_rw_command *cmd = &(req->cmd->rw);

	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);

	uint32_t zid = lba_to_zone((struct zns_ftl *)(&(*zms_ftl)), slba);
	uint32_t status = NVME_SC_SUCCESS;

	if (zone_descs[zid].state == ZONE_STATE_OFFLINE) {
		status = NVME_SC_ZNS_ERR_OFFLINE;
		goto out;
	}
	// we do not support read across boundaries (OZCS_READ_ACROSS_ZONE)
	if (__check_boundary_error((struct zns_ftl *)(&(*zms_ftl)), slba, nr_lba) == false) {
		// return boundary error
		status = NVME_SC_ZNS_ERR_BOUNDARY;
		goto out;
	}
	// we do not support DULBE (the "__nvmev_admin_set_features" do nothing when receive
	// NVME_FEAT_ERR_RECOVERY)
	if (slba + nr_lba >
		(zone_to_slba((struct zns_ftl *)(&(*zms_ftl)), zid) + zone_descs[zid].zone_capacity)) {
		NVMEV_ERROR(
			"[%s] Trying to read invalid area!! zid %u read [%llu,%llu] zone "
			"[%llu,%llu] \n",
			__func__, zid, slba, slba + nr_lba, zone_to_slba((struct zns_ftl *)(&(*zms_ftl)), zid),
			(zone_to_slba((struct zns_ftl *)(&(*zms_ftl)), zid) + zone_descs[zid].zone_capacity));
		status = NVME_SC_INVALID_FIELD;
		goto out;
	}

	return handle_read_request(zms_ftl, req, ret);

out:
	ret->status = status;
	ret->nsecs_target = 0;
	return true;
}

void zone_reset(struct zms_ftl *zms_ftl, uint64_t zid, int sqid)
{
	uint64_t slpn = zone_to_slpn((struct zns_ftl *)(&(*zms_ftl)), zid);
	uint64_t elpn = slpn + zms_ftl->zp.pgs_per_zone - 1;
	struct buffer *write_buffer = __zms_wb_get(zms_ftl, slpn);
	uint64_t lpn;
	struct ppa ppa;
	size_t bufs_to_release = 0;
	struct zms_line *line;

	for (lpn = slpn; lpn <= elpn; lpn++) {
		ppa = get_maptbl_ent(zms_ftl, lpn);
		if (mapped_ppa(&ppa)) {
			mark_page_invalid(zms_ftl, &ppa);
			set_rmap_ent(zms_ftl, INVALID_LPN, &ppa);
			zms_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;

			line = get_line(zms_ftl, &ppa);
			if (line->ipc + line->rpc == line->pgs_per_line) {
				erase_line(zms_ftl, line, USER_IO);
			}
		} else {
			if (write_buffer && __zms_wb_check(zms_ftl, write_buffer, slpn) == SUCCESS &&
				__zms_wb_hit(zms_ftl, write_buffer, lpn)) {
				bufs_to_release = (elpn - lpn + 1) * zms_ftl->ssd->sp.pgsz;
			}
			break;
		}
	}

	if (write_buffer && bufs_to_release && !write_buffer->flushing) {
		NVMEV_CONZONE_GC_DEBUG("Evict write buffer %ld KiB\n", BYTE_TO_KB(bufs_to_release));
		write_buffer->flush_data -= bufs_to_release;
		uint64_t flush_pgs = (elpn - lpn + 1);
		int last_write = -1;
		for (int i = 0; i < write_buffer->pgs; i++) {
			if (write_buffer->lpns[i] == lpn) {
				write_buffer->lpns[i] = INVALID_LPN;
				if (lpn < elpn)
					lpn++;
				if (last_write == -1)
					last_write = i;
			} else if (last_write >= 0 && last_write < i) {
				write_buffer->lpns[last_write] = write_buffer->lpns[i];
				last_write++;
			}
		}
		write_buffer->pgs -= flush_pgs;
		NVMEV_ASSERT(last_write == write_buffer->pgs);
		write_buffer->zid = -1;
		write_buffer->sqid = -1;
		write_buffer->flushing = true;
		schedule_internal_operation(sqid, 0, write_buffer, bufs_to_release);
	}

	zms_ftl->zone_agg_pgs[zid] = 0;
	zms_ftl->zone_reset_cnt++;

	NVMEV_CONZONE_GC_DEBUG(
		"Zone %lld [%lld-%lld] Reset. pSLC lines: %d/%d/%d/%d, normal lines %d/%d/%d/%d "
		"(free/full/victim/all)\n",
		zid, slpn, elpn, zms_ftl->lm.pslc_free_line_cnt, zms_ftl->lm.pslc_full_line_cnt,
		zms_ftl->lm.pslc_victim_line_cnt, zms_ftl->lm.pslc_tt_lines, zms_ftl->lm.free_line_cnt,
		zms_ftl->lm.full_line_cnt, zms_ftl->lm.victim_line_cnt, zms_ftl->lm.tt_lines);
}

bool block_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;
	return handle_write_request(zms_ftl, req, ret);
}

bool block_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zms_ftl *zms_ftl = (struct zms_ftl *)ns->ftls;
	return handle_read_request(zms_ftl, req, ret);
}
#endif