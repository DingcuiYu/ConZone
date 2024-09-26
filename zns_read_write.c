// SPDX-License-Identifier: GPL-2.0-only

#include "nvmev.h"
#include "ssd.h"
#include "zns_ftl.h"

static inline uint32_t __nr_lbas_from_rw_cmd(struct nvme_rw_command *cmd)
{
	return cmd->length + 1;
}

static bool __check_boundary_error(struct zns_ftl *zns_ftl, uint64_t slba, uint32_t nr_lba)
{
	return lba_to_zone(zns_ftl, slba) == lba_to_zone(zns_ftl, slba + nr_lba - 1);
}

static void __increase_write_ptr(struct zns_ftl *zns_ftl, uint32_t zid, uint32_t nr_lba)
{
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	uint64_t cur_write_ptr = zone_descs[zid].wp;
	uint64_t zone_capacity = zone_descs[zid].zone_capacity;

	cur_write_ptr += nr_lba;

	zone_descs[zid].wp = cur_write_ptr;

	if (cur_write_ptr == (zone_to_slba(zns_ftl, zid) + zone_capacity)) {
		//change state to ZSF
		release_zone_resource(zns_ftl, OPEN_ZONE);
		release_zone_resource(zns_ftl, ACTIVE_ZONE);

		if (zone_descs[zid].zrwav)
			ASSERT(0);

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
		.g = {
			.lun = lun,
			.ch = channel,
			.pg = off % spp->pgs_per_oneshotpg,
		},
	};

	return ppa;
}

static bool check_pSLC(struct zns_ftl *zns_ftl,struct ppa *ppa)
{
	return (ppa->zms.blk < zns_ftl->pslc_blks);
}

static bool check_resident(struct zns_ftl *zns_ftl,int gran)
{
	if(L2P_HYBRID_MAP_RESIDENT)
		return gran!=PAGE_MAP;
	return 0;
}

static bool ppa_same(struct ppa pa,struct ppa pb)
{
	pa.g.rsv = 0;
	pb.g.rsv = 0;
	return pa.ppa==pb.ppa;
}

//for write
static inline bool zms_last_pg_in_wordline(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	int pgs_per_oneshotpg = check_pSLC(zns_ftl,ppa)?spp->pslc_pgs_per_oneshotpg:spp->pgs_per_oneshotpg;
	return (ppa->g.pg % pgs_per_oneshotpg) == (pgs_per_oneshotpg - 1);
}

static inline bool zms_last_pg_in_flashpg(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	int pgs_per_flashpg = check_pSLC(zns_ftl,ppa)?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;
	return (ppa->g.pg % pgs_per_flashpg) == (pgs_per_flashpg - 1);
}

static inline void check_addr(int a, int max)
{
	NVMEV_ASSERT(a >= 0 && a < max);
}


static uint64_t ppa2pgidx(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	uint64_t pgidx;

	NVMEV_DEBUG_VERBOSE("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__,
			ppa->zms.ch, ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);

	pgidx = ppa->zms.ch * spp->pgs_per_ch + ppa->zms.lun * spp->pgs_per_lun +
		ppa->zms.pl * spp->pgs_per_pl + ppa->zms.blk * spp->pgs_per_blk + ppa->zms.pg;

	if(pgidx >= zns_ftl->pslc_ttpgs)
	{
		NVMEV_ERROR("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__,
			ppa->zms.ch, ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);
		NVMEV_ERROR("Error pgidx %lld\n",pgidx);
		return 0;
	}

	return pgidx;
}

static uint64_t ppa2wpidx(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	uint64_t wpidx;
	int pgs_per_flashpg = check_pSLC(zns_ftl,ppa)?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;

	wpidx = ppa->zms.blk * spp->pgs_per_blk;
	//full run
	wpidx += (ppa->zms.pg/pgs_per_flashpg)*(pgs_per_flashpg*spp->nchs*spp->luns_per_ch);

	//chip offset
	wpidx += (ppa->zms.lun*spp->nchs + ppa->zms.ch)*pgs_per_flashpg;

	//pg offset
	wpidx += ppa->zms.pg%pgs_per_flashpg;

	if(wpidx >= zns_ftl->pslc_ttpgs)
	{
		NVMEV_ERROR("%s: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n", __func__,
			ppa->zms.ch, ppa->zms.lun, ppa->zms.pl, ppa->zms.blk, ppa->zms.pg);
		NVMEV_ERROR("Error wpidx %lld\n",wpidx);
		return 0;
	}

	return wpidx;
}

static inline uint64_t get_rmap_ent(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	uint64_t pgidx = ppa2wpidx(zns_ftl, ppa);
	if(pgidx==0 && ppa->ppa!=0)
	{
		NVMEV_ERROR("%s error\n",__func__);
	}
	return zns_ftl->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct zns_ftl *zns_ftl, uint64_t lpn, struct ppa *ppa)
{
	uint64_t pgidx = ppa2wpidx(zns_ftl, ppa);
	if(pgidx==0 && ppa->ppa!=0)
	{
		NVMEV_ERROR("%s error\n",__func__);
		return;
	}
	zns_ftl->rmap[pgidx] = lpn;
}

static inline void consume_write_credit(struct zns_ftl *zns_ftl,bool pSLC)
{
	if(pSLC) zns_ftl->pslc_wfc.write_credits--;
	else zns_ftl->wfc.write_credits--;
}

static void foreground_gc(struct zns_ftl *zns_ftl,bool pSLC);

static inline void check_and_refill_write_credit(struct zns_ftl *zns_ftl,bool pSLC)
{
	struct zms_write_flow_control *wfc = pSLC?&(zns_ftl->pslc_wfc) :&(zns_ftl->wfc);
	//NVMEV_ZMS_GC_DEBUG("%s write credit:%d\n",pSLC?"pSLC":"normal",wfc->write_credits);
	if (wfc->write_credits <= 0) {
		foreground_gc(zns_ftl,pSLC);

		wfc->write_credits += wfc->credits_to_refill;
	}
}

//only for pSLC area now
static void zms_advance_write_pointer(struct zns_ftl *zns_ftl, uint32_t io_type, bool pSLC)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zms_line_mgmt *lm;
	if(io_type==UNALIGNED_IO) lm = &zns_ftl->pslc_lm;
	else lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;

	struct zms_write_pointer *wpp = zms_get_wp(zns_ftl, io_type, pSLC);
	if(!wpp->curline) return;
	
	int pgs_per_flashpg = wpp->loc?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;

	NVMEV_DEBUG_VERBOSE("current wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d\n",
			wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg);

	check_addr(wpp->pg, spp->pgs_per_blk);
	wpp->pg++;
	if ((wpp->pg % pgs_per_flashpg) != 0)
		goto out;

	wpp->pg -= pgs_per_flashpg;
	check_addr(wpp->ch, spp->nchs);
	wpp->ch++;
	if (wpp->ch != spp->nchs)
		goto out;

	wpp->ch = 0;
	check_addr(wpp->lun, spp->luns_per_ch);
	wpp->lun++;
	/* in this case, we should go to next lun */
	if (wpp->lun != spp->luns_per_ch)
		goto out;

	wpp->lun = 0;
	/* go to next wordline in the block */

	wpp->pg += pgs_per_flashpg;
	if (wpp->pg != spp->pgs_per_blk)
		goto out;

	wpp->pg = 0;
	/* move current line to {victim,full} line list */
	if (wpp->curline->vpc == spp->pgs_per_line) {
		/* all pgs are still valid, move to full line list */
		NVMEV_ASSERT(wpp->curline->ipc == 0);
		list_add_tail(&wpp->curline->entry, &lm->full_line_list);
		lm->full_line_cnt++;
		NVMEV_DEBUG_VERBOSE("wpp: move line to full_line_list\n");
	} else {
		NVMEV_DEBUG_VERBOSE("wpp: line is moved to victim list\n");
		NVMEV_ASSERT(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
		/* there must be some invalid pages in this line */
		NVMEV_ASSERT(wpp->curline->ipc > 0);
		pqueue_insert(lm->victim_line_pq, wpp->curline);
		lm->victim_line_cnt++;
	}
	/* current line is used up, pick another empty line */
	check_addr(wpp->blk, spp->blks_per_pl);
	check_addr(wpp->blk, lm->tt_lines);
	wpp->curline = get_next_free_line(zns_ftl,pSLC);
	if(!wpp->curline)
	{
		return;
	}

	NVMEV_DEBUG_VERBOSE("wpp: got new clean line %d\n", wpp->curline->id);

	wpp->blk = wpp->curline->id;
	check_addr(wpp->blk, spp->blks_per_pl);
	check_addr(wpp->blk, lm->tt_lines);

	/* make sure we are starting from page 0 in the super block */
	NVMEV_ASSERT(wpp->pg == 0);
	NVMEV_ASSERT(wpp->lun == 0);
	NVMEV_ASSERT(wpp->ch == 0);
	/* TODO: assume # of pl_per_lun is 1, fix later */
	NVMEV_ASSERT(wpp->pl == 0);
out:
//NVMEV_DEBUG_VERBOSE
	NVMEV_ZMS_GC_DEBUG_VERBOSE("advanced %s wpp: ch:%d, lun:%d, pl:%d, blk:%d, pg:%d (curline %d ipc %d vpc %d)\n",
			wpp->loc?"pslc":"normal",wpp->ch, wpp->lun, wpp->pl, wpp->blk, wpp->pg, wpp->curline->id,wpp->curline->ipc,wpp->curline->vpc);
}

static struct ppa get_new_page(struct zns_ftl *zns_ftl, uint32_t io_type, bool pSLC)
{
	struct ppa ppa;
	struct zms_write_pointer *wp = zms_get_wp(zns_ftl, io_type,pSLC);
	
	ppa.ppa = 0;
	if(!wp->curline)
	{
		ppa.ppa = UNMAPPED_PPA;
		return ppa;
	}

	ppa.zms.map = PAGE_MAP; //default
	ppa.zms.ch = wp->ch;
	ppa.zms.lun = wp->lun;
	ppa.zms.pl = wp->pl;
	ppa.zms.blk = wp->loc?wp->blk:wp->blk+zns_ftl->pslc_blks;
	ppa.zms.pg = wp->pg;

	NVMEV_ASSERT(ppa.g.pl == 0);
	return ppa;
}

static inline bool mapped_ppa(struct ppa *ppa)
{
	return !(ppa->ppa == UNMAPPED_PPA);
}

static inline bool valid_lpn(struct zns_ftl *zns_ftl, uint64_t lpn)
{
	return (lpn < zns_ftl->ssd->sp.tt_pgs);
}

static inline void set_maptbl_ent(struct zns_ftl *zns_ftl, uint64_t lpn, struct ppa *ppa)
{
	NVMEV_ASSERT(lpn < zns_ftl->ssd->sp.tt_pgs);
	zns_ftl->maptbl[lpn] = *ppa;
}

static inline struct ppa get_maptbl_ent(struct zns_ftl *zns_ftl, uint64_t lpn)
{
	return zns_ftl->maptbl[lpn];
}

static inline struct zms_line *get_line(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	bool pSLC = check_pSLC(zns_ftl,ppa);
	if(pSLC) return &(zns_ftl->pslc_lm.lines[ppa->zms.blk]);
	return &(zns_ftl->lm.lines[ppa->zms.blk-zns_ftl->pslc_blks]);
}

static void mark_page_valid(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	struct zms_line *line;

	//NVMEV_ZMS_DEBUG("mark ppa ch %d lun %d pl %d blk %d pg %d valid\n",ppa->zms.ch,ppa->zms.lun,ppa->zms.pl,ppa->zms.blk,ppa->zms.pg);
	
	/* update page status */
	pg = get_pg(zns_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_FREE);
	pg->status = PG_VALID;

	/* update corresponding block status */
	blk = get_blk(zns_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->vpc >= 0 && blk->vpc < spp->pgs_per_blk);
	blk->vpc++;

	/* update corresponding line status */
	line = get_line(zns_ftl, ppa);
	NVMEV_ASSERT(line->vpc >= 0 && line->vpc < spp->pgs_per_line);
	line->vpc++;
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	bool pSLC = check_pSLC(zns_ftl,ppa);
	struct zms_line_mgmt *lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct nand_block *blk = NULL;
	struct nand_page *pg = NULL;
	bool was_full_line = false;
	struct zms_line *line;

	/* update corresponding page status */
	pg = get_pg(zns_ftl->ssd, ppa);
	NVMEV_ASSERT(pg->status == PG_VALID);

	//NVMEV_ZMS_DEBUG("mark ppa ch %d lun %d pl %d blk %d pg %d invalid\n",ppa->zms.ch,ppa->zms.lun,ppa->zms.pl,ppa->zms.blk,ppa->zms.pg);

	pg->status = PG_INVALID;

	/* update corresponding block status */
	blk = get_blk(zns_ftl->ssd, ppa);
	NVMEV_ASSERT(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
	blk->ipc++;
	NVMEV_ASSERT(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
	blk->vpc--;

	/* update corresponding line status */
	line = get_line(zns_ftl, ppa);
	NVMEV_ASSERT(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
	if (line->vpc == spp->pgs_per_line) {
		NVMEV_ASSERT(line->ipc == 0);
		was_full_line = true;
	}
	line->ipc++;
	NVMEV_ASSERT(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
	/* Adjust the position of the victime line in the pq under over-writes */
	if (line->pos) {
		/* Note that line->vpc will be updated by this call */
		pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
	} else {
		line->vpc--;
	}

	if (was_full_line) {
		/* move line: "full" -> "victim" */
		list_del_init(&line->entry);
		lm->full_line_cnt--;
		pqueue_insert(lm->victim_line_pq, line);
		lm->victim_line_cnt++;
	}
}

static void mark_PA_invalid(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	mark_page_invalid(zns_ftl,ppa);
	if(check_pSLC(zns_ftl,ppa)) 
		set_rmap_ent(zns_ftl,INVALID_LPN,ppa);
	return;
}

static void mark_block_free(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct nand_block *blk = get_blk(zns_ftl->ssd, ppa);
	struct nand_page *pg = NULL;
	int i;

	for (i = 0; i < spp->pgs_per_blk; i++) {
		/* reset page status */
		pg = &blk->pg[i];
		NVMEV_ASSERT(pg->nsecs == spp->secs_per_pg);
		pg->status = PG_FREE;
		//NVMEV_ZMS_GC_DEBUG_VERBOSE("mark ppa(ch %d lun %d pl %d blk %d pg %d) free\n",ppa->zms.ch,ppa->zms.lun,ppa->zms.pl,ppa->zms.blk,i);
	}

	/* reset block status */
	NVMEV_ASSERT(blk->npgs == spp->pgs_per_blk);
	blk->ipc = 0;
	blk->vpc = 0;
	blk->erase_cnt++;
}

static void nextpage(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	bool pSLC = check_pSLC(zns_ftl,ppa);
	int pgs_per_flashpg = pSLC?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;

	check_addr(ppa->zms.pg, spp->pgs_per_blk);
	ppa->zms.pg++;
	if ((ppa->zms.pg % pgs_per_flashpg) != 0)
		goto out;

	ppa->zms.pg -= pgs_per_flashpg;
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

	ppa->zms.pg += pgs_per_flashpg;
	if (ppa->zms.pg != spp->pgs_per_blk)
		goto out;

	ppa->ppa = UNMAPPED_PPA;
	NVMEV_ERROR("%s: Too many pages!\n",__func__);
out:
	return;
}

static int pgs_per_AU(struct zns_ftl *zns_ftl, int au)
{
	int pgs = 1;
	switch (au)
	{
	case PAGE_MAP:
		pgs = 1;
		break;
	case CHUNK_MAP:
		pgs = zns_ftl->zp.pgs_per_chunk;
		break;
	case ZONE_MAP:
		pgs = zns_ftl->zp.pgs_per_zone;
		break;
	default:
		NVMEV_ERROR("%s Invalid gran:%d\n",__func__,au);
		NVMEV_ASSERT(0);
		break;
	}
	return pgs;
}

uint64_t AU_to_slpn(struct zns_ftl *zns_ftl, int au, uint64_t lpn)
{
	int pgs = pgs_per_AU(zns_ftl,au);
	uint64_t au_slpn = lpn/pgs*pgs; //slpn of this allocation unit
	return au_slpn;
}

static void set_map_gran(struct zns_ftl *zns_ftl, int gran, uint64_t lpn)
{
	uint64_t slpn = AU_to_slpn(zns_ftl,gran,lpn);
	uint64_t rla;
	int idx;
	int res = check_resident(zns_ftl,gran);
	struct ppa ppa = get_maptbl_ent(zns_ftl,slpn);

	idx = l2p_search(zns_ftl,slpn,&rla);

	if(idx!=-1)
	{
		struct l2pcache* cache = &zns_ftl->l2pcache;
		uint64_t slot = slpn%cache->num_slots;
		cache->mapping[slot][idx].gran = gran;
		cache->mapping[slot][idx].resident = res;
	}
	else
	{
		l2p_insert(zns_ftl,slpn,gran,res);
	}

	ppa.zms.map = gran;
	set_maptbl_ent(zns_ftl,slpn,&ppa);
	NVMEV_ZMS_L2P_DEBUG("set %s map [slpn %lld] \n",gran==PAGE_MAP?"page":(gran==CHUNK_MAP?"chunk":"zone"),slpn);
}

static int __zms_map_gran(struct zns_ftl *zns_ftl,bool pSLC)
{
	if(zns_ftl->zp.zone_type == SSD_TYPE_ZNS_CONV) return PAGE_MAP;
	return pSLC?PAGE_MAP:ZONE_MAP;
}

static int recover_map(struct zns_ftl *zns_ftl, int gran, uint64_t lpn)
{
	uint64_t au_slpn = AU_to_slpn(zns_ftl,gran,lpn); //slpn of this allocation unit
	uint64_t slpn = lpn>au_slpn?lpn-1:au_slpn;
	struct ppa ppa = get_maptbl_ent(zns_ftl, slpn);
	if(lpn==au_slpn || !mapped_ppa(&ppa))
	{
		zns_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;
		if(lpn!=au_slpn) NVMEV_ZMS_L2P_DEBUG_VERBOSE("map lpn %lld -> unmapped\n",lpn);
		return FAILURE;
	}
	
	nextpage(zns_ftl,&ppa);
	if(!mapped_ppa(&ppa))
	{
		return FAILURE;
	}

	set_maptbl_ent(zns_ftl, lpn, &ppa);
	NVMEV_ZMS_L2P_DEBUG_VERBOSE("map lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n",lpn,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);

	if(ZONED_DEVICE && L2P_HYBRID_MAP)
	{
		if((lpn-au_slpn+1)%zns_ftl->zp.pgs_per_zone==0)
		{
			set_map_gran(zns_ftl,ZONE_MAP,lpn);
		}
		else if((lpn-au_slpn+1)%zns_ftl->zp.pgs_per_chunk==0)
		{
			set_map_gran(zns_ftl,CHUNK_MAP,lpn);
		}
	}
	return SUCCESS;
}

static void check_or_new_map(struct zns_ftl *zns_ftl, bool loc, bool unaligned, uint64_t lpn)
{
	int au = __zms_map_gran(zns_ftl,loc);
	struct ppa ppa = get_maptbl_ent(zns_ftl,lpn);
	if(mapped_ppa(&ppa))
	{
		if(!loc && zns_ftl->zp.zone_type == SSD_TYPE_ZNS)
		{
			NVMEV_ERROR("update write for seq.zone?\n");
			//NVMEV_ASSERT(0);
		}
	}
	else
	{
		if(unaligned || (recover_map(zns_ftl,au,lpn)!=SUCCESS))
		{
			int pgs = pgs_per_AU(zns_ftl,au);
			int io_type = unaligned?UNALIGNED_IO:USER_IO;
			struct ppa ppa = get_new_page(zns_ftl,io_type,loc);
			int i;
			uint64_t slpn;

			slpn = lpn - lpn%pgs; //logical zone / logical page's start lpn

			if(au==ZONE_MAP) pgs = zns_ftl->unaligned_slpn; //avoid the allocation of unaligned pages

			//NVMEV_ZMS_L2P_DEBUG_VERBOSE("Tring allocate %d pgs in %s area, current ppa ch %d lun %d blk %d pg %d\n",pgs,loc==LOC_PSLC?"pSLC":"Normal",ppa.zms.ch,ppa.zms.lun,ppa.zms.blk,ppa.zms.pg);
			for(i = 0; i < pgs; i++)
			{
				if(!mapped_ppa(&ppa)) break;
				
				consume_write_credit(zns_ftl,loc);
				if(loc) //We do not need to do GC with seq zone
				{
					check_and_refill_write_credit(zns_ftl,loc);
				}

				if(slpn+i <= lpn)
				{
					set_maptbl_ent(zns_ftl, slpn+i, &ppa);
					NVMEV_ZMS_L2P_DEBUG_VERBOSE("%s I/O: %s map lpn %lld -> ch %d lun %d pl %d blk %d pg %d\n",io_type==USER_IO?"user":"unaligned",(ppa.zms.map==PAGE_MAP?"page":(ppa.zms.map==CHUNK_MAP?"chunk":"zone")),slpn+i,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);

					if(ZONED_DEVICE && L2P_HYBRID_MAP && (unaligned || au==ZONE_MAP))
					{
						if(((i+1)%zns_ftl->zp.pgs_per_zone)==0)
						{
							set_map_gran(zns_ftl,ZONE_MAP,slpn+i); //if there are unaligned I/O, we can not get zone map
						}
						else if(((slpn+i+1)%zns_ftl->zp.pgs_per_chunk)==0)
						{
							set_map_gran(zns_ftl,CHUNK_MAP,slpn+i);
						}
					}
				}
				else
				{
					//NVMEV_ZMS_L2P_DEBUG_VERBOSE("%s map [reserved page] ch %d lun %d pl %d blk %d pg %d --> lpn %lld\n",(ppa.zms.map==PAGE_MAP?"page":(ppa.zms.map==CHUNK_MAP?"chunk":"zone")),ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg,slpn+i);
				}

				if(check_pSLC(zns_ftl,&ppa)) 
					set_rmap_ent(zns_ftl, slpn+i, &ppa);
				mark_page_valid(zns_ftl, &ppa);
				zms_advance_write_pointer(zns_ftl, io_type, loc);
				ppa = get_new_page(zns_ftl,io_type,loc);
			}
		}
	}
}

static inline bool should_gc_high(struct zns_ftl *zns_ftl,bool pSLC)
{
	if(pSLC) return zns_ftl->pslc_lm.free_line_cnt <= zns_ftl->zp.gc_thres_lines_high;
	return zns_ftl->lm.free_line_cnt <= zns_ftl->zp.gc_thres_lines_high;
}

static struct zms_line *select_victim_line(struct zns_ftl *zns_ftl, bool force, bool pSLC)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zms_line_mgmt *lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct zms_line *victim_line = NULL;

	victim_line = pqueue_peek(lm->victim_line_pq);
	if (!victim_line) {
		return NULL;
	}

	if (!force && (victim_line->vpc > (spp->pgs_per_line / 8))) {
		return NULL;
	}

	pqueue_pop(lm->victim_line_pq);
	victim_line->pos = 0;
	lm->victim_line_cnt--;

	/* victim_line is a danggling node now */
	return victim_line;
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct zns_ftl *zns_ftl, struct ppa *old_ppa,bool pSLC)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct znsparams *zpp = &zns_ftl->zp;
	struct ppa new_ppa;
	if(!check_pSLC(zns_ftl,old_ppa) || !pSLC)
	{
		NVMEV_ERROR("Error: GC in zone area...\n");
		return 0;
	}

	uint64_t lpn = get_rmap_ent(zns_ftl, old_ppa);

	if(!valid_lpn(zns_ftl, lpn))
	{
		NVMEV_ERROR("Error: invalid lpn (%lld) for ppa ch %d lun %d blk %d pg %d\n",lpn,old_ppa->zms.ch,old_ppa->zms.lun,old_ppa->zms.blk,old_ppa->zms.pg);
		return 0;
	}
	new_ppa = get_new_page(zns_ftl, GC_IO, pSLC);
	if(!mapped_ppa(&new_ppa))
	{
		NVMEV_ERROR("No Free Pages for GC!!\n");
		return 0;
	}
	/* update maptbl */
	set_maptbl_ent(zns_ftl, lpn, &new_ppa);
	NVMEV_ZMS_GC_DEBUG_VERBOSE("%s GC map lpn %lld --> ch %d lun %d pl %d blk %d pg %d\n",pSLC?"pSLC":"normal",lpn,new_ppa.zms.ch,new_ppa.zms.lun,new_ppa.zms.pl,new_ppa.zms.blk,new_ppa.zms.pg);
	/* update rmap */
	if(check_pSLC(zns_ftl,&new_ppa)) 
		set_rmap_ent(zns_ftl, lpn, &new_ppa);

	mark_page_valid(zns_ftl, &new_ppa);

	/* need to advance the write pointer here */
	zms_advance_write_pointer(zns_ftl, GC_IO, pSLC);


	if (zpp->enable_gc_delay) {
		struct nand_cmd gcw = {
			.type = GC_IO,
			.cmd = NAND_NOP,
			.stime = 0,
			.interleave_pci_dma = false,
			.ppa = &new_ppa,
		};
		//TODO: maybe we need manage gc write buffer here?
		if (zms_last_pg_in_wordline(zns_ftl, &new_ppa)) {
			gcw.cmd = pSLC?NAND_PSLC_WRITE:NAND_WRITE;
			gcw.xfer_size = (pSLC?spp->pslc_pgs_per_oneshotpg:spp->pgs_per_oneshotpg) * spp->pgsz;
		}

		ssd_advance_nand(zns_ftl->ssd, &gcw);
	}

	return 0;
}

/* here ppa identifies the block we want to clean */
static void clean_one_flashpg(struct zns_ftl *zns_ftl, struct ppa *ppa,bool pSLC)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct znsparams *zpp = &zns_ftl->zp;
	struct nand_page *pg_iter = NULL;
	int cnt = 0, i = 0;
	uint64_t completed_time = 0;
	struct ppa ppa_copy = *ppa;
	int pgs_per_flshpg = pSLC?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;

	for (i = 0; i < pgs_per_flshpg; i++) {
		pg_iter = get_pg(zns_ftl->ssd, &ppa_copy);
		/* there shouldn't be any free page in victim blocks */
		NVMEV_ASSERT(pg_iter->status != PG_FREE);
		if (pg_iter->status == PG_VALID)
			cnt++;

		ppa_copy.g.pg++;
	}

	ppa_copy = *ppa;

	if (cnt <= 0)
		return;

	if (zpp->enable_gc_delay && cnt>0) {
		struct nand_cmd gcr = {
			.type = GC_IO,
			.cmd = pSLC?NAND_PSLC_READ:NAND_READ,
			.stime = 0,
			.xfer_size = spp->pgsz * cnt,
			.interleave_pci_dma = false,
			.ppa = &ppa_copy,
		};
		completed_time = ssd_advance_nand(zns_ftl->ssd, &gcr);
	}

	for (i = 0; i < pgs_per_flshpg; i++) {
		pg_iter = get_pg(zns_ftl->ssd, &ppa_copy);

		/* there shouldn't be any free page in victim blocks */
		if (pg_iter->status == PG_VALID) {
			/* delay the maptbl update until "write" happens */
			zns_ftl->device_w_pgs++;
			gc_write_page(zns_ftl, &ppa_copy, pSLC);
		}

		ppa_copy.g.pg++;
	}
}

static void mark_line_free(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct zms_line_mgmt *lm = check_pSLC(zns_ftl,ppa)?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct zms_line *line = get_line(zns_ftl, ppa);
	line->ipc = 0;
	line->vpc = 0;
	/* move this line to free line list */
	list_add_tail(&line->entry, &lm->free_line_list);
	lm->free_line_cnt++;
}


static int do_gc(struct zns_ftl *zns_ftl, bool force, bool pSLC)
{
	struct zms_line *victim_line = NULL;
	struct zms_line_mgmt* lm = pSLC?&zns_ftl->pslc_lm:&zns_ftl->lm;
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zms_write_flow_control* wfc = pSLC?&zns_ftl->pslc_wfc:&zns_ftl->wfc;
	struct ppa ppa;
	int flashpg;
	int flashpgs_per_blk = pSLC?spp->pslc_flashpgs_per_blk:spp->flashpgs_per_blk;
	int pgs_per_flashpg = pSLC?spp->pslc_pgs_per_flashpg:spp->pgs_per_flashpg;

	victim_line = select_victim_line(zns_ftl, force,pSLC);
	if (!victim_line) {
		return -1;
	}

	ppa.g.blk = pSLC?victim_line->id:victim_line->id+zns_ftl->pslc_blks;
	NVMEV_ZMS_GC_DEBUG("GC-ing %s line:%d(ipc=%d,vpc=%d), victim= %d ,full= %d ,free= %d, all = %d\n",pSLC?"pslc":"normal",victim_line->id,victim_line->ipc, victim_line->vpc, lm->victim_line_cnt,lm->full_line_cnt, lm->free_line_cnt, lm->tt_lines);

	wfc->credits_to_refill = victim_line->ipc;

	/* copy back valid data */
	for (flashpg = 0; flashpg < flashpgs_per_blk; flashpg++) {
		int ch, lun;

		ppa.g.pg = flashpg * pgs_per_flashpg;
		for (ch = 0; ch < spp->nchs; ch++) {
			for (lun = 0; lun < spp->luns_per_ch; lun++) {
				struct nand_lun *lunp;

				ppa.g.ch = ch;
				ppa.g.lun = lun;
				ppa.g.pl = 0;
				lunp = get_lun(zns_ftl->ssd, &ppa);
				clean_one_flashpg(zns_ftl, &ppa,pSLC);

				if (flashpg == (flashpgs_per_blk - 1)) {
					struct znsparams *zpp = &zns_ftl->zp;

					mark_block_free(zns_ftl, &ppa);

					if (zpp->enable_gc_delay) {
						struct nand_cmd gce = {
							.type = GC_IO,
							.cmd = NAND_ERASE,
							.stime = 0,
							.interleave_pci_dma = false,
							.ppa = &ppa,
						};
						ssd_advance_nand(zns_ftl->ssd, &gce);
					}

					lunp->gc_endtime = lunp->next_lun_avail_time;
				}
			}
		}
	}

	/* update line status */
	mark_line_free(zns_ftl, &ppa);

	return 0;
}

static void foreground_gc(struct zns_ftl *zns_ftl,bool pSLC)
{
	if (should_gc_high(zns_ftl,pSLC)) {
		NVMEV_ZMS_GC_DEBUG("should_gc_high passed");
		/* perform GC here until !should_gc(zns_ftl) */
		do_gc(zns_ftl, true, pSLC);
	}
}


static uint64_t zms_seqzone_flush(struct zns_ftl *zns_ftl,struct buffer *write_buffer,uint64_t nsecs_start)
{
	if(write_buffer->flushing) return nsecs_start;
	struct ppa ppa,_ppa;
	uint64_t lpn,_lpn;
	uint64_t slpn = write_buffer->lpn;
	uint64_t elpn = write_buffer->lpn + write_buffer->pgs;
	uint64_t pgs = 0;
	uint64_t pg_off = 0;
	uint64_t lpn_off = 0;
	uint64_t nsecs_latest = nsecs_start;
	struct ssdparams *spp=&zns_ftl->ssd->sp;
	bool loc = LOC_NORMAL;
	bool unaligned = false;
	for(lpn = slpn; lpn < elpn; lpn+=pgs )
	{
		uint64_t nsecs_flush_begin = nsecs_start;

		lpn_off = lpn - zone_to_slpn(zns_ftl,lpn_to_zone(zns_ftl,lpn));
		pg_off = lpn_off % spp->pgs_per_oneshotpg;
		pgs = min(elpn - lpn, (uint64_t)(spp->pgs_per_oneshotpg - pg_off));

		if(lpn_off < zns_ftl->unaligned_slpn  && pg_off + pgs == spp->pgs_per_oneshotpg)
		{
			loc = LOC_NORMAL;
			unaligned = false;
			int au = __zms_map_gran(zns_ftl,LOC_NORMAL);
			if(pg_off > 0)
			{
				//data in write buffer are unaligned, should read from pSLC first
				NVMEV_ASSERT(lpn-pg_off>=0);
				bool _loc;
				for( _lpn = lpn-pg_off; _lpn < lpn; _lpn++)
				{
					_ppa = get_maptbl_ent(zns_ftl, _lpn);
					if (mapped_ppa(&_ppa)) {
						zns_ftl->device_w_pgs++; //read and write back to normal area
						/* update old page information first */
						_loc = check_pSLC(zns_ftl, &_ppa);
						if(!_loc)
						{
							NVMEV_ERROR("%s early flushed data is not in pSLC??\n",__func__);
						}
						mark_PA_invalid(zns_ftl,&_ppa);
						recover_map(zns_ftl,au,_lpn);
						struct nand_cmd rcmd = {
							.type = USER_IO,
							.cmd = _loc?NAND_PSLC_READ:NAND_READ,
							.stime = nsecs_start,
							.xfer_size = spp->pgsz,
							.interleave_pci_dma = false,
							.ppa = &_ppa,
						};
						uint64_t r_nsecs_completed = ssd_advance_nand(zns_ftl->ssd, &rcmd);
						nsecs_flush_begin = max(r_nsecs_completed, nsecs_flush_begin);
					}
					else
					{
						NVMEV_ERROR("%s unaligned write?? (maybe for conventional zone??)\n",__func__);
					}
				}
				NVMEV_ZMS_RW_DEBUG_VERBOSE("%s read slpn %lld pgs %lld from pSLC lat %lld us\n",
					__func__, lpn-pg_off,pg_off,(nsecs_flush_begin - nsecs_start)/1000);
			}

		}
		else
		{
			loc = LOC_PSLC;
			if(lpn_off < zns_ftl->unaligned_slpn) unaligned = false;
			else unaligned = true;
		}

		for(_lpn = lpn; _lpn < lpn+pgs; _lpn++)
		{
			zns_ftl->device_w_pgs++;
			zns_ftl->host_w_pgs++;
			check_or_new_map(zns_ftl,loc,unaligned,_lpn);
			_ppa = get_maptbl_ent(zns_ftl,_lpn);

			if(zms_last_pg_in_wordline(zns_ftl,&_ppa))
			{
				struct nand_cmd swr = {
					.type = USER_IO,
					.cmd = loc?NAND_PSLC_WRITE:NAND_WRITE, //early flush to slc
					.stime = nsecs_flush_begin,
					.xfer_size = (loc?spp->pslc_pgs_per_oneshotpg:spp->pgs_per_oneshotpg)*spp->pgsz,
					.interleave_pci_dma = false,
					.ppa = &_ppa,
				};
				uint64_t flush_nsecs_completed = ssd_advance_nand(zns_ftl->ssd, &swr);
				nsecs_latest = max(flush_nsecs_completed, nsecs_latest);
			}
		}

		NVMEV_ZMS_RW_DEBUG_VERBOSE("%s %s Flush slpn %lld pgs %lld zone_id %d lat %lld us\n",__func__,unaligned?"Unaligned":(loc?"Early":"Normal"),lpn, pgs, write_buffer->zid,(nsecs_latest-nsecs_start)/1000);
	}

	NVMEV_ZMS_RW_DEBUG_VERBOSE("%s [END] flush slpn %lld pgs %lld zone_id %d lat %lld us\n",
					__func__,write_buffer->lpn, write_buffer->pgs, write_buffer->zid,(nsecs_latest-nsecs_start)/1000);

	size_t bufs_to_release = write_buffer->flush_data;
	write_buffer->flush_data = 0;
	write_buffer->lpn = INVALID_LPN;
	write_buffer->pgs = 0;
	write_buffer->flushing = true;
	schedule_internal_operation(write_buffer->sqid, nsecs_latest, write_buffer,
			bufs_to_release);
	return nsecs_latest;
}

static bool __zns_write(struct zns_ftl *zns_ftl, struct nvmev_request *req,
			struct nvmev_result *ret)
{
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct nvme_rw_command *cmd = &(req->cmd->rw);

	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);
	uint64_t slpn, elpn, lpn, zone_elpn;
	// get zone from start_lbai
	uint32_t zid = lba_to_zone(zns_ftl, slba);
	enum zone_state state = zone_descs[zid].state;

	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_xfer_completed = nsecs_start;
	uint64_t nsecs_latest = nsecs_start;
	uint32_t status = NVME_SC_SUCCESS;

	uint64_t pgs = 0;
	uint32_t wb_allocate_size = 0;

	struct buffer *write_buffer;

	if (cmd->opcode == nvme_cmd_zone_append) {
		slba = zone_descs[zid].wp;
		cmd->slba = slba;
	}

	slpn = lba_to_lpn(zns_ftl, slba);
	elpn = lba_to_lpn(zns_ftl, slba + nr_lba - 1);
	zone_elpn = zone_to_elpn(zns_ftl, zid);

	NVMEV_ZNS_DEBUG("%s slba 0x%llx nr_lba 0x%llx zone_id %d state %d\n", __func__, slba,
			nr_lba, zid, state);

	if(zns_ftl->last_nlb != nr_lba || zns_ftl->last_slba != slba)
	{
		NVMEV_ZMS_RW_DEBUG_VERBOSE("%s slpn %lld elpn %lld zone_id %d\n", __func__, slpn,elpn, zid);
		zns_ftl->last_nlb = nr_lba;
		zns_ftl->last_slba = slba;
		zns_ftl->last_stime  = nsecs_start;
	}
	if (zns_ftl->zp.zone_wb_size)
	{
		//#if(BASE_SSD == ZMS_PROTOTYPE)
		uint32_t wb_pointer = ZONED_DEVICE?zid % zns_ftl->zp.nr_zone_wb:0;
		write_buffer = &(zns_ftl->zone_write_buffer[wb_pointer]);
		if(write_buffer->zid!=zid)
		{
			if(write_buffer->notempty)
			{
				if(!write_buffer->flushing){
					//should flush
					NVMEV_ZMS_RW_DEBUG_VERBOSE("ZONE SWITCHING %d -> %d\n",write_buffer->zid,zid);
					nsecs_latest = zms_seqzone_flush(zns_ftl,write_buffer,nsecs_start);
				}
				return false;			
			}
			write_buffer->zid = zid;
			write_buffer->flushing = false;
		}
		//#else
		//write_buffer = &(zns_ftl->zone_write_buffer[zid]);
		//#endif
	}
	else
		write_buffer = zns_ftl->ssd->write_buffer;

	//only return false if buf remaining == 0
	if (!(wb_allocate_size = buffer_allocate(write_buffer, LBA_TO_BYTE(nr_lba))))
		return false;

	if ((LBA_TO_BYTE(nr_lba) % spp->write_unit_size) != 0) {
		status = NVME_SC_ZNS_INVALID_WRITE;
		goto out;
	}

	if (__check_boundary_error(zns_ftl, slba, nr_lba) == false) {
		// return boundary error
		status = NVME_SC_ZNS_ERR_BOUNDARY;
		goto out;
	}

	// check if slba == current write pointer
	if (slba != zone_descs[zid].wp) {
		NVMEV_ERROR("%s WP error slba 0x%llx nr_lba 0x%llx zone_id %d wp %llx state %d\n",
			    __func__, slba, nr_lba, zid, zns_ftl->zone_descs[zid].wp, state);
		status = NVME_SC_ZNS_INVALID_WRITE;
		goto out;
	}

	switch (state) {
	case ZONE_STATE_EMPTY: {
		// check if slba == start lba in zone
		if (slba != zone_descs[zid].zslba) {
			status = NVME_SC_ZNS_INVALID_WRITE;
			goto out;
		}

		if (is_zone_resource_full(zns_ftl, ACTIVE_ZONE)) {
			status = NVME_SC_ZNS_NO_ACTIVE_ZONE;
			goto out;
		}
		if (is_zone_resource_full(zns_ftl, OPEN_ZONE)) {
			status = NVME_SC_ZNS_NO_OPEN_ZONE;
			goto out;
		}
		acquire_zone_resource(zns_ftl, ACTIVE_ZONE);
		// go through
	}
	case ZONE_STATE_CLOSED: {
		if (acquire_zone_resource(zns_ftl, OPEN_ZONE) == false) {
			status = NVME_SC_ZNS_NO_OPEN_ZONE;
			goto out;
		}

		// change to ZSIO
		change_zone_state(zns_ftl, zid, ZONE_STATE_OPENED_IMPL);
		break;
	}
	case ZONE_STATE_OPENED_IMPL:
	case ZONE_STATE_OPENED_EXPL: {
		break;
	}
	case ZONE_STATE_FULL:
		status = NVME_SC_ZNS_ERR_FULL;
		goto out;
	case ZONE_STATE_READ_ONLY:
		status = NVME_SC_ZNS_ERR_READ_ONLY;
		goto out;
	case ZONE_STATE_OFFLINE:
		status = NVME_SC_ZNS_ERR_OFFLINE;
		goto out;
	}

	__increase_write_ptr(zns_ftl, zid, nr_lba);

	// get delay from nand model
	nsecs_latest = ssd_advance_write_buffer(zns_ftl->ssd, nsecs_latest, LBA_TO_BYTE(nr_lba));
	nsecs_xfer_completed = nsecs_latest;

	write_buffer->sqid = req->sq_id;
	NVMEV_ASSERT(zns_ftl->ssd_type==SSD_TYPE_ZNS); //TODO seq zone only!!
	for (lpn = slpn; lpn <= elpn; lpn += pgs) {

		uint64_t pg_off = lpn % (write_buffer->size / spp->pgsz);
		pgs = min(elpn - lpn + 1, (uint64_t)((write_buffer->size / spp->pgsz) - pg_off));

		while(wb_allocate_size==0)
		{
			wb_allocate_size = buffer_allocate(write_buffer, (elpn - lpn + 1)*spp->pgsz);
		}
		if(wb_allocate_size < pgs*spp->pgsz)
		{
			pgs = wb_allocate_size / spp->pgsz;
			wb_allocate_size = 0;
		}
		else wb_allocate_size -= spp->pgsz*pgs;

		if(write_buffer->lpn == INVALID_LPN) write_buffer->lpn = lpn;
		write_buffer->pgs += pgs;
		write_buffer->flush_data += spp->pgsz*pgs;

		NVMEV_ZMS_RW_DEBUG_VERBOSE("write buffer %d flush data(KiB): %ld / %ld,remaining %ld, slpn %lld pgs %lld\n",write_buffer->zid,BYTE_TO_KB(write_buffer->flush_data),BYTE_TO_KB(write_buffer->size),BYTE_TO_KB(write_buffer->remaining),write_buffer->lpn,write_buffer->pgs);
		/* Aggregate write io*/
		if ((write_buffer->flush_data == write_buffer->size)) {
			
			uint64_t nsecs_completed;
			nsecs_completed = zms_seqzone_flush(zns_ftl,write_buffer,nsecs_xfer_completed);
			nsecs_latest = max(nsecs_completed, nsecs_latest);
		}
	}

out:
	ret->status = status;
	if ((cmd->control & NVME_RW_FUA) ||
	    (spp->write_early_completion == 0)) /*Wait all flash operations*/
		ret->nsecs_target = nsecs_latest;
	else /*Early completion*/
		ret->nsecs_target = nsecs_xfer_completed;

	NVMEV_ZMS_RW_DEBUG("%s slpn %lld pgs %lld zone_id %d lat %lld us\n", __func__, slpn,(elpn-slpn+1), zid, (ret->nsecs_target-zns_ftl->last_stime)/1000);
	return true;
}

static bool __zns_write_zrwa(struct zns_ftl *zns_ftl, struct nvmev_request *req,
			     struct nvmev_result *ret)
{
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct znsparams *zpp = &zns_ftl->zp;
	struct nvme_rw_command *cmd = &(req->cmd->rw);
	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);
	uint64_t elba = cmd->slba + nr_lba - 1;

	// get zone from start_lbai
	uint32_t zid = lba_to_zone(zns_ftl, slba);
	enum zone_state state = zone_descs[zid].state;

	uint64_t prev_wp = zone_descs[zid].wp;
	const uint32_t lbas_per_zrwa = zpp->lbas_per_zrwa;
	const uint32_t lbas_per_zrwafg = zpp->lbas_per_zrwafg;
	uint64_t zrwa_impl_start = prev_wp + lbas_per_zrwa;
	uint64_t zrwa_impl_end = prev_wp + (2 * lbas_per_zrwa) - 1;

	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed = nsecs_start;
	uint64_t nsecs_xfer_completed = nsecs_start;
	uint64_t nsecs_latest = nsecs_start;
	uint32_t status = NVME_SC_SUCCESS;

	struct ppa ppa;
	struct nand_cmd swr;

	uint64_t nr_lbas_flush = 0, lpn, remaining, pgs = 0, pg_off;

	NVMEV_DEBUG(
		"%s slba 0x%llx nr_lba 0x%llx zone_id %d state %d wp 0x%llx zrwa_impl_start 0x%llx zrwa_impl_end 0x%llx  buffer %lu\n",
		__func__, slba, nr_lba, zid, state, prev_wp, zrwa_impl_start, zrwa_impl_end,
		zns_ftl->zrwa_buffer[zid].remaining);

	if ((LBA_TO_BYTE(nr_lba) % spp->write_unit_size) != 0) {
		status = NVME_SC_ZNS_INVALID_WRITE;
		goto out;
	}

	if (__check_boundary_error(zns_ftl, slba, nr_lba) == false) {
		// return boundary error
		status = NVME_SC_ZNS_ERR_BOUNDARY;
		goto out;
	}

	// valid range : wp <=  <= wp + 2*(size of zwra) -1
	if (slba < zone_descs[zid].wp || elba > zrwa_impl_end) {
		NVMEV_ERROR("%s slba 0x%llx nr_lba 0x%llx zone_id %d wp 0x%llx state %d\n",
			    __func__, slba, nr_lba, zid, zone_descs[zid].wp, state);
		status = NVME_SC_ZNS_INVALID_WRITE;
		goto out;
	}

	switch (state) {
	case ZONE_STATE_CLOSED:
	case ZONE_STATE_EMPTY: {
		if (acquire_zone_resource(zns_ftl, OPEN_ZONE) == false) {
			status = NVME_SC_ZNS_NO_OPEN_ZONE;
			goto out;
		}

		if (!buffer_allocate(&zns_ftl->zrwa_buffer[zid], zpp->zrwa_size))
			NVMEV_ASSERT(0);

		// change to ZSIO
		change_zone_state(zns_ftl, zid, ZONE_STATE_OPENED_IMPL);
		break;
	}
	case ZONE_STATE_OPENED_IMPL:
	case ZONE_STATE_OPENED_EXPL: {
		break;
	}
	case ZONE_STATE_FULL:
		status = NVME_SC_ZNS_ERR_FULL;
		goto out;
	case ZONE_STATE_READ_ONLY:
		status = NVME_SC_ZNS_ERR_READ_ONLY;
		goto out;
	case ZONE_STATE_OFFLINE:
		status = NVME_SC_ZNS_ERR_OFFLINE;
		goto out;
#if 0
		case ZONE_STATE_EMPTY :
			return NVME_SC_ZNS_INVALID_ZONE_OPERATION;
#endif
	}

	if (elba >= zrwa_impl_start) {
		nr_lbas_flush = DIV_ROUND_UP((elba - zrwa_impl_start + 1), lbas_per_zrwafg) *
				lbas_per_zrwafg;

		NVMEV_DEBUG("%s implicitly flush zid %d wp before 0x%llx after 0x%llx buffer %lu",
			    __func__, zid, prev_wp, zone_descs[zid].wp + nr_lbas_flush,
			    zns_ftl->zrwa_buffer[zid].remaining);
	} else if (elba == zone_to_elba(zns_ftl, zid)) {
		// Workaround. move wp to end of the zone and make state full implicitly
		nr_lbas_flush = elba - prev_wp + 1;

		NVMEV_DEBUG("%s end of zone zid %d wp before 0x%llx after 0x%llx buffer %lu",
			    __func__, zid, prev_wp, zone_descs[zid].wp + nr_lbas_flush,
			    zns_ftl->zrwa_buffer[zid].remaining);
	}

	if (nr_lbas_flush > 0) {
		if (!buffer_allocate(&zns_ftl->zrwa_buffer[zid], LBA_TO_BYTE(nr_lbas_flush)))
			return false;

		__increase_write_ptr(zns_ftl, zid, nr_lbas_flush);
	}
	// get delay from nand model
	nsecs_latest = nsecs_start;
	nsecs_latest = ssd_advance_write_buffer(zns_ftl->ssd, nsecs_latest, LBA_TO_BYTE(nr_lba));
	nsecs_xfer_completed = nsecs_latest;

	lpn = lba_to_lpn(zns_ftl, prev_wp);
	remaining = nr_lbas_flush / spp->secs_per_pg;
	/* Aggregate write io in flash page */
	while (remaining > 0) {
		ppa = __lpn_to_ppa(zns_ftl, lpn);
		pg_off = ppa.g.pg % spp->pgs_per_oneshotpg;
		pgs = min(remaining, (uint64_t)(spp->pgs_per_oneshotpg - pg_off));

		if ((pg_off + pgs) == spp->pgs_per_oneshotpg) {
			swr.type = USER_IO;
			swr.cmd = NAND_WRITE;
			swr.stime = nsecs_xfer_completed;
			swr.xfer_size = spp->pgs_per_oneshotpg * spp->pgsz;
			swr.interleave_pci_dma = false;
			swr.ppa = &ppa;

			nsecs_completed = ssd_advance_nand(zns_ftl->ssd, &swr);
			nsecs_latest = max(nsecs_completed, nsecs_latest);

			schedule_internal_operation(req->sq_id, nsecs_completed,
						    &zns_ftl->zrwa_buffer[zid],
						    spp->pgs_per_oneshotpg * spp->pgsz);
		}

		lpn += pgs;
		remaining -= pgs;
	}

out:
	ret->status = status;

	if ((cmd->control & NVME_RW_FUA) ||
	    (spp->write_early_completion == 0)) /*Wait all flash operations*/
		ret->nsecs_target = nsecs_latest;
	else /*Early completion*/
		ret->nsecs_target = nsecs_xfer_completed;

	return true;
}

bool zns_write(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	struct nvme_rw_command *cmd = &(req->cmd->rw);
	uint64_t slpn = lba_to_lpn(zns_ftl, cmd->slba);

	// get zone from start_lba
	uint32_t zid = lpn_to_zone(zns_ftl, slpn);

	NVMEV_DEBUG("%s slba 0x%llx zone_id %d \n", __func__, cmd->slba, zid);

	if (zone_descs[zid].zrwav == 0)
		return __zns_write(zns_ftl, req, ret);
	else
		return __zns_write_zrwa(zns_ftl, req, ret);
}

bool zns_read(struct nvmev_ns *ns, struct nvmev_request *req, struct nvmev_result *ret)
{
	struct zns_ftl *zns_ftl = (struct zns_ftl *)ns->ftls;
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zone_descriptor *zone_descs = zns_ftl->zone_descs;
	struct nvme_rw_command *cmd = &(req->cmd->rw);

	uint64_t slba = cmd->slba;
	uint64_t nr_lba = __nr_lbas_from_rw_cmd(cmd);

	uint64_t slpn = lba_to_lpn(zns_ftl, slba);
	uint64_t elpn = lba_to_lpn(zns_ftl, slba + nr_lba - 1);
	uint64_t lpn,la;

	// get zone from start_lba
	uint32_t zid = lpn_to_zone(zns_ftl, slpn);
	uint32_t status = NVME_SC_SUCCESS;
	uint64_t nsecs_start = req->nsecs_start;
	uint64_t nsecs_completed = nsecs_start, nsecs_latest = 0;
	struct ppa ppa;
	struct nand_cmd swr;
	uint64_t read_slpn,read_pgs; // for debug

	NVMEV_ZNS_DEBUG(
		"%s slba 0x%llx nr_lba 0x%llx zone_id %d state %d wp 0x%llx last lba 0x%llx\n",
		__func__, slba, nr_lba, zid, zone_descs[zid].state, zone_descs[zid].wp,
		(slba + nr_lba - 1));

	if (zone_descs[zid].state == ZONE_STATE_OFFLINE) {
		status = NVME_SC_ZNS_ERR_OFFLINE;
	} else if (__check_boundary_error(zns_ftl, slba, nr_lba) == false) {
		// return boundary error
		status = NVME_SC_ZNS_ERR_BOUNDARY;
	}

	// get delay from nand model
	nsecs_latest = nsecs_start;
	if (LBA_TO_BYTE(nr_lba) <= KB(4))
		nsecs_latest += spp->fw_4kb_rd_lat;
	else
		nsecs_latest += spp->fw_rd_lat;

	swr.type = USER_IO;
	swr.stime = nsecs_latest;
	swr.interleave_pci_dma = false;

	uint64_t nsecs_read_begin = nsecs_latest;
	read_slpn = INVALID_LPN;
	read_pgs = 0;
	
	for (lpn = slpn; lpn <= elpn; lpn ++) {
		ppa = get_maptbl_ent(zns_ftl, lpn);
		//#if(BASE_SSD == ZMS_PROTOTYPE)
	
		//we do not implement read process for the page map for conventional zones
		NVMEV_ASSERT(zns_ftl->zp.zone_type == SSD_TYPE_ZNS);
		//check if this page in write buffer
		uint32_t wb_pointer = ZONED_DEVICE?zid % zns_ftl->zp.nr_zone_wb:0;
		struct buffer* write_buffer = &(zns_ftl->zone_write_buffer[wb_pointer]);
		if(zid==write_buffer->zid && write_buffer->lpn!=INVALID_LPN && lpn >= write_buffer->lpn && lpn <= write_buffer->lpn+write_buffer->pgs)
		{
			read_pgs++;
			if(read_slpn == INVALID_LPN) read_slpn = lpn;
			if(lpn == write_buffer->lpn+write_buffer->pgs || lpn == elpn)
			{
				nsecs_completed = ssd_advance_write_buffer(zns_ftl->ssd, nsecs_read_begin,read_pgs*spp->pgsz);
				nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
				NVMEV_ZMS_RW_DEBUG("%s [write buffer] read lpn %lld  pgs %lld lat %lld us\n", __func__, read_slpn,read_pgs,(nsecs_completed-nsecs_read_begin)/1000);
				read_slpn = INVALID_LPN;
				read_pgs = 0;
			}
			continue;
		}
		//read from nand
		
		int idx;
		idx = l2p_search(zns_ftl,lpn,&la);
		uint64_t nsecs_map_read = nsecs_read_begin;
		if(idx==-1)
		{
			int i;
			struct ppa pa;
			pa.ppa = ppa.ppa;
			la = lpn;

			struct nand_cmd mapr = {
				.type = USER_IO,
				.cmd = NAND_READ,
				.interleave_pci_dma = false,
				.xfer_size = spp->pgsz,
			};

			if(ZONED_DEVICE && L2P_HYBRID_MAP)
			{
				for( i = 0;i < NUM_MAP;i++){
					la = AU_to_slpn(zns_ftl,MAP_GRAN(i),lpn);
					pa = get_maptbl_ent(zns_ftl, la);
					if(mapped_ppa(&pa) && pa.zms.map == MAP_GRAN(i)) break;
					if(L2P_SEARCH_SCHMEME == L2P_SEARCH_MULTIPLE && mapped_ppa(&pa))
					{
						//read from nand and get wrong information
						NVMEV_ZMS_L2P_DEBUG("check for %s map...\n",MAP_GRAN(i)==ZONE_MAP?"zone":(MAP_GRAN(i)==CHUNK_MAP?"chunk":"page"));
						mapr.stime = nsecs_map_read;
						mapr.ppa = &pa; //FIXME location of l2p mapping table
						nsecs_map_read = ssd_advance_nand(zns_ftl->ssd, &mapr);
					}
				}
			}

			if(mapped_ppa(&pa))
			{
				idx = l2p_insert(zns_ftl,la,pa.zms.map,check_resident(zns_ftl,pa.zms.map));
				if(pa.zms.map==PAGE_MAP)
				{
					//pre read for page map
					for(int i = 0;i < zns_ftl->pre_read; i++)
					{
						if(la+i >= zns_ftl->tt_lpns) break;
						struct ppa pre_ppa = get_maptbl_ent(zns_ftl, la+i);
						if(mapped_ppa(&pre_ppa))
						{
							l2p_insert(zns_ftl,la+i,pre_ppa.zms.map,0);
							if(pre_ppa.zms.map!=PAGE_MAP) break;
						}
						else break;
					}
				}

				mapr.stime = nsecs_map_read;
				mapr.ppa = &pa; //FIXME location of l2p mapping table
				nsecs_map_read = ssd_advance_nand(zns_ftl->ssd, &mapr);
				NVMEV_ZMS_L2P_DEBUG("%s L2P Fetch lpn %lld (%s la %lld) lat %lld us idx %d\n", __func__, lpn, pa.zms.map==ZONE_MAP?"zone":(pa.zms.map==CHUNK_MAP?"chunk":"page"),la,(nsecs_map_read-nsecs_read_begin)/1000,idx);
			}
			else
			{
				idx = l2p_insert(zns_ftl,la,PAGE_MAP,0);
			}
		}
		
		swr.stime = max(swr.stime,nsecs_map_read);

		if(idx!=-1) l2p_access(zns_ftl,la,idx);
		
		//#endif
		
		if(!mapped_ppa(&ppa))
		{
			//NVMEV_ZMS_DEBUG("lpn %lld not mapped\n",lpn);
			continue;
		}
		read_pgs++;
		if(read_slpn == INVALID_LPN) read_slpn = lpn;
		bool pSLC = check_pSLC(zns_ftl,&ppa);

		//NVMEV_ZMS_DEBUG("%s ppa ch %d lun %d pl %d blk %d pg %d\n",__func__,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);

		if(lpn==elpn || zms_last_pg_in_flashpg(zns_ftl,&ppa))
		{
			swr.cmd = pSLC?NAND_PSLC_READ:NAND_READ;
			swr.xfer_size = read_pgs * spp->pgsz;
			swr.ppa = &ppa;

			nsecs_completed = ssd_advance_nand(zns_ftl->ssd, &swr);
			nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
			NVMEV_ZMS_RW_DEBUG("%s [%s read] lpn %lld pgs %lld lat %lld us\n",__func__,pSLC?"pSLC":"normal",read_slpn,read_pgs,(nsecs_completed-nsecs_read_begin)/1000);

			//NVMEV_ZMS_DEBUG("READ lat %lld us\n",(nsecs_completed-nsecs_read_begin)/1000);

			swr.stime = nsecs_read_begin;
			read_slpn = INVALID_LPN;
			read_pgs = 0;
		}
	}
	

	if (swr.interleave_pci_dma == false) {
		nsecs_completed = ssd_advance_pcie(zns_ftl->ssd, nsecs_latest, nr_lba * spp->secsz);
		nsecs_latest = (nsecs_completed > nsecs_latest) ? nsecs_completed : nsecs_latest;
	}
	

	ret->status = status;
	ret->nsecs_target = nsecs_latest;
	//NVMEV_ZMS_DEBUG("%s slba 0x%llx (slpn 0x%llx) nr_lba 0x%llx zone_id %d lat %lld us\n", __func__, slba,slpn,nr_lba, zid, (ret->nsecs_target-nsecs_start)/1000);
	return true;
}

static void try_and_earse_line(struct zns_ftl *zns_ftl, struct ppa *ppa)
{
	struct ssdparams *spp = &zns_ftl->ssd->sp;
	struct zms_line *line;
	line = get_line(zns_ftl,ppa);
	if(line->ipc == spp->pgs_per_line)
	{
		struct ppa e_ppa;
		struct zms_write_flow_control* wfc = check_pSLC(zns_ftl,ppa)?&zns_ftl->pslc_wfc:&zns_ftl->wfc;
		wfc->write_credits += line->ipc;

		e_ppa.ppa = 0;
		e_ppa.zms.blk = ppa->zms.blk;
		int ch,lun;
		for(ch = 0; ch < spp->nchs; ch++)
		{
			for(lun = 0;lun < spp->luns_per_ch;lun++)
			{
				struct nand_lun *lunp = get_lun(zns_ftl->ssd, &e_ppa);

				e_ppa.zms.ch = ch;
				e_ppa.zms.lun = lun;
				mark_block_free(zns_ftl, &e_ppa);

				if (zns_ftl->zp.enable_gc_delay) {
					struct nand_cmd gce = {
						.type = GC_IO,
						.cmd = NAND_ERASE,
						.stime = 0,
						.interleave_pci_dma = false,
						.ppa = &e_ppa,
					};
					ssd_advance_nand(zns_ftl->ssd, &gce);
				}

				lunp->gc_endtime = lunp->next_lun_avail_time;
			}
		}

		NVMEV_ZMS_GC_DEBUG("Erase %s line (id = %d)!\n",check_pSLC(zns_ftl,ppa)?"pSLC":"normal",line->id);
		mark_line_free(zns_ftl,ppa);
	}
}

void zms_reset_zone(struct zns_ftl *zns_ftl, uint64_t zid)
{
	uint64_t slpn = zone_to_slpn(zns_ftl,zid);
	uint64_t elpn = zone_to_elpn(zns_ftl,zid);
	uint32_t wb_pointer = ZONED_DEVICE?zid % zns_ftl->zp.nr_zone_wb:0;
	struct buffer* write_buffer = &(zns_ftl->zone_write_buffer[wb_pointer]);
	uint64_t lpn;
	struct ppa ppa,last_ppa;
	last_ppa.ppa = UNMAPPED_PPA;
	
	size_t bufs_to_release = 0;

	for(lpn = slpn; lpn<=elpn ;lpn++)
	{
		ppa = get_maptbl_ent(zns_ftl,lpn);
		if(mapped_ppa(&ppa))
		{
			//NVMEV_INFO("try to unmap lpn %lld ppa (ch %d lun %d pl %d blk %d pg %d)\n",lpn,ppa.zms.ch,ppa.zms.lun,ppa.zms.pl,ppa.zms.blk,ppa.zms.pg);
			mark_PA_invalid(zns_ftl,&ppa);
			zns_ftl->maptbl[lpn].ppa = UNMAPPED_PPA;
			try_and_earse_line(zns_ftl,&ppa);
		}
		else
		{
			if(zid==write_buffer->zid && write_buffer->lpn!=INVALID_LPN && lpn >= write_buffer->lpn && lpn <= write_buffer->lpn+write_buffer->pgs)
			{
				bufs_to_release = write_buffer->flush_data; //FIXME: non-zoned device!
				break;
			}
		}

		if(mapped_ppa(&last_ppa))
		{
			nextpage(zns_ftl,&last_ppa);
			if(!mapped_ppa(&last_ppa))
			{
				//unaligned writes may come here
				continue;
			}

			if(!ppa_same(last_ppa,ppa)) 
			{
				mark_PA_invalid(zns_ftl,&last_ppa);
				try_and_earse_line(zns_ftl,&last_ppa); //erase the reserved pages
			}
		}
		else
		{
			if(mapped_ppa(&ppa) && !check_pSLC(zns_ftl,&ppa)) last_ppa.ppa = ppa.ppa;
		}
		
	}

	if(bufs_to_release)
	{
		NVMEV_ZMS_GC_DEBUG("Evict write buffer %ld KiB\n",BYTE_TO_KB(bufs_to_release));
		write_buffer->flush_data = 0;
		write_buffer->lpn = INVALID_LPN;
		write_buffer->pgs = 0;
		write_buffer->flushing = true;
		schedule_internal_operation(write_buffer->sqid, 0, write_buffer,
			bufs_to_release);
	}

	NVMEV_ZMS_GC_DEBUG("Zone Reset. pSLC lines: %d/%d, normal lines %d/%d (free/all)\n",zns_ftl->pslc_lm.free_line_cnt,zns_ftl->pslc_lm.tt_lines,zns_ftl->lm.free_line_cnt,zns_ftl->lm.tt_lines);
}