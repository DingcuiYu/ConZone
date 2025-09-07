// SPDX-License-Identifier: GPL-2.0-only

#include <linux/ktime.h>
#include <linux/sched/clock.h>

#include "nvmev.h"
#include "ssd.h"

static inline uint64_t __get_ioclock(struct ssd *ssd) { return cpu_clock(ssd->cpu_nr_dispatcher); }

void buffer_init(struct buffer *buf, size_t size)
{
	spin_lock_init(&buf->lock);
	buf->size = size;
	buf->remaining = size;
	buf->capacity = size;

	buf->tt_lpns = size / 4096;
	buf->zid = -1;
	buf->lpns = kmalloc(sizeof(uint64_t) * buf->tt_lpns, GFP_KERNEL);
	for (int i = 0; i < buf->tt_lpns; i++) {
		buf->lpns[i] = INVALID_LPN;
	}
	buf->pgs = 0;
	buf->sqid = 0;
	buf->flushing = false;
	buf->flush_data = 0;
	buf->time = 0;
}

uint32_t buffer_allocate(struct buffer *buf, size_t size)
{
	while (!spin_trylock(&buf->lock)) {
		cpu_relax();
	}

	if (buf->remaining < size) {
		size = 0;
#if (BASE_SSD == CONZONE_PROTOTYPE)
		size = buf->remaining;
#endif
	}

	buf->remaining -= size;

	spin_unlock(&buf->lock);
	return size;
}

bool buffer_release(struct buffer *buf, size_t size)
{
	while (!spin_trylock(&buf->lock))
		;
	buf->remaining += size;
	if (buf->remaining >= buf->size) {
		buf->remaining = buf->size;
	}
	// NVMEV_INFO("%s: release %ld KiB  remaining %ld
	// KiB\n",__func__,BYTE_TO_KB(size),BYTE_TO_KB(buf->remaining));
	buf->flushing = false;
	spin_unlock(&buf->lock);

	return true;
}

void buffer_refill(struct buffer *buf)
{
	while (!spin_trylock(&buf->lock))
		;
	buf->remaining = buf->size;
	spin_unlock(&buf->lock);
}

void buffer_remove(struct buffer *buf) { kfree(buf->lpns); }

#if (BASE_SSD == CONZONE_PROTOTYPE)
static void ssd_init_l2pcache(struct l2pcache *cache)
{
	int i, j;

	cache->size = L2P_CACHE_SIZE / L2P_ENTRY_SIZE; //# of entries
	cache->evict_policy = L2P_EVICT_POLICY;
	cache->num_slots = L2P_CACHE_HASH_SLOT;

	cache->slot_size = cache->size / cache->num_slots;
	cache->mapping = kmalloc(sizeof(struct l2pcache_ent *) * (cache->num_slots), GFP_KERNEL);
	cache->slot_len = kmalloc(sizeof(int) * cache->num_slots, GFP_KERNEL);
	cache->head = kmalloc(sizeof(int) * cache->num_slots, GFP_KERNEL);
	cache->tail = kmalloc(sizeof(int) * cache->num_slots, GFP_KERNEL);

	for (i = 0; i < cache->num_slots; i++) {
		cache->mapping[i] = kmalloc(sizeof(struct l2pcache_ent) * (cache->slot_size), GFP_KERNEL);
		for (j = 0; j < cache->slot_size; j++) {
			cache->mapping[i][j].lpn = INVALID_LPN;
			cache->mapping[i][j].granularity = PAGE_MAP;
			cache->mapping[i][j].resident = 0;
			cache->mapping[i][j].last = -1;
			cache->mapping[i][j].next = -1;
		}
		cache->head[i] = 0;
		cache->tail[i] = 0;
		cache->slot_len[i] = 0;
	}
}

static void ssd_remove_l2pcache(struct l2pcache *cache)
{
	for (int i = 0; i < cache->num_slots; i++) {
		kfree(cache->mapping[i]);
	}
	kfree(cache->tail);
	kfree(cache->head);
	kfree(cache->slot_len);
	kfree(cache->mapping);
}
#endif

static void check_params(struct ssdparams *spp)
{
	/*
	 * we are using a general write pointer increment method now, no need to
	 * force luns_per_ch and nchs to be power of 2
	 */

	// ftl_assert(is_power_of_2(spp->luns_per_ch));
	// ftl_assert(is_power_of_2(spp->nchs));
}

void ssd_init_params(struct ssdparams *spp, uint64_t capacity, uint32_t nparts)
{
	uint64_t blk_size, total_size, meta_size;

	spp->secsz = LBA_SIZE;
	spp->secs_per_pg = 4096 / LBA_SIZE; // pg == 4KB
	spp->pgsz = spp->secsz * spp->secs_per_pg;

	spp->nchs = NAND_CHANNELS;
	spp->pls_per_lun = PLNS_PER_LUN;
	spp->luns_per_ch = LUNS_PER_NAND_CH;
	spp->cell_mode = CELL_MODE;

	/* partitioning SSD by dividing channel*/
	NVMEV_ASSERT((spp->nchs % nparts) == 0);
	spp->nchs /= nparts;
	capacity /= nparts;

#if (BASE_SSD == CONZONE_PROTOTYPE)
	spp->pslc_blks = pSLC_INIT_BLKS;
	spp->meta_pslc_blks = META_pSLC_INIT_BLKS;
	spp->meta_normal_blks = 0;
#endif

	if (BLKS_PER_PLN > 0) {
		/* flashpgs_per_blk depends on capacity */
		spp->blks_per_pl = BLKS_PER_PLN;
		blk_size = DIV_ROUND_UP(capacity,
								spp->blks_per_pl * spp->pls_per_lun * spp->luns_per_ch * spp->nchs);
	} else {
		NVMEV_ASSERT(BLK_SIZE > 0);
		blk_size = BLK_SIZE;
#if (BASE_SSD == CONZONE_PROTOTYPE)
		meta_size =
			blk_size * (spp->meta_pslc_blks + spp->meta_normal_blks) * spp->luns_per_ch * spp->nchs;
		if (meta_size > PHYSICAL_META_SIZE) {
			capacity += (meta_size - PHYSICAL_META_SIZE);
		}
		spp->blks_per_pl =
			DIV_ROUND_UP(capacity, (blk_size * spp->pls_per_lun * spp->luns_per_ch * spp->nchs));
		// SSD can't be too small.
		NVMEV_ASSERT(spp->blks_per_pl >= 4);
#else
		spp->blks_per_pl =
			DIV_ROUND_UP(capacity, (blk_size * spp->pls_per_lun * spp->luns_per_ch * spp->nchs));
#endif
	}

	NVMEV_ASSERT((ONESHOT_PAGE_SIZE % spp->pgsz) == 0 && (FLASH_PAGE_SIZE % spp->pgsz) == 0);
	NVMEV_ASSERT((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

	spp->pgs_per_oneshotpg = ONESHOT_PAGE_SIZE / (spp->pgsz);
	spp->oneshotpgs_per_blk = blk_size / ONESHOT_PAGE_SIZE; // DIV_ROUND_UP
	spp->pgs_per_flashpg = FLASH_PAGE_SIZE / (spp->pgsz);
	spp->flashpgs_per_blk = (ONESHOT_PAGE_SIZE / FLASH_PAGE_SIZE) * spp->oneshotpgs_per_blk;
	spp->pgs_per_blk = spp->pgs_per_oneshotpg * spp->oneshotpgs_per_blk;
	NVMEV_INFO("------------SSD PARAMS-----------\n");
	NVMEV_INFO(
		"[# of Channels] %d [Luns per Channel] %d [Planes per Lun] %d [Blocks per Plane] %llu "
		"[Logical Pages per Block] %llu\n",
		spp->nchs, spp->luns_per_ch, spp->pls_per_lun, spp->blks_per_pl, spp->pgs_per_blk);
	NVMEV_INFO("[Logical Page Size] %d KiB [Flash Page Size] %d KiB [Oneshot Page Size] %d KiB\n",
			   BYTE_TO_KB(spp->pgsz), BYTE_TO_KB(FLASH_PAGE_SIZE), BYTE_TO_KB(ONESHOT_PAGE_SIZE));
	NVMEV_INFO("[pgs_per_oneshotpg] %lld\n", spp->pgs_per_oneshotpg);
	spp->write_unit_size = WRITE_UNIT_SIZE;

#if (BASE_SSD == CONZONE_PROTOTYPE)
	spp->pg_4kb_rd_lat[CELL_MODE_SLC][CELL_TYPE_LSB] = SLC_NAND_READ_LATENCY_LSB;
	spp->pg_4kb_rd_lat[CELL_MODE_SLC][CELL_TYPE_MSB] = 0;
	spp->pg_4kb_rd_lat[CELL_MODE_SLC][CELL_TYPE_CSB] = 0;
	spp->pg_4kb_rd_lat[CELL_MODE_SLC][CELL_TYPE_TSB] = 0;

	spp->pg_4kb_rd_lat[CELL_MODE_MLC][CELL_TYPE_LSB] = MLC_NAND_READ_LATENCY_LSB;
	spp->pg_4kb_rd_lat[CELL_MODE_MLC][CELL_TYPE_MSB] = MLC_NAND_READ_LATENCY_MSB;
	spp->pg_4kb_rd_lat[CELL_MODE_MLC][CELL_TYPE_CSB] = 0;
	spp->pg_4kb_rd_lat[CELL_MODE_MLC][CELL_TYPE_TSB] = 0;

	spp->pg_4kb_rd_lat[CELL_MODE_TLC][CELL_TYPE_LSB] = TLC_NAND_READ_LATENCY_LSB;
	spp->pg_4kb_rd_lat[CELL_MODE_TLC][CELL_TYPE_MSB] = TLC_NAND_READ_LATENCY_MSB;
	spp->pg_4kb_rd_lat[CELL_MODE_TLC][CELL_TYPE_CSB] = TLC_NAND_READ_LATENCY_CSB;
	spp->pg_4kb_rd_lat[CELL_MODE_TLC][CELL_TYPE_TSB] = 0;

	spp->pg_4kb_rd_lat[CELL_MODE_QLC][CELL_TYPE_LSB] = QLC_NAND_READ_LATENCY_LSB;
	spp->pg_4kb_rd_lat[CELL_MODE_QLC][CELL_TYPE_MSB] = QLC_NAND_READ_LATENCY_MSB;
	spp->pg_4kb_rd_lat[CELL_MODE_QLC][CELL_TYPE_CSB] = QLC_NAND_READ_LATENCY_CSB;
	spp->pg_4kb_rd_lat[CELL_MODE_QLC][CELL_TYPE_TSB] = QLC_NAND_READ_LATENCY_TSB;

	spp->pg_rd_lat[CELL_MODE_SLC][CELL_TYPE_LSB] = SLC_NAND_READ_LATENCY_LSB;
	spp->pg_rd_lat[CELL_MODE_SLC][CELL_TYPE_MSB] = 0;
	spp->pg_rd_lat[CELL_MODE_SLC][CELL_TYPE_CSB] = 0;
	spp->pg_rd_lat[CELL_MODE_SLC][CELL_TYPE_TSB] = 0;

	spp->pg_rd_lat[CELL_MODE_MLC][CELL_TYPE_LSB] = MLC_NAND_READ_LATENCY_LSB;
	spp->pg_rd_lat[CELL_MODE_MLC][CELL_TYPE_MSB] = MLC_NAND_READ_LATENCY_MSB;
	spp->pg_rd_lat[CELL_MODE_MLC][CELL_TYPE_CSB] = 0;
	spp->pg_rd_lat[CELL_MODE_MLC][CELL_TYPE_TSB] = 0;

	spp->pg_rd_lat[CELL_MODE_TLC][CELL_TYPE_LSB] = TLC_NAND_READ_LATENCY_LSB;
	spp->pg_rd_lat[CELL_MODE_TLC][CELL_TYPE_MSB] = TLC_NAND_READ_LATENCY_MSB;
	spp->pg_rd_lat[CELL_MODE_TLC][CELL_TYPE_CSB] = TLC_NAND_READ_LATENCY_CSB;
	spp->pg_rd_lat[CELL_MODE_TLC][CELL_TYPE_TSB] = 0;

	spp->pg_rd_lat[CELL_MODE_QLC][CELL_TYPE_LSB] = QLC_NAND_READ_LATENCY_LSB;
	spp->pg_rd_lat[CELL_MODE_QLC][CELL_TYPE_MSB] = QLC_NAND_READ_LATENCY_MSB;
	spp->pg_rd_lat[CELL_MODE_QLC][CELL_TYPE_CSB] = QLC_NAND_READ_LATENCY_CSB;
	spp->pg_rd_lat[CELL_MODE_QLC][CELL_TYPE_TSB] = QLC_NAND_READ_LATENCY_TSB;

	spp->pg_wr_lat[CELL_MODE_SLC] = SLC_NAND_PROG_LATENCY;
	spp->pg_wr_lat[CELL_MODE_MLC] = MLC_NAND_PROG_LATENCY;
	spp->pg_wr_lat[CELL_MODE_TLC] = TLC_NAND_PROG_LATENCY;
	spp->pg_wr_lat[CELL_MODE_QLC] = QLC_NAND_PROG_LATENCY;
#else
	spp->pg_4kb_rd_lat[spp->cell_mode][CELL_TYPE_LSB] = NAND_4KB_READ_LATENCY_LSB;
	spp->pg_4kb_rd_lat[spp->cell_mode][CELL_TYPE_MSB] = NAND_4KB_READ_LATENCY_MSB;
	spp->pg_4kb_rd_lat[spp->cell_mode][CELL_TYPE_CSB] = NAND_4KB_READ_LATENCY_CSB;
	spp->pg_4kb_rd_lat[spp->cell_mode][CELL_TYPE_TSB] = NAND_4KB_READ_LATENCY_TSB;
	spp->pg_rd_lat[spp->cell_mode][CELL_TYPE_LSB] = NAND_READ_LATENCY_LSB;
	spp->pg_rd_lat[spp->cell_mode][CELL_TYPE_MSB] = NAND_READ_LATENCY_MSB;
	spp->pg_rd_lat[spp->cell_mode][CELL_TYPE_CSB] = NAND_READ_LATENCY_CSB;
	spp->pg_rd_lat[spp->cell_mode][CELL_TYPE_TSB] = NAND_READ_LATENCY_TSB;

	spp->pg_wr_lat[spp->cell_mode] = NAND_PROG_LATENCY;
#endif
	spp->blk_er_lat = NAND_ERASE_LATENCY;
	spp->max_ch_xfer_size = MAX_CH_XFER_SIZE;

	spp->fw_4kb_rd_lat = FW_4KB_READ_LATENCY;
	spp->fw_rd_lat = FW_READ_LATENCY;
	spp->fw_ch_xfer_lat = FW_CH_XFER_LATENCY;
	spp->fw_wbuf_lat0 = FW_WBUF_LATENCY0;
	spp->fw_wbuf_lat1 = FW_WBUF_LATENCY1;

	spp->ch_bandwidth = NAND_CHANNEL_BANDWIDTH;
	spp->pcie_bandwidth = PCIE_BANDWIDTH;

	spp->write_buffer_size = GLOBAL_WB_SIZE;
	spp->write_early_completion = WRITE_EARLY_COMPLETION;

	/* calculated values */
	spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
	spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
	spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
	spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
	spp->tt_secs = spp->secs_per_ch * spp->nchs;

	spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
	spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
	spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
	spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

	spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
	spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
	spp->tt_blks = spp->blks_per_ch * spp->nchs;

	spp->pls_per_ch = spp->pls_per_lun * spp->luns_per_ch;
	spp->tt_pls = spp->pls_per_ch * spp->nchs;

	spp->tt_luns = spp->luns_per_ch * spp->nchs;

	/* line is special, put it at the end */
	spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
	spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
	spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
	spp->tt_lines = spp->blks_per_lun;
	/* TODO: to fix under multiplanes */ // lun size is super-block(line) size

#if (BASE_SSD == CONZONE_PROTOTYPE)
	spp->blksz = blk_size;
	spp->pslc_blksz = pSLC_BLK_SIZE;
	spp->pslc_pgs_per_oneshotpg = pSLC_ONESHOT_PAGE_SIZE / (spp->pgsz);
	spp->pslc_flashpgs_per_blk = DIV_ROUND_UP(spp->pslc_blksz, pSLC_ONESHOT_PAGE_SIZE);
	spp->pslc_pgs_per_flashpg = pSLC_ONESHOT_PAGE_SIZE / (spp->pgsz);
	spp->pslc_pgs_per_blk = DIV_ROUND_UP(spp->pslc_blksz, spp->pgsz);
	spp->pslc_pgs_per_line = spp->blks_per_line * spp->pslc_pgs_per_blk;

	NVMEV_INFO(
		"[Total pSLC Superblocks] %u [Meta pSLC Superblocks] %u [Meta Normal Superblocks] %u\n",
		spp->pslc_blks, spp->meta_pslc_blks, spp->meta_normal_blks);
	NVMEV_INFO("[Total pSLC Size] %u MiB [Meta pSLC Size] %u MiB\n",
			   BYTE_TO_MB(spp->blksz) * spp->pslc_blks * spp->luns_per_ch * spp->nchs,
			   BYTE_TO_MB(spp->blksz) * spp->meta_pslc_blks * spp->luns_per_ch * spp->nchs);

	NVMEV_INFO("[Total pSLC Capacity] %u MiB [Meta pSLC Capacity] %u MiB\n",
			   BYTE_TO_MB(spp->pslc_blksz) * spp->pslc_blks * spp->luns_per_ch * spp->nchs,
			   BYTE_TO_MB(spp->pslc_blksz) * spp->meta_pslc_blks * spp->luns_per_ch * spp->nchs);
	NVMEV_INFO("[Logical Pages per pSLC Block] %u [Logical Pages per pSLC Line] %lu",
			   spp->pslc_pgs_per_blk, spp->pslc_pgs_per_line);
	NVMEV_INFO("[pslc_pgs_per_oneshotpg] %lld\n", spp->pslc_pgs_per_oneshotpg);
#endif

	check_params(spp);

	total_size = (unsigned long)spp->tt_luns * spp->blks_per_lun * spp->pgs_per_blk * spp->secsz *
				 spp->secs_per_pg;
	blk_size = spp->pgs_per_blk * spp->secsz * spp->secs_per_pg;
	NVMEV_INFO(
		"Total Capacity(GiB,MiB)=%llu,%llu chs=%u luns=%lu lines=%lu blk-size(MiB,KiB)=%llu,%llu "
		"line-size(MiB,KiB)=%lu,%lu",
		BYTE_TO_GB(total_size), BYTE_TO_MB(total_size), spp->nchs, spp->tt_luns, spp->tt_lines,
		BYTE_TO_MB(spp->pgs_per_blk * spp->pgsz), BYTE_TO_KB(spp->pgs_per_blk * spp->pgsz),
		BYTE_TO_MB(spp->pgs_per_line * spp->pgsz), BYTE_TO_KB(spp->pgs_per_line * spp->pgsz));
	NVMEV_INFO("----------------------------\n");
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
	int i;
	pg->nsecs = spp->secs_per_pg;
	pg->sec = kmalloc(sizeof(nand_sec_status_t) * pg->nsecs, GFP_KERNEL);
	for (i = 0; i < pg->nsecs; i++) {
		pg->sec[i] = SEC_FREE;
	}
	pg->status = PG_FREE;
}

static void ssd_remove_nand_page(struct nand_page *pg) { kfree(pg->sec); }

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
	int i;
	blk->npgs = spp->pgs_per_blk;
	blk->pg = kmalloc(sizeof(struct nand_page) * blk->npgs, GFP_KERNEL);
	for (i = 0; i < blk->npgs; i++) {
		ssd_init_nand_page(&blk->pg[i], spp);
	}
	blk->ipc = 0;
	blk->vpc = 0;
	blk->erase_cnt = 0;
	blk->wp = 0;
}

static void ssd_remove_nand_blk(struct nand_block *blk)
{
	int i;

	for (i = 0; i < blk->npgs; i++)
		ssd_remove_nand_page(&blk->pg[i]);

	kfree(blk->pg);
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
	int i;
	pl->nblks = spp->blks_per_pl;
	pl->blk = kmalloc(sizeof(struct nand_block) * pl->nblks, GFP_KERNEL);
	for (i = 0; i < pl->nblks; i++) {
		ssd_init_nand_blk(&pl->blk[i], spp);
#if (BASE_SSD == CONZONE_PROTOTYPE)
		if (i < spp->meta_pslc_blks || (i >= (spp->meta_pslc_blks + spp->meta_normal_blks) &&
										i < (spp->meta_normal_blks + spp->pslc_blks))) {
			pl->blk[i].nand_type = CELL_MODE_SLC;
			pl->blk[i].used_pgs = spp->pslc_pgs_per_blk;
		} else {
			pl->blk[i].nand_type = CELL_MODE;
			pl->blk[i].used_pgs = spp->pgs_per_blk;
		}
#endif
	}
}

static void ssd_remove_nand_plane(struct nand_plane *pl)
{
	int i;

	for (i = 0; i < pl->nblks; i++)
		ssd_remove_nand_blk(&pl->blk[i]);

	kfree(pl->blk);
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
	int i;
	lun->npls = spp->pls_per_lun;
	lun->pl = kmalloc(sizeof(struct nand_plane) * lun->npls, GFP_KERNEL);
	for (i = 0; i < lun->npls; i++) {
		ssd_init_nand_plane(&lun->pl[i], spp);
	}
	lun->next_lun_avail_time = 0;
	INIT_LIST_HEAD(&lun->cmd_queue_head);
	lun->migrating_etime = 0;
	lun->migrating = false;
	lun->cmd_queue_depth = 0;
	lun->max_cmd_queue_depth = 0;
	lun->busy = false;
}

static void ssd_remove_nand_lun(struct nand_lun *lun)
{
	int i;

	for (i = 0; i < lun->npls; i++)
		ssd_remove_nand_plane(&lun->pl[i]);

	kfree(lun->pl);
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
	int i;
	ch->nluns = spp->luns_per_ch;
	ch->lun = kmalloc(sizeof(struct nand_lun) * ch->nluns, GFP_KERNEL);
	for (i = 0; i < ch->nluns; i++) {
		ssd_init_nand_lun(&ch->lun[i], spp);
	}

	ch->perf_model = kmalloc(sizeof(struct channel_model), GFP_KERNEL);
	chmodel_init(ch->perf_model, spp->ch_bandwidth);

	/* Add firmware overhead */
	ch->perf_model->xfer_lat += (spp->fw_ch_xfer_lat * UNIT_XFER_SIZE / KB(4));
}

static void ssd_remove_ch(struct ssd_channel *ch)
{
	int i;

	kfree(ch->perf_model);

	for (i = 0; i < ch->nluns; i++)
		ssd_remove_nand_lun(&ch->lun[i]);

	kfree(ch->lun);
}

static void ssd_init_pcie(struct ssd_pcie *pcie, struct ssdparams *spp)
{
	pcie->perf_model = kmalloc(sizeof(struct channel_model), GFP_KERNEL);
	chmodel_init(pcie->perf_model, spp->pcie_bandwidth);
}

void ssd_init(struct ssd *ssd, struct ssdparams *spp, uint32_t cpu_nr_dispatcher)
{
	uint32_t i;
	/* copy spp */
	ssd->sp = *spp;

	/* initialize conv_ftl internal layout architecture */
	ssd->ch = kmalloc(sizeof(struct ssd_channel) * spp->nchs, GFP_KERNEL); // 40 * 8 = 320
	for (i = 0; i < spp->nchs; i++) {
		ssd_init_ch(&(ssd->ch[i]), spp);
	}

	/* Set CPU number to use same cpuclock as io.c */
	ssd->cpu_nr_dispatcher = cpu_nr_dispatcher;

	ssd->pcie = kmalloc(sizeof(struct ssd_pcie), GFP_KERNEL);
	ssd_init_pcie(ssd->pcie, spp);

	ssd->write_buffer = kmalloc(sizeof(struct buffer), GFP_KERNEL);
	buffer_init(ssd->write_buffer, spp->write_buffer_size);

#if (BASE_SSD == CONZONE_PROTOTYPE)
	ssd_init_l2pcache(&ssd->l2pcache);
#endif

	return;
}

void ssd_remove(struct ssd *ssd)
{
	uint32_t i;
	NVMEV_INFO("-------------MISAO SSD statistic info-----------\n");
	struct ppa ppa;
	for (int ch = 0; ch < ssd->sp.nchs; ch++) {
		for (int lun = 0; lun < ssd->sp.luns_per_ch; lun++) {
			ppa.ppa = 0;
			ppa.zms.ch = ch;
			ppa.zms.lun = lun;

			struct nand_lun *lunp = get_lun(ssd, &ppa);
			NVMEV_INFO("[Channel %d Lun %d] [Max CMD Queue Depth] %llu\n", ch, lun,
					   lunp->max_cmd_queue_depth);
			struct nand_cmd *cmd =
				list_first_entry_or_null(&lunp->cmd_queue_head, struct nand_cmd, entry);
			while (cmd) {
				list_del_init(&cmd->entry);
				kfree(cmd);
				cmd = list_first_entry_or_null(&lunp->cmd_queue_head, struct nand_cmd, entry);
			}
		}
	}

	buffer_remove(ssd->write_buffer);
	kfree(ssd->write_buffer);
	if (ssd->pcie) {
		kfree(ssd->pcie->perf_model);
		kfree(ssd->pcie);
	}

	for (i = 0; i < ssd->sp.nchs; i++) {
		ssd_remove_ch(&(ssd->ch[i]));
	}

	kfree(ssd->ch);

#if (BASE_SSD == CONZONE_PROTOTYPE)
	ssd_remove_l2pcache(&ssd->l2pcache);
#endif
}

uint64_t ssd_advance_pcie(struct ssd *ssd, uint64_t request_time, uint64_t length)
{
	struct channel_model *perf_model = ssd->pcie->perf_model;
	return chmodel_request(perf_model, request_time, length);
}

/* Write buffer Performance Model
  Y = A + (B * X)
  Y : latency (ns)
  X : transfer size (4KB unit)
  A : fw_wbuf_lat0
  B : fw_wbuf_lat1 + pcie dma transfer
*/
uint64_t ssd_advance_write_buffer(struct ssd *ssd, uint64_t request_time, uint64_t length)
{
	uint64_t nsecs_latest = request_time;
	struct ssdparams *spp = &ssd->sp;

	nsecs_latest += spp->fw_wbuf_lat0;
	nsecs_latest += spp->fw_wbuf_lat1 * DIV_ROUND_UP(length, KB(4));

	nsecs_latest = ssd_advance_pcie(ssd, nsecs_latest, length);

	return nsecs_latest;
}

static bool lun_getstime(struct nand_lun *lun, struct nand_cmd *ncmd, uint64_t ncmd_stime)
{
#if (BASE_SSD == CONZONE_PROTOTYPE)
	// Clear the current completed requests
	struct nand_cmd *cmd, *next_cmd;
	list_for_each_entry_safe(cmd, next_cmd, &lun->cmd_queue_head, entry)
	{
		if (cmd->ctime < ncmd_stime) {
			list_del(&cmd->entry);
			lun->cmd_queue_depth--;
			kfree(cmd);
		}
	}
	if (ncmd_stime > lun->migrating_etime)
		lun->migrating = false;

	bool preemp = false;
	if (ncmd->type != MIGRATE_IO && lun->migrating) {
		list_for_each_entry_safe(cmd, next_cmd, &lun->cmd_queue_head, entry)
		{
			if (cmd->type == MIGRATE_IO && cmd->stime > ncmd_stime &&
				cmd->ppa.zms.blk != ncmd->ppa.zms.blk) {
				list_add_tail(&ncmd->entry, &cmd->entry);
				lun->cmd_queue_depth++;
				lun->max_cmd_queue_depth = max(lun->max_cmd_queue_depth, lun->cmd_queue_depth);
				ncmd->stime = cmd->stime;
				preemp = true;
				break;
			}
		}
	}

	if (!preemp) {
		list_add_tail(&ncmd->entry, &lun->cmd_queue_head);
		lun->cmd_queue_depth++;
		lun->max_cmd_queue_depth = max(lun->max_cmd_queue_depth, lun->cmd_queue_depth);
		ncmd->stime = max(lun->next_lun_avail_time, ncmd_stime);

		if (ncmd->type == MIGRATE_IO && !lun->migrating) {
			lun->migrating = true;
		}
	}

	NVMEV_CONZONE_PRINT_TIME(
		"%s: preemp %d current queue depth %llu lun next avaial time %llu ncmd submit time "
		"%llu ncmd stime %llu\n",
		__func__, preemp, lun->cmd_queue_depth, lun->next_lun_avail_time, ncmd_stime, ncmd->stime);
	return preemp;
#else
	ncmd->stime = max(lun->next_lun_avail_time, ncmd_stime);
	return false;
#endif
}

static void lun_update(struct nand_lun *lun, struct nand_cmd *ncmd, bool preemp, uint64_t cmd_etime)
{
#if (BASE_SSD == CONZONE_PROTOTYPE)
	if (preemp) {
		struct nand_cmd *cmd, *next_cmd;
		bool delay = false;
		list_for_each_entry_safe(cmd, next_cmd, &lun->cmd_queue_head, entry)
		{
			if (delay) {
				cmd->stime += cmd_etime - ncmd->stime;
				cmd->ctime += cmd_etime - ncmd->stime;

				if (cmd->type == MIGRATE_IO) {
					lun->migrating_etime = max(lun->migrating_etime, cmd->ctime);
				}
			}
			if (cmd == ncmd) {
				delay = true;
			}
		}
		lun->next_lun_avail_time += cmd_etime - ncmd->stime;
	} else {
		lun->next_lun_avail_time = cmd_etime;
	}

#else
	lun->next_lun_avail_time = cmd_etime;
#endif
	ncmd->ctime = cmd_etime;
	if (ncmd->type == MIGRATE_IO) {
		lun->migrating_etime = max(lun->migrating_etime, ncmd->ctime);
	}

	NVMEV_CONZONE_PRINT_TIME(
		"%s: preemp %d lun next avaial time %llu complete time %llu ncmd ctime %llu\n", __func__,
		preemp, lun->next_lun_avail_time, cmd_etime, ncmd->ctime);
}

uint64_t ssd_advance_nand(struct ssd *ssd, struct nand_cmd *ncmd)
{
	int c = ncmd->cmd;
	uint64_t cmd_stime = (ncmd->stime == 0) ? __get_ioclock(ssd) : ncmd->stime;
	uint64_t nand_stime, nand_etime;
	uint64_t chnl_stime, chnl_etime;
	uint64_t remaining, xfer_size, completed_time;
	struct ssdparams *spp;
	struct nand_lun *lun;
	struct ssd_channel *ch;
	struct nand_block *blk;
	struct ppa *ppa = &ncmd->ppa;
	uint32_t cell;
	int cell_mode;
	int preemp;
	NVMEV_DEBUG("SSD: %p, Enter stime: %lld, ch %d lun %d blk %d page %d command %d ppa 0x%llx\n",
				ssd, ncmd->stime, ppa->g.ch, ppa->g.lun, ppa->g.blk, ppa->g.pg, c, ppa->ppa);

	if (ppa->ppa == UNMAPPED_PPA) {
		NVMEV_ERROR("Error ppa 0x%llx\n", ppa->ppa);
		return cmd_stime;
	}

	spp = &ssd->sp;
	lun = get_lun(ssd, ppa);
	ch = get_ch(ssd, ppa);
	blk = get_blk(ssd, ppa);
	cell = get_cell(ssd, ppa);
	cell_mode = blk->nand_type;
	remaining = ncmd->xfer_size;
	if (ncmd->type == MAP_READ_IO) {
		cell_mode = CELL_MODE_SLC;
	}
	if (cell_mode == CELL_MODE_SLC) {
		cell = 0;
	}

	switch (c) {
	case NAND_READ:
		/* read: perform NAND cmd first */
		preemp = lun_getstime(lun, ncmd, cmd_stime);
		nand_stime = ncmd->stime;

		if (ncmd->xfer_size == 4096) {
			nand_etime = nand_stime + spp->pg_4kb_rd_lat[cell_mode][cell];
		} else {
			nand_etime = nand_stime + spp->pg_rd_lat[cell_mode][cell];
		}

		/* read: then data transfer through channel */
		chnl_stime = nand_etime;

		while (remaining) {
			xfer_size = min(remaining, (uint64_t)spp->max_ch_xfer_size);
			chnl_etime = chmodel_request(ch->perf_model, chnl_stime, xfer_size);

			if (ncmd->interleave_pci_dma) { /* overlap pci transfer with nand ch transfer*/
				completed_time = ssd_advance_pcie(ssd, chnl_etime, xfer_size);
			} else {
				completed_time = chnl_etime;
			}

			remaining -= xfer_size;
			chnl_stime = chnl_etime;
		}

		lun_update(lun, ncmd, preemp, chnl_etime);
		break;
	case NAND_WRITE:
		/* write: transfer data through channel first */
		preemp = lun_getstime(lun, ncmd, cmd_stime);
		chnl_stime = ncmd->stime;

		chnl_etime = chmodel_request(ch->perf_model, chnl_stime, ncmd->xfer_size);

		/* write: then do NAND program */
		nand_stime = chnl_etime;
		nand_etime = nand_stime + spp->pg_wr_lat[cell_mode];
		lun_update(lun, ncmd, preemp, nand_etime);
		completed_time = nand_etime;
		break;

	case NAND_ERASE:
		/* erase: only need to advance NAND status */
		preemp = lun_getstime(lun, ncmd, cmd_stime);
		nand_stime = ncmd->stime;
		nand_etime = nand_stime + spp->blk_er_lat;
		lun_update(lun, ncmd, preemp, nand_etime);
		completed_time = nand_etime;
		break;

	case NAND_NOP:
		/* no operation: just return last completed time of lun */
		nand_stime = max(lun->next_lun_avail_time, cmd_stime);
		lun->next_lun_avail_time = nand_stime;
		completed_time = nand_stime;
		break;

	default:
		NVMEV_ERROR("Unsupported NAND command: 0x%x\n", c);
		return 0;
	}

	NVMEV_CONZONE_PRINT_TIME("%s completed time %llu\n", __func__, completed_time);
	return completed_time;
}

uint64_t ssd_next_idle_time(struct ssd *ssd)
{
	struct ssdparams *spp = &ssd->sp;
	uint32_t i, j;
	uint64_t latest = __get_ioclock(ssd);

	for (i = 0; i < spp->nchs; i++) {
		struct ssd_channel *ch = &ssd->ch[i];

		for (j = 0; j < spp->luns_per_ch; j++) {
			struct nand_lun *lun = &ch->lun[j];
			latest = max(latest, lun->next_lun_avail_time);
		}
	}

	return latest;
}

void adjust_ftl_latency(int target, int lat)
{
/* TODO ..*/
#if 0
    struct ssdparams *spp;
    int i;

    for (i = 0; i < SSD_PARTITIONS; i++) {
        spp = &(g_conv_ftls[i].sp);
        NVMEV_INFO("Before latency: %d %d %d, change to %d\n", spp->pg_rd_lat, spp->pg_wr_lat, spp->blk_er_lat, lat);
        switch (target) {
            case NAND_READ:
                spp->pg_rd_lat = lat;
                break;

            case NAND_WRITE:
                spp->pg_wr_lat = lat;
                break;

            case NAND_ERASE:
                spp->blk_er_lat = lat;
                break;

            default:
                NVMEV_ERROR("Unsupported NAND command\n");
        }
        NVMEV_INFO("After latency: %d %d %d\n", spp->pg_rd_lat, spp->pg_wr_lat, spp->blk_er_lat);
    }
#endif
}
