// SPDX-License-Identifier: GPL-2.0-only

#ifndef _NVMEVIRT_SSD_CONFIG_H
#define _NVMEVIRT_SSD_CONFIG_H

/* SSD Model */
#define INTEL_OPTANE 0
#define SAMSUNG_970PRO 1
#define ZNS_PROTOTYPE 2
#define KV_PROTOTYPE 3
#define WD_ZN540 4
#define ZMS_PROTOTYPE 5

/* SSD Type */
#define SSD_TYPE_NVM 0
#define SSD_TYPE_CONV 1
#define SSD_TYPE_ZNS 2
#define SSD_TYPE_KV 3
#define SSD_TYPE_ZNS_CONV 4  //conventional zone

/* Cell Mode */
#define CELL_MODE_UNKNOWN 0
#define CELL_MODE_SLC 1
#define CELL_MODE_MLC 2
#define CELL_MODE_TLC 3
#define CELL_MODE_QLC 4
#define MAX_CELL_MODE 5

/* Must select one of INTEL_OPTANE, SAMSUNG_970PRO, or ZNS_PROTOTYPE
 * in Makefile */

#if (BASE_SSD == INTEL_OPTANE)
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_NVM
#define NS_CAPACITY_0 (0)
#define NS_SSD_TYPE_1 NS_SSD_TYPE_0
#define NS_CAPACITY_1 (0)
#define MDTS (5)
#define CELL_MODE (CELL_MODE_UNKNOWN)

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#elif (BASE_SSD == KV_PROTOTYPE)
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_KV
#define NS_CAPACITY_0 (0)
#define NS_SSD_TYPE_1 NS_SSD_TYPE_0
#define NS_CAPACITY_1 (0)
#define MDTS (5)
#define CELL_MODE (CELL_MODE_MLC)

enum {
	ALLOCATOR_TYPE_BITMAP,
	ALLOCATOR_TYPE_APPEND_ONLY,
};

#define KV_MAPPING_TABLE_SIZE GB(1)
#define ALLOCATOR_TYPE ALLOCATOR_TYPE_APPEND_ONLY

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#elif (BASE_SSD == SAMSUNG_970PRO)
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_CONV
#define NS_CAPACITY_0 (0)
#define NS_SSD_TYPE_1 NS_SSD_TYPE_0
#define NS_CAPACITY_1 (0)
#define MDTS (6)
#define CELL_MODE (CELL_MODE_MLC)

#define SSD_PARTITIONS (4)
#define NAND_CHANNELS (8)
#define LUNS_PER_NAND_CH (2)
#define PLNS_PER_LUN (1)
#define FLASH_PAGE_SIZE KB(32)
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * 1)
#define BLKS_PER_PLN (8192)
#define BLK_SIZE (0) /*BLKS_PER_PLN should not be 0 */
static_assert((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

#define MAX_CH_XFER_SIZE KB(16) /* to overlap with pcie transfer */
#define WRITE_UNIT_SIZE (512)

#define NAND_CHANNEL_BANDWIDTH (800ull) //MB/s
#define PCIE_BANDWIDTH (3360ull) //MB/s

#define NAND_4KB_READ_LATENCY_LSB (35760 - 6000) //ns
#define NAND_4KB_READ_LATENCY_MSB (35760 + 6000) //ns
#define NAND_4KB_READ_LATENCY_CSB (0) //not used
#define NAND_4KB_READ_LATENCY_TSB (0) //not used
#define NAND_READ_LATENCY_LSB (36013 - 6000)
#define NAND_READ_LATENCY_MSB (36013 + 6000)
#define NAND_READ_LATENCY_CSB (0) //not used
#define NAND_READ_LATENCY_TSB (0) //not used
#define NAND_PROG_LATENCY (185000)
#define NAND_ERASE_LATENCY (0)

#define FW_4KB_READ_LATENCY (21500)
#define FW_READ_LATENCY (30490)
#define FW_WBUF_LATENCY0 (4000)
#define FW_WBUF_LATENCY1 (460)
#define FW_CH_XFER_LATENCY (0)
#define OP_AREA_PERCENT (0.07)

#define GLOBAL_WB_SIZE (NAND_CHANNELS * LUNS_PER_NAND_CH * ONESHOT_PAGE_SIZE * 2)
#define WRITE_EARLY_COMPLETION 1

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#elif (BASE_SSD == ZNS_PROTOTYPE)
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_ZNS
#define NS_CAPACITY_0 (0)
#define NS_SSD_TYPE_1 NS_SSD_TYPE_0
#define NS_CAPACITY_1 (0)
#define MDTS (6)
#define CELL_MODE (CELL_MODE_TLC)

#define SSD_PARTITIONS (1)
#define NAND_CHANNELS (8)
#define LUNS_PER_NAND_CH (16)
#define FLASH_PAGE_SIZE KB(64)
#define PLNS_PER_LUN (1) /* not used*/
#define DIES_PER_ZONE (1)

#if 0
/* Real device configuration. Need to modify kernel to support zone size which is not power of 2*/
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * 3)
#define ZONE_SIZE MB(96) /* kernal only support zone size which is power of 2 */
#else /* If kernel is not modified, use this config for just testing ZNS*/
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * 2)
#define ZONE_SIZE MB(32)
#endif
static_assert((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

#define MAX_CH_XFER_SIZE (FLASH_PAGE_SIZE) /* to overlap with pcie transfer */
#define WRITE_UNIT_SIZE (ONESHOT_PAGE_SIZE)

#define NAND_CHANNEL_BANDWIDTH (800ull) //MB/s
#define PCIE_BANDWIDTH (3200ull) //MB/s

#define NAND_4KB_READ_LATENCY_LSB (25485)
#define NAND_4KB_READ_LATENCY_MSB (25485)
#define NAND_4KB_READ_LATENCY_CSB (25485)
#define NAND_4KB_READ_LATENCY_TSB (0) //not used
#define NAND_READ_LATENCY_LSB (40950)
#define NAND_READ_LATENCY_MSB (40950)
#define NAND_READ_LATENCY_CSB (40950)
#define NAND_READ_LATENCY_TSB (0) //not used
#define NAND_PROG_LATENCY (1913640)
#define NAND_ERASE_LATENCY (0)

#define FW_4KB_READ_LATENCY (37540 - 7390 + 2000)
#define FW_READ_LATENCY (37540 - 7390 + 2000)
#define FW_WBUF_LATENCY0 (0)
#define FW_WBUF_LATENCY1 (0)
#define FW_CH_XFER_LATENCY (413)
#define OP_AREA_PERCENT (0)

#define GLOBAL_WB_SIZE (NAND_CHANNELS * LUNS_PER_NAND_CH * ONESHOT_PAGE_SIZE * 2)
#define ZONE_WB_SIZE (0)
#define NR_SEQ_ZONE_WB (0)
#define NR_CONV_ZONE_WB (0)
#define WRITE_EARLY_COMPLETION 0

/* Don't modify followings. BLK_SIZE is caculated from ZONE_SIZE and DIES_PER_ZONE */
#define BLKS_PER_PLN 0 /* BLK_SIZE should not be 0 */
#define BLK_SIZE (ZONE_SIZE / DIES_PER_ZONE)
static_assert((ZONE_SIZE % DIES_PER_ZONE) == 0);

/* For ZRWA */
#define MAX_ZRWA_ZONES (0)
#define ZRWAFG_SIZE (0)
#define ZRWA_SIZE (0)
#define ZRWA_BUFFER_SIZE (0)

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#elif (BASE_SSD == WD_ZN540)
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_ZNS
#define NS_CAPACITY_0 (0)
#define NS_SSD_TYPE_1 NS_SSD_TYPE_0
#define NS_CAPACITY_1 (0)
#define MDTS (6)
#define CELL_MODE (CELL_MODE_TLC)

#define SSD_PARTITIONS (1)
#define NAND_CHANNELS (8)
#define LUNS_PER_NAND_CH (4)
#define PLNS_PER_LUN (1) /* not used*/
#define DIES_PER_ZONE (NAND_CHANNELS * LUNS_PER_NAND_CH)

#define FLASH_PAGE_SIZE KB(32)
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * 3)
/*In an emulator environment, it may be too large to run an application
  which requires a certain number of zones or more.
  So, adjust the zone size to fit your environment */
#define ZONE_SIZE GB(2ULL)

static_assert((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

#define MAX_CH_XFER_SIZE (FLASH_PAGE_SIZE) /* to overlap with pcie transfer */
#define WRITE_UNIT_SIZE (512)

#define NAND_CHANNEL_BANDWIDTH (450ull) //MB/s
#define PCIE_BANDWIDTH (3050ull) //MB/s

#define NAND_4KB_READ_LATENCY_LSB (50000)
#define NAND_4KB_READ_LATENCY_MSB (50000)
#define NAND_4KB_READ_LATENCY_CSB (50000)
#define NAND_4KB_READ_LATENCY_TSB (0) //not used
#define NAND_READ_LATENCY_LSB (58000)
#define NAND_READ_LATENCY_MSB (58000)
#define NAND_READ_LATENCY_CSB (58000)
#define NAND_READ_LATENCY_TSB (0) //not used
#define NAND_PROG_LATENCY (561000)
#define NAND_ERASE_LATENCY (0)

#define FW_4KB_READ_LATENCY (20000)
#define FW_READ_LATENCY (13000)
#define FW_WBUF_LATENCY0 (5600)
#define FW_WBUF_LATENCY1 (600)
#define FW_CH_XFER_LATENCY (0)
#define OP_AREA_PERCENT (0)

#define ZONE_WB_SIZE (10 * ONESHOT_PAGE_SIZE)
#define NR_SEQ_ZONE_WB (0)
#define NR_CONV_ZONE_WB (0)
#define GLOBAL_WB_SIZE (0)
#define WRITE_EARLY_COMPLETION 1

/* Don't modify followings. BLK_SIZE is caculated from ZONE_SIZE and DIES_PER_ZONE */
#define BLKS_PER_PLN 0 /* BLK_SIZE should not be 0 */
#define BLK_SIZE (ZONE_SIZE / DIES_PER_ZONE)
static_assert((ZONE_SIZE % DIES_PER_ZONE) == 0);

/* For ZRWA */
#define MAX_ZRWA_ZONES (0)
#define ZRWAFG_SIZE (0)
#define ZRWA_SIZE (0)
#define ZRWA_BUFFER_SIZE (0)

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#elif (BASE_SSD == ZMS_PROTOTYPE) 
#define NR_NAMESPACES 1

#define NS_SSD_TYPE_0 SSD_TYPE_ZNS
#define NS_CAPACITY_0 GB(0)
#define NS_SSD_TYPE_1 SSD_TYPE_ZNS_CONV
#define NS_CAPACITY_1 MB(128ULL)
#define MDTS (6)
#define CELL_MODE (CELL_MODE_TLC)

#define SSD_PARTITIONS (1)
#define NAND_CHANNELS (2)
#define LUNS_PER_NAND_CH (2)
#define PLNS_PER_LUN (1) /* not used*/
#define DIES_PER_ZONE (NAND_CHANNELS * LUNS_PER_NAND_CH)

#define FLASH_PAGE_SIZE KB(32)
#define ONESHOT_PAGE_SIZE (FLASH_PAGE_SIZE * (CELL_MODE) * (PLNS_PER_LUN))
/*In an emulator environment, it may be too large to run an application
  which requires a certain number of zones or more.
  So, adjust the zone size to fit your environment */

/*  ZONE_CAPACITY is caculated from BLK_SIZE and DIES_PER_ZONE */
static inline uint64_t Log2(uint64_t num)
{
	long long int orig = num;
	long long int ret = 0;
    while(num > 1)
    {
        num/=2;
        ret++;
    }
    if(orig != 1<<ret) ret++;
    return ret;
}

#define BLKS_PER_PLN 0 /* BLK_SIZE should not be 0 */
#define BLK_SIZE_KB (3072ULL) //3072ULL
#define BLK_SIZE KB(BLK_SIZE_KB)
#define BLK_SIZE_ALIGNED KB(1<<(Log2(BLK_SIZE_KB)))
#define ZONE_SIZE (BLK_SIZE_ALIGNED * DIES_PER_ZONE)

static_assert((BLK_SIZE % ONESHOT_PAGE_SIZE) == 0);

static_assert((ONESHOT_PAGE_SIZE % FLASH_PAGE_SIZE) == 0);

#define MAX_CH_XFER_SIZE FLASH_PAGE_SIZE /* to overlap with pcie transfer */
#define WRITE_UNIT_SIZE (512) 

//(450ull)  5939 2970 3200
#define NAND_CHANNEL_BANDWIDTH (1600ull) //MB/s
//3050
#define PCIE_BANDWIDTH (3000ull) //MB/s

/**
 * REFERENCE
 * slc: [ISSCC 2020] A 128Gb 1b/Cell 96-Word-Line-Layer 3D Flash Memory to Improve Random Read Latency with tPROG=75µs and tR=4µs
 * mlc: SAMSUNG_970_PRO
 * tlc: one-step program, prog_lat_us = 1e6×PageSize×3÷(ProgramThroughput/# of planes) [ISSCC 2024] A 1Tb Density 3b/Cell 3D-NAND Flash on a 2YY-Tier Technology with a 300MB/s Write Throughput
 * qlc: two-step program, prog_lat_us = 2×1e6×PageSize×4÷(ProgramThroughput/# of planes) [ISSCC 2024] A 280-Layer 1Tb 4b/cell 3D-NAND Flash Memory with a 28.5Gb/mm2 Areal Density and a 3.2GB/s High-Speed IO Rate
 */
#define SLC_NAND_PROG_LATENCY (75000)
#define MLC_NAND_PROG_LATENCY (185000)
#define TLC_NAND_PROG_LATENCY (937500)
#define QLC_NAND_PROG_LATENCY (6098000)

#define SLC_NAND_READ_LATENCY_LSB (20000)

/*mlc & tlc : samsung 970 & zn540*/
#define MLC_NAND_READ_LATENCY_LSB (36013 - 6000)
#define MLC_NAND_READ_LATENCY_MSB (36013 + 6000)

#define TLC_NAND_READ_LATENCY_LSB (32000)
#define TLC_NAND_READ_LATENCY_MSB (32000)
#define TLC_NAND_READ_LATENCY_CSB (32000)

#define QLC_NAND_READ_LATENCY_LSB (85000)
#define QLC_NAND_READ_LATENCY_MSB (85000)
#define QLC_NAND_READ_LATENCY_CSB (85000)
#define QLC_NAND_READ_LATENCY_TSB (85000)

#define NAND_ERASE_LATENCY (3500000)
#define NAND_ERASE_LATENCY_AGED (10000000) //not used

// #define FW_4KB_READ_LATENCY (20000)
// #define FW_READ_LATENCY (13000)
#define FW_4KB_READ_LATENCY (0)
#define FW_READ_LATENCY (0)
#define FW_WBUF_LATENCY0 (5600)
#define FW_WBUF_LATENCY1 (600)
#define FW_CH_XFER_LATENCY (0)
#define OP_AREA_PERCENT (0)

#define ZONE_WB_SIZE (ONESHOT_PAGE_SIZE*LUNS_PER_NAND_CH*NAND_CHANNELS)
#define NR_SEQ_ZONE_WB (2)
#define NR_CONV_ZONE_WB (1)

#define GLOBAL_WB_SIZE (0)
#define WRITE_EARLY_COMPLETION 1

/* For L2P cache*/
enum {
  L2P_EVICTION_POLICY_NONE, //random evict
	L2P_EVICTION_POLICY_LRU,
};
enum{
  L2P_SEARCH_BITMAP,
  L2P_SEARCH_MULTIPLE,
};

#define L2P_ENTRY_SIZE (4) //4B
#define L2P_CACHE_SIZE KB(12) //small l2p cache
#define L2P_CACHE_HASH_SLOT (3) //# of l2p hash slots (3)
#define L2P_LOG_SIZE (ONESHOT_PAGE_SIZE) /*not used*/ //TODO
#define L2P_EVICT_POLICY L2P_EVICTION_POLICY_LRU
#define L2P_SEARCH_SCHMEME L2P_SEARCH_BITMAP
#define L2P_PREREAD (1023)
#define L2P_HYBRID_MAP 1
#define L2P_HYBRID_MAP_RESIDENT 1

enum {
  PAGE_MAP = 0, /* 4 KiB */
  CHUNK_MAP = 1, /* 4 MiB */
  ZONE_MAP = 2, /* zone */
  NUM_MAP,
};
#define CHUNK_SIZE MB(4ULL)
static const int map_type[] = {ZONE_MAP,CHUNK_MAP,PAGE_MAP};
#define MAP_GRAN(idx) (map_type[idx])

/* For pSLC area*/
#define pSLC_INIT_BLKS (36) //pSLC area size, unit: # of blks //36
static_assert(pSLC_INIT_BLKS >= 4); //for gc
#define RSV_SIZE (BLK_SIZE* NAND_CHANNELS * LUNS_PER_NAND_CH*pSLC_INIT_BLKS)
#define pSLC_ONESHOT_PAGE_SIZE KB(4)
static_assert((BLK_SIZE % pSLC_ONESHOT_PAGE_SIZE) == 0);
#define USER_SPACE_REDUCTION (0) //whether the pSLC buffer reduces the total configurable user space //TODO not used

#define ZONED_DEVICE (0) //to compare with unzoned-device

/* For ZRWA */
#define MAX_ZRWA_ZONES (0)
#define ZRWAFG_SIZE (0)
#define ZRWA_SIZE (0)
#define ZRWA_BUFFER_SIZE (0)

#define LBA_BITS (9)
#define LBA_SIZE (1 << LBA_BITS)

#endif
///////////////////////////////////////////////////////////////////////////

static const uint32_t ns_ssd_type[] = { NS_SSD_TYPE_0, NS_SSD_TYPE_1 };
static const uint64_t ns_capacity[] = { NS_CAPACITY_0, NS_CAPACITY_1 };

#define NS_SSD_TYPE(ns) (ns_ssd_type[ns])
#define NS_CAPACITY(ns) (ns_capacity[ns])

/* Still only support NR_NAMESPACES <= 2 */
static_assert(NR_NAMESPACES <= 2);

#define SUPPORTED_SSD_TYPE(type) \
	(NS_SSD_TYPE_0 == SSD_TYPE_##type || NS_SSD_TYPE_1 == SSD_TYPE_##type)

#endif
