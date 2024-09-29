#include <stdio.h>

char units_upper[3] = {'K','M','G'};
char units_lower[3] = {'k','m','g'};

long long int Log2(long long int num)
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

long long int gcd(long long int a,long long int b)
{
    return a%b?gcd(b,a%b):b;
}

long long int lcm(long long int a,long long int b)
{
    return a*b/gcd(a,b);
}

int get_ui(char c)
{
    for(int i = 0; i < 3;i++)
    {
        if(c==units_lower[i] || c==units_upper[i]) return i;
    }
    return -1;
}

void normalized_sz(long long int *sz, int *ui,char *unit)
{
    int tsz = *sz;
    int tui = *ui;
    if(tui>=2) return;
    while(tui<=1 && tsz%1024==0)
    {
        tui++;
        tsz = tsz/1024;
    }
    *sz = tsz;
    *ui = tui;
    *unit = units_upper[tui];
    return;
}

long long int mymod(long long int asz,int a_ui,long long int bsz,int b_ui)
{
    if(a_ui < b_ui)
    {
        printf("a should be larger than b!\n");
        return 0;
    }

    asz = asz << (10*(a_ui-b_ui));
    return asz%bsz;
}

long long int myadd(long long int asz,int a_ui,long long int bsz,int b_ui,int *rui, char *runit)
{
    int min_ui = a_ui>b_ui?b_ui:a_ui;
    long long int ret;
    asz = asz << (10*(a_ui-min_ui));
    bsz = bsz << (10*(b_ui-min_ui));
    ret = asz + bsz;
    normalized_sz(&ret,&min_ui,runit);
    *rui = min_ui;
    return ret;
}

int main()
{
    char blk_unit;
    int blk_ui;
    long long int blksz;
    long long int blksz_aligned;
    
    int dies_per_sblk;
    int pslc_blks;
    char storage_unit;
    int storage_ui;
    long long int storage_capacity;

    long long int sblksz;
    long long int zonesz;
    long long int sz_lcm;

    char pslc_rsv_unit = 'K';
    int pslc_ui;
    long long int pslc_rsv;

    int n_rsv_ui = 1;
    long long int nvme_rsv = 1;

    char ret_unit = 'K';
    int ret_ui;
    long long int ret;

    printf("Please enter the block size(Unit: K,M,G):\n");
    scanf("%lld%c",&blksz,&blk_unit);
    printf("Please enter the number of dies per superblock:\n");
    scanf("%d",&dies_per_sblk);
    printf("Please enter the number of pslc blks:\n");
    scanf("%d",&pslc_blks);
    printf("Please enter the storage size(Unit: K,M,G):\n");
    scanf("%lld%c",&storage_capacity,&storage_unit);

    blk_ui = get_ui(blk_unit);
    storage_ui = get_ui(storage_unit);
    
    if(blk_ui == -1 || storage_ui==-1)
    {
        printf("Invalid units - blk %c storage %c\n",blk_unit,storage_unit);
        return 0;
    }

    normalized_sz(&blksz,&blk_ui,&blk_unit);
    normalized_sz(&storage_capacity,&storage_ui,&storage_unit);

    blksz_aligned = 1<<Log2(blksz);
    zonesz = blksz_aligned*dies_per_sblk;
    sblksz = blksz*dies_per_sblk;
    sz_lcm = lcm(sblksz,zonesz);
    printf("blksz %lld,blksz_aligned %lld, zonesz %lld, sblksz %lld, lcm %lld (%c)\n",blksz,blksz_aligned,zonesz,sblksz,sz_lcm,blk_unit);

    if(mymod(storage_capacity,storage_ui,sz_lcm,blk_ui))
    {
        storage_capacity = storage_capacity << (10*(storage_ui-blk_ui));
        storage_ui = blk_ui;
        storage_capacity = (storage_capacity/sz_lcm + 1)*sz_lcm;
        normalized_sz(&storage_capacity,&storage_ui,&storage_unit);
        printf("new storage sz %lld%c (ui %d)\n",storage_capacity,storage_unit,storage_ui);
    }

    long long int pslc_unaligned_space = (storage_capacity<<(10*(storage_ui-blk_ui))) / zonesz * (zonesz-sblksz);
    pslc_blks = (pslc_blks>=4?pslc_blks:4) + pslc_unaligned_space/(sblksz);

    printf("The number of rsv pslc blks should be :\n%d\n",pslc_blks);
    printf("The size of L2P cache shoule be:\nKB(%lld)\n",1024/((128<<10*(2-storage_ui))/storage_capacity));

    pslc_rsv = sblksz*pslc_blks;
    pslc_rsv_unit = blk_unit;
    pslc_ui = blk_ui;
    
    normalized_sz(&pslc_rsv,&pslc_ui,&pslc_rsv_unit);

    printf("The reserved space is:\n%lld%c\n",pslc_rsv,pslc_rsv_unit);

    ret = myadd(storage_capacity,storage_ui,pslc_rsv,pslc_ui,&ret_ui,&ret_unit);
    ret = myadd(ret,ret_ui,nvme_rsv,n_rsv_ui,&ret_ui,&ret_unit);

    printf("The memmap_size should be:\n");
    printf("%lld%c\n",ret,ret_unit);
    return 0;
}