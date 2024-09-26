#include <stdio.h>
#include <string.h>

#define MAXN 100
#define MAXM 1000
#define MAXC 100


int main()
{
    int N,M,C;
    int u_msg[MAXM];
    int size_msg[MAXN][MAXM]; //1e5*4b = 0.4MB
    int down_msg[MAXN][MAXM]; //0.4MB
    //int value[MAXN][MAXC+1];
    int dp[MAXN][MAXM][MAXC+1]; //N 0-1 bag
    int ans[MAXC+1];

    int in = 0;
    int input_pointer[MAXM];
    memset(input_pointer,0,sizeof(int)*MAXM);
    memset(u_msg,0,sizeof(int)*MAXM);
    //memset(value,0,sizeof(int)*MAXN*(MAXC+1));

    scanf("%d%d%d",&N,&M,&C);
    while(in < N*M)
    {
        int pid,size,down;
        scanf("%d%d%d",&pid,&size,&down);
        int pointer = input_pointer[pid];
        size_msg[pid][pointer] = size;
        down_msg[pid][pointer] = down;
        input_pointer[pid]++;
        in++;
    }

    //1e4
    for(int i = 0;i < N;i++)
    {
        for(int j = 0; j < size_msg[i][0];j++)
        {
            dp[i][0][j] = -1;
        }
        for(int j = size_msg[i][0]; j <=C ;j++)
        {
            dp[i][0][j] = down_msg[i][0];
        }
    }

    //1e7
    for(int pid = 0;pid < N;pid++)
    {
        for(int i = 1;i < M;i++) //遍历物品
        {
            for(int j = 0;j <= C;j++)
            {
                if(j < size_msg[pid][i]) dp[pid][i][j] = dp[pid][i-1][j];
                else dp[pid][i][j] = max(dp[pid][i - 1][j], dp[pid][i - 1][j - size_msg[pid][i]] + down_msg[pid][i]);

                //value[pid][j] = max(value[pid][j],dp[pid][i][j]);
            }
        }
    }

    ans[0] = -1;
    for(int pid = 0;pid <= N; pid++)
    {
        for(int j = 0; j < M;j++)
        {
            if(dp[pid][j][0]==0)
            {
                ans[0] = 0;
                break;
            }
        }
        if(ans[0]==0) break;
    }

    for(int i = 1; i<=C; i++)
    {
        ans[i] = ans[i-1];
        //pid 0
        for(int j = 0;j < M;j++)
        {
            ans[i]
        }
    }

    printf("%d\n",ans[C]);


    return 0;
}