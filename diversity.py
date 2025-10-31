from pyspark.sql import SparkSession
from  pyspark.sql.functions import udf,collect_list,col,avg,concat,lit
from pyspark.sql.types import DoubleType
import math
import sys



def diver_udf(cate_list):
    total_sum = 0
    exp_sum = 0
    n = len(cate_list)
    for i in range(n):
        for j in range(1,4):
            if i+j >= n:
                continue
            b = 1.0 / math.log(j + 1)
            total_sum += b
            if cate_list[i] != cate_list[i+j]:
                exp_sum += b
    return exp_sum/total_sum if total_sum != 0 else None

def get_diversity(startdt,enddt,abname):
    spark = SparkSession.builder.appName("diversity").enableHiveSupport().getOrCreate()

    data = spark.sql(f'''
      select a.dt as dt,ab,requestmark,pg,idx,nvl(end_cate,'Nll') as cate from (
                select token,pg,idx,info_id,requestmark,dt from 
                hdp_zhuanzhuan_dw_global.dw_server_pgidx_1d
                where dt between '{startdt}' and '{enddt}' )a 
        inner join (
            select distinct dt,info_id,
            if(cate3 = 0 ,if(cate2 = 0 ,cate1 ,cate2) ,cate3) as end_cate,brand_id
            from hdp_zhuanzhuan_dm_rec.exp_all_info_1d 
            where dt between '{startdt}' and '{enddt}'  )b on a.dt = b.dt and a.info_id=b.info_id
        inner join( 
            select DISTINCT dt,regexp_extract(datapool['abtest'],'(exp|base)[1-2]_{abname}',0) as ab, token
                from hdp_zhuanzhuan_dw_global.dw_log_server_action_1d 
                where dt between '{startdt}' and '{enddt}' 
                and token is not NULL
                and terminal in ('15', '16','20')
                and action = 'homepage_metric' and region = 'h'
                and datapool['abtest'] like '%{abname}%'  )c
        on a.dt = c.dt and a.token = c.token
    ''')

    data = data.orderBy(100*col('pg')+col('idx'))
    data_grouped = data.groupby('dt','ab','requestmark').agg(collect_list('cate').alias('cate_list'))
    diversity_udf = udf(diver_udf,DoubleType())
    diversity_df = data_grouped.withColumn("diversity", diversity_udf(col("cate_list")))
    ab_diversity = diversity_df.groupby('dt','ab').agg(avg('diversity').alias('diversity'))
    ab_diversity.select('dt','ab','diversity').write.mode('overwrite').saveAsTable(f'hdp_zhuanzhuan_dm_rec.request_diversity_1d')
    result = ab_diversity.select('dt','ab','diversity').groupby('ab').agg(avg('diversity').alias('diversity'))
    print(result.select('ab','diversity').show())


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('startdt', type=str,help='开始日期')
    # parser.add_argument('enddt', type=str, help='结束日期')
    # parser.add_argument('abname',type = str,help='实验名')
    # args = parser.parse_args()


    abname,startdt,enddt = sys.argv[1].split(',')
    print(abname,startdt,enddt )
    get_diversity(startdt=startdt, enddt = enddt,abname=abname)