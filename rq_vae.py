from pyspark.sql import SparkSession
from  pyspark.sql.functions import udf,collect_list,col,avg,concat,lit
from pyspark.sql.types import DoubleType
import math
import sys

def get_rq_vae_seq_data():
    spark = SparkSession.builder.appName("rq_vae_data").enableHiveSupport().getOrCreate()
    rq_data  = spark.sql("""
        select distinct key as info_id,split(value,',')[0] as vae0,split(value,',')[1] as vae1,split(value,',')[2] as vae2 from 
        hdp_zhuanzhuan_dm_rec.dm_rec_llm_brief_full_1d
        where dt = '${hivevar:dt1}'
        and `version` ='bgem3_semantic_ids'
    """)



if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('startdt', type=str,help='开始日期')
    # parser.add_argument('enddt', type=str, help='结束日期')
    # parser.add_argument('abname',type = str,help='实验名')
    # args = parser.parse_args()



    startdt,enddt = sys.argv[1].split(',')
