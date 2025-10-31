from pyexpat import features

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, collect_list, col, avg, concat, lit, get_json_object
from pyspark.sql.types import StringType
import math
from datetime import datetime, timedelta
import sys



def concat_ebd(token_ebd,click_str):
    placeholder = ','.join(['0'] * 1024)
    # 拼接
    inputs = (token_ebd+','+click_str).replace('mask',placeholder)
    assert len(inputs.split(','))%1024 == 0
    inputs += ','+','.join(['0']*(1024*11-len(inputs.split(','))))
    return inputs


def getdata(dt):
    # 直接计算
    current_date = datetime.strptime(dt, "%Y-%m-%d")
    previous_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
    spark = SparkSession.builder.appName("onerec_input").enableHiveSupport().getOrCreate()

    data = spark.sql(
    f'''
        select nvl(kkkk.value,'mask') as token_ebd,kkkk2.value as target_ebd,concat_ws(',',click_arr) as click_str,concat_ws(',',time_arr) as time_str,concat_ws(',',rank_arr) as rank_str from (
select token,target,collect_list(click_ebd) as click_arr,collect_list(cast(exp_time-clk_time as string)) as time_arr,collect_list(cast(click_rank as string )) as rank_arr from (
select token,target,nvl(gg4.value,'mask') as click_ebd,exp_time,clk_time,click_rank from (
select token,target,click_item,exp_time,clk_time,click_rank from 
hdp_zhuanzhuan_dm_global.dm_bagtab_click_list_1d
where dt = '{dt}' 
and click_rank <=10
and token != ''
and token is not null )gg1
left join (
	select key,value from 
	hdp_zhuanzhuan_dm_rec.dm_rec_zz_recall_strategy_full_1d
	where dt = '{dt}' 
	and scene = 'homepage'
	and strategy = 'bgem3-90'
)gg4 on gg1.click_item = gg4.key 
distribute by token,target,exp_time
sort by token,target,exp_time,cast(click_rank as int))ggg
group by token,target,exp_time )gggg
left join (
	select key,value from 
	hdp_zhuanzhuan_dm_rec.dm_rec_zz_recall_strategy_full_1d
	where dt = '{previous_date}'
	and scene = 'homepage'
	and strategy = 'token-bge-90'
)kkkk on gggg.token = kkkk.key
inner join (
	select key,value from 
	hdp_zhuanzhuan_dm_rec.dm_rec_zz_recall_strategy_full_1d
	where dt =  '{dt}' 
	and scene = 'homepage'
	and strategy = 'bgem3-90'
)kkkk2 on gggg.target = kkkk2.key
   ''')

    data = data.filter('target_ebd is not null').filter('token_ebd != "mask" or click_str !="mask" ')
    cct_udf = udf(concat_ebd,StringType())
    data = data.withColumn('features',cct_udf(col('token_ebd'),col('click_str')))
    data.select(['features','target_ebd']).write.csv(f'/home/hdp_ubu_zhuanzhuan/resultdata/xucang/bagtab/{dt}',mode='overwrite',sep = ',')








  





if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('startdt', type=str,help='开始日期')
    # parser.add_argument('enddt', type=str, help='结束日期')
    # parser.add_argument('abname',type = str,help='实验名')
    # args = parser.parse_args()
    dt = sys.argv[1]
    print(dt)
    getdata(dt)
