from pyspark.sql import SparkSession
from  pyspark.sql.functions import udf
import sys



def cate2_price_bin(cate2,price):

def save_squence_data(dt):
    spark = SparkSession.builder.appName("diversity").enableHiveSupport().getOrCreate()
    text_rdd = spark.sparkContext.textFile("/home/hdp_ubu_zhuanzhuan/resultdata/zhangjie/item_cate2price_disc/")
    cate2_dict = {}
    lines = text_rdd.collect()
    for line in lines:



    data = spark.sql(f'''      
        add JAR viewfs://58-cluster/home/hdp_58dp/udf/4.15_time_bucket.jar;
        CREATE TEMPORARY FUNCTION timebucket AS 'com.zhuanzhuan.spirit.ctr.timebucket';
        create temporary function getCate2PriceDist as 'com.zhuanzhuan.spirit.ctr.getCate2PriceDistUdf';
        
        
        select info_id,t0.logid,pctr,label,price,cate1,cate2,cate3,brand_id,model_id, color_id,quality_id,ram_mem_id, cate2_price_bin,
        pricelist,timebinlist,clickranklist,cate1list,cate2list,cate3list,brandlist,modellist,ram_mem_idlist,colorlist,qualitylist,pricebinlist from (
        select info_id,context['logid'] as logid,pctr,label,price,cate1,cate2,cate3,brand_id,model_id, 
        context['color_id'] as color_id,context['quality_id'] as quality_id,context['ram_mem_id'] as ram_mem_id ,
        nvl(getCate2PriceDist(if(cate2=0,concat(cate1,'_',brand_id),cate2),price),0) as cate2_price_bin
        from hdp_zhuanzhuan_dm_rec.dm_rec_home_ctr_sample_feature_full_1d
        where  dt = '{dt}'
        and scene = 'homepage_metric'
        and context['first_from'] in ('homepage_rec','homepage_rec_personal')
        and length(info_id) > 10 )t0
        left join( 
            select * from hdp_zhuanzhuan_dm_rec.dm_rec_click_squence_1d
            where dt = '{dt}'
        )t1 on t0.logid = t1.logid
    ''')
    data.write.mode(saveMode='overwrite').csv(path=f'/home/hdp_ubu_zhuanzhuan/resultdata/xucang/squencedata/squence{dt}.csv')




if __name__ == '__main__':
    dt = sys.argv[1]
    print(dt)
    save_squence_data(dt=dt)