from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import sys

def num_cate_udf(cate1list,cate2list,cate3list):
    cateset = set()
    for i,j,k in zip(cate1list.split('%'),cate2list.split('%'),cate3list.split('%')):
        if i == 'padding':
            break
        elif i == 'mask':
            continue
        else:
            cateset.add(i+j+k)
    return len(cateset)


def get_interest_cate(startdt,enddt,abname):


if __name__ == '__main__':
    abname, startdt, enddt = sys.argv[1].split(',')
    print(abname, startdt, enddt)
    get_diversity(startdt=startdt, enddt=enddt, abname=abname)

get_interest_cate = udf(num_cate_udf,IntegerType())




