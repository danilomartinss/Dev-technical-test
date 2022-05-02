#!/usr/bin/python3

import requests
import fastparquet
import pyarrow
import os
import pandas as pd
import dask
import dask.dataframe as dd
from multiprocessing import Pool, cpu_count
    
ftp_url='http://ftp.ebi.ac.uk/'
path='pub/databases/opentargets/platform/21.11/output/etl/parquet/'
ext = '*.parquet'
path_eva = 'evidence/sourceId%3Deva/'
path_tgt = 'targets/'
path_dis = 'diseases/'
parent_dir = os.getcwd()
directory = 'output'
outdir = os.mkdir(os.path.join(parent_dir, directory))
paths = [ftp_url+path+path_eva+ext, ftp_url+path+path_tgt+ext, ftp_url+path+path_dis+ext]

def get_files(path):
    print("path: " + path)
    result = dd.read_parquet(path, engine="pyarrow")
    return result
    
def process(eva, tgt, dis):
    # calculate median per target-disease pair
    eva['median'] = eva.map_partitions(lambda df: df.groupby(['targetId', 'diseaseId'])['score'].transform('median'), meta=eva).compute()

    # get top 3 scores per target-disease pair
    eva['medianScore] = eva.map_partitions(lambda df: df.groupby(['targetId', 'diseaseId'])['score'].transform(nlargest(3)), meta=eva).compute()
        
    # join target and disease dataframes
    jointdf = eva.merge(tgt, left_on='targetId', right_on='id').drop('id', axis=1).merge(dis, left_on='diseaseId', right_on='id').drop('id', axis=1)
    joint = jointdf[['targetId', 'approvedSymbol', 'diseaseId', 'name', 'score', 'medianScore']].drop_duplicates()

    # sort dataframe
    
    joint.sort_values('medianScore', ascending=True)
    
    #convert to json
    
    joinjson = jointdf.to_json(outdir, "joinTargetDisease.json")
    outjson = joint.to_json(outdir, "sortedAssociations.json")
    
    return outjson, joinjson

if _name_ == '_main_':
    with Pool(cpu_count()) as p:
    #eva, tgt, dis = get_files(path)
        results = p.map(get_files, paths)
    eva = results[0]
    tgt = results[1]
    dis = results[2]
    print("EVA")
    print("Target")
    print("Disease")
    print(dis.head())
    df = process(eva, tgt, dis)
    print("Final Dataframe")
