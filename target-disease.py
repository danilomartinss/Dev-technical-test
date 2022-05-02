#!/usr/bin/python3

import requests
import numpy as np
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
paths = [ftp_url+path+path_eva+ext, ftp_url+path+path_tgt+ext, ftp_url+path+path_dis+ext]

def get_files(path):
    print("path: " + path)
    result = dd.read_parquet(path, engine="pyarrow")
    return result
    
def process(eva, tgt, dis):
    # convert to pandas dataframe
    pd_eva = eva.compute()
    pd_tgt = tgt.compute()
    pd_dis = dis.compute()
    
    # calculate median per target-disease pair
    mdn = pd_eva.groupby(['targetId','diseaseId'])['score'].median().reset_index()
    print("Data frame with median score")
    print(mdn.head(20))

    # get top 3 scores per target-disease pair
    evasub = pd_eva.groupby(['targetId','diseaseId'])['score'].nlargest(3).reset_index()
    print("Dataframe with 3 greatest scores")
    print(evasub.head(10))
    
    # merge dataframes eva, disease and target and filter columns
    mdn_tgt = mdn.merge(pd_tgt, left_on='targetId', right_on='id').drop('id', axis=1)
    mdn_tgt_dis = mdn_tgt.merge(pd_dis, left_on='diseaseId', right_on='id').drop('id', axis=1)
    mdn_tgt_dis = mdn_tgt_dis[['diseaseId', 'targetId', 'score', 'approvedSymbol', 'name']]
    mdn_tgt_dis = mdn_tgt_dis.rename(columns={"score":"Median Score"})
    
    print("Dataframe with all requisits")
    print(mdn_tgt_dis.head(10))
    
    return mdn_tgt_dis, evasub

if __name__ == '__main__':
    # main function
    with Pool(cpu_count()) as p:
        results = p.map(get_files, paths)
    eva = results[0]
    tgt = results[1]
    dis = results[2]
    df, evasub = process(eva, tgt, dis)
    
    # save files
    df.to_json("target-disease.json", orient="records", lines=True)
    evasub.to_json("evatop3.json", orient="records", lines=True)
