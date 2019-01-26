# 目标：http://www.freefullpdf.com/#gsc.tab=0内进行关键字的内容爬取
#  https://cse.google.com/cse/element/v1?
# 进行搜索网址变化（搜索java）：http://www.freefullpdf.com/#gsc.tab=0&gsc.q=python&gsc.sort=&gsc.page=1

import requests
import urllib.request
from urllib.parse import urlencode
import random
from lxml import etree
import json
from pymongo import MongoClient
import hashlib
from multiprocessing import Pool
import time
import urllib.parse
from Ip_and_Agent.ip_and_Agent import User_Agent,Inland_ip
import random

conn=MongoClient("192.168.8.211",27017)#取关键字的数据库
db=conn.Runoob
my_set=db.Runoob
my_set2=db.url_set3

# conn2=MongoClient("192.168.2.214",27017)#存数据的数据库
# db2=conn2.test4
# my_set2=db2.url_set3

# def download_rep(index):
#     try:
#         for i in range(index+1,10):
#             # print("title    :", dic["results"][i]["title"])
#             # print("url      :", urllib.parse.unquote(dic["results"][i]["url"]))
#             # print("con      :", dic["results"][i]["contentNoFormatting"])
#             MD5_url11 = hashlib.md5()
#             MD5_url11.update(urllib.parse.unquote(dic["results"][i]["url"]).encode())
#             my_set2.insert_one(
#                 {"title": dic["results"][i]["title"], "summary": dic["results"][i]["contentNoFormatting"],
#                  "url": urllib.parse.unquote(dic["results"][i]["url"]), "MD5_url": MD5_url11.hexdigest(), "state": 0,
#                  "type": " PDF/Adobe Acrobat"})
#             print(i)
#         print("insert_one2 is ok")
#     except:
#         print("重复数据")

index2=0
def download_data(keyword,page):#每天重新选择url
    # url="https://cse.google.com/cse/element/v1?rsz=filtered_cse&num=10&hl=en&source=gcsc&gss=.com&start="+str(page)+"&cx=001431978847466539083:xsldadcvvvo&q="+keyword+"%20filetype%3Apdf%20AND%20(%22et%20al%22%20OR%20%22DOI%22%20OR%20%22Patent%20N0%22%20OR%20%22EUROPEAN%20PATENT%20APPLICATION%22%20OR%20%22Working%20Paper%20No%22%20OR%20%22Elsevier%20Science%20B.V.%20All%22%20OR%20%22tel.archives-ouvertes%22%20OR%20%22current%20issue%20and%20full%20text%20archive%22)&safe=off&cse_tok=AKaTTZh8k8xgnRgZ6KMD45ZbBgTx:1545457100076&filter=1&as_oq=&sort=&callback=google.search.cse.api4850&nocache=1545457114615"
    url="https://cse.google.com/cse/element/v1?rsz=filtered_cse&num=10&hl=en&source=gcsc&gss=.com&start="+str(page)+"&cx=001431978847466539083:xsldadcvvvo&q="+keyword+"%20filetype%3Apdf%20AND%20(%22et%20al%22%20OR%20%22DOI%22%20OR%20%22Patent%20N0%22%20OR%20%22EUROPEAN%20PATENT%20APPLICATION%22%20OR%20%22Working%20Paper%20No%22%20OR%20%22Elsevier%20Science%20B.V.%20All%22%20OR%20%22tel.archives-ouvertes%22%20OR%20%22current%20issue%20and%20full%20text%20archive%22)&safe=off&cse_tok=AKaTTZinlGT_kfqxFMO6PK4SJJ_N:1545640689075&filter=1&as_oq=&sort=&callback=google.search.cse.api4332&nocache=1545641125112"
    head={"Accept":"*/*",
            # "Accept-Encoding":"gzip, deflate, br",
            # "Accept-Language":"zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Connection":"keep-alive",
            # "Cookie":"IDE=AHWqTUlnrmlFaFGCt64vm_WF8MyN82rrzejR7U715OzTKcaEsBh2MdgJalSHVJhz; DSID=NO_DATA",
            "Host":"cse.google.com",
            "Referer":"http://www.freefullpdf.com/",
            # "TE":"Trailers",
            "User-Agent":random.choice(User_Agent)}

    json_data=requests.get(url=url,headers=head)
    time.sleep(3)
    # print(len('/*O_o*/\ngoogle.search.cse.api4670'))
    # print(json_data.text[34:-2])
    # print(json_data.content)
    text=json_data.text[34:-2]
    global dic
    dic=json.loads(text)
    # for k in range(10):
    #     print("title    :",dic["results"][k]["title"])
    #     print("url      :",urllib.parse.unquote(dic["results"][k]["url"]))
    #     print("con      :",dic["results"][k]["contentNoFormatting"])

    try:
        MD5_url1 = hashlib.md5()
        MD5_url1.update(urllib.parse.unquote(dic["results"][0]["url"]).encode())
        MD5_url2 = hashlib.md5()
        MD5_url2.update(urllib.parse.unquote(dic["results"][1]["url"]).encode())
        MD5_url3 = hashlib.md5()
        MD5_url3.update(urllib.parse.unquote(dic["results"][2]["url"]).encode())
        MD5_url4 = hashlib.md5()
        MD5_url4.update(urllib.parse.unquote(dic["results"][3]["url"]).encode())
        MD5_url5 = hashlib.md5()
        MD5_url5.update(urllib.parse.unquote(dic["results"][4]["url"]).encode())
        MD5_url6 = hashlib.md5()
        MD5_url6.update(urllib.parse.unquote(dic["results"][5]["url"]).encode())
        MD5_url7 = hashlib.md5()
        MD5_url7.update(urllib.parse.unquote(dic["results"][6]["url"]).encode())
        MD5_url8 = hashlib.md5()
        MD5_url8.update(urllib.parse.unquote(dic["results"][7]["url"]).encode())
        MD5_url9 = hashlib.md5()
        MD5_url9.update(urllib.parse.unquote(dic["results"][8]["url"]).encode())
        MD5_url10 = hashlib.md5()
        MD5_url10.update(urllib.parse.unquote(dic["results"][9]["url"]).encode())

        my_set2.insert_many(
                            [{"title":dic["results"][0]["title"],"summary":dic["results"][0]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][0]["url"]),"MD5_url":MD5_url1.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                           {"title":dic["results"][1]["title"],"summary":dic["results"][1]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][1]["url"]),"MD5_url":MD5_url2.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                            {"title":dic["results"][2]["title"],"summary":dic["results"][2]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][2]["url"]),"MD5_url":MD5_url3.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                           {"title":dic["results"][3]["title"],"summary":dic["results"][3]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][3]["url"]),"MD5_url":MD5_url4.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                            {"title":dic["results"][4]["title"],"summary":dic["results"][4]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][4]["url"]),"MD5_url":MD5_url5.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                           {"title":dic["results"][5]["title"],"summary":dic["results"][5]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][5]["url"]),"MD5_url":MD5_url6.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                            {"title":dic["results"][6]["title"],"summary":dic["results"][6]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][6]["url"]),"MD5_url":MD5_url7.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                           {"title":dic["results"][7]["title"],"summary":dic["results"][7]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][7]["url"]),"MD5_url":MD5_url8.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                            {"title":dic["results"][8]["title"],"summary":dic["results"][8]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][8]["url"]),"MD5_url":MD5_url9.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"},
                            {"title":dic["results"][9]["title"],"summary":dic["results"][9]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][9]["url"]),"MD5_url":MD5_url10.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"}
                              ])
        print("insert_many is ok ")
    except:
            for i in range(10):
                try:
                    # print("title    :", dic["results"][i]["title"])
                    # print("url      :", urllib.parse.unquote(dic["results"][i]["url"]))
                    # print("con      :", dic["results"][i]["contentNoFormatting"])
                    MD5_url11 = hashlib.md5()
                    MD5_url11.update(urllib.parse.unquote(dic["results"][i]["url"]).encode())
                    my_set2.insert_one({"title":dic["results"][i]["title"],"summary":dic["results"][i]["contentNoFormatting"],"url":urllib.parse.unquote(dic["results"][i]["url"]),"MD5_url":MD5_url11.hexdigest(),"state":0,"type":" PDF/Adobe Acrobat"})
                    print(i)
                    print("insert_one is ok")
                except:
                    print("重复数据")

def download_keyword():
    while True:
        # try:
        data = my_set.find_one_and_update({"state":1.0}, {"$set": {"state":0}})
        # print(data)
        if not data:
            print("return")
            return
        key_word = data["keyword"]
        print(key_word)

        for page in range(0,100,10):
            print("开始下载第%s页" % ((page+10)//10))
            download_data(key_word, page)
        # except:
        #     print("取出关键字出错")

def poo():
    pool=Pool(10)
    for i in range(20):
        pool.apply_async(func=download_keyword)
    pool.close()
    pool.join()

if __name__=="__main__":
    # download_data("seed size variation",20)
    # download_keyword()
    poo()