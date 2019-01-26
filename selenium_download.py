from selenium import webdriver
from selenium.webdriver import ActionChains
from pymongo import MongoClient
from multiprocessing import Process
import time
import hashlib
from multiprocessing import Pool


conn=MongoClient("192.168.2.211",27017)#取关键字的数据库
db=conn.Runoob
my_set=db.Runoob

conn2=MongoClient("192.168.2.214",27017)#存数据的数据库
db2=conn2.test4
my_set2=db2.url_set2

# print(href)
# print(len(href))
def download_data(keyword,page):
    browser = webdriver.Chrome()
    browser.get("http://www.freefullpdf.com/#gsc.tab=0&gsc.q="+keyword+"&gsc.sort=&gsc.page="+str(page))
    node_list = browser.find_elements_by_xpath("//div[@class='gs-title']/a[@class='gs-title']")  # 获取href所在的节点(该节点包含啦文件url与title),返回list
    # print(node_list)
    print(len(node_list))

    # title=node_list.text
    # href=node_list.get_attribute('href')
    # node_list=[]

    if len(node_list)==11:
        MD5_href0 = hashlib.md5()
        MD5_href0.update(node_list[0].get_attribute('href').encode())
        MD5_href1 = hashlib.md5()
        MD5_href1.update(node_list[1].get_attribute('href').encode())
        MD5_href2 = hashlib.md5()
        MD5_href2.update(node_list[2].get_attribute('href').encode())
        MD5_href3 = hashlib.md5()
        MD5_href3.update(node_list[3].get_attribute('href').encode())
        MD5_href4 = hashlib.md5()
        MD5_href4.update(node_list[4].get_attribute('href').encode())
        MD5_href5 = hashlib.md5()
        MD5_href5.update(node_list[5].get_attribute('href').encode())
        MD5_href6 = hashlib.md5()
        MD5_href6.update(node_list[6].get_attribute('href').encode())
        MD5_href7 = hashlib.md5()
        MD5_href7.update(node_list[7].get_attribute('href').encode())
        MD5_href8 = hashlib.md5()
        MD5_href8.update(node_list[8].get_attribute('href').encode())
        MD5_href9 = hashlib.md5()
        MD5_href9.update(node_list[9].get_attribute('href').encode())

        my_set2.insert_many([{"title":node_list[0].text,"href":node_list[0].get_attribute('href'),"MD5_href":MD5_href0.hexdigest()},
                            {"title":node_list[1].text,"href":node_list[1].get_attribute('href'),"MD5_href":MD5_href1.hexdigest()},
                           {"title":node_list[2].text,"href":node_list[2].get_attribute('href'),"MD5_href":MD5_href2.hexdigest()},
                            {"title":node_list[3].text,"href":node_list[3].get_attribute('href'),"MD5_href":MD5_href3.hexdigest()},
                           {"title":node_list[4].text,"href":node_list[4].get_attribute('href'),"MD5_href":MD5_href4.hexdigest()},
                            {"title":node_list[5].text,"href":node_list[5].get_attribute('href'),"MD5_href":MD5_href5.hexdigest()},
                           {"title":node_list[6].text,"href":node_list[6].get_attribute('href'),"MD5_href":MD5_href6.hexdigest()},
                            {"title":node_list[7].text,"href":node_list[7].get_attribute('href'),"MD5_href":MD5_href7.hexdigest()},
                           {"title":node_list[8].text,"href":node_list[8].get_attribute('href'),"MD5_href":MD5_href8.hexdigest()},
                            {"title":node_list[9].text,"href":node_list[9].get_attribute('href'),"MD5_href":MD5_href9.hexdigest()}
                          ])
        print("insert_many is ok ")
        browser.close()

    elif node_list!=None:
        for node_one in node_list:
            MD5_href = hashlib.md5()
            MD5_href.update(node_list.get_attribute('href').encode())
            my_set2.insert_one({"title":node_one.text,"href":node_one.get_attribute('href'),"MD5_href":MD5_href.hexdigest()})
        print("insert_one is ok ")
        browser.close()
        return "end_page"
    else:
        print("download_over")
        # exit()
        browser.close()
    # print(node_list[0].text)
    # print(node_list[0].get_attribute('href'))

    # for i in node_list:
        # node_list.append(i.get_attribute('href'))
        # node_list.append(i)
        # print("i",i.get_attribute('href'))
        # print('j',i.text)

    # url_set = set(node_list)
    # url_list2=list(url_set)
    # # url_list2.remove(None)
    # for j in url_list2:
    #     print("j",j)

def download_keyword():
    while True:
        # print("2")
        try:
            data=my_set.find_one_and_update({"state":1},{"$set":{"state":0}})
            print(data)

            # time.sleep(5)
            if not data:
                print("return")
                return
            key_word = data["keyword"]
            print(key_word)
            # p=Pool(10)
            for page in range(1,20):
                print("开始下载第%s页" % (page))
                # p.apply_async(download_data,args=(key_word,page,))
                # print("开始爬取第%s页" % (page))
                page_state=download_data(key_word,page)
                print(page_state)
                if page_state=="end_page":
                    print("发现结束，提前跳出翻页")
                    break
                # time.sleep(1)
            # p.close()
            # p.join()
        except:
            print("出错")

# def pro():
#     trader = []
#     for i in range(10):
#         print("1")
#         pr = Process(target=download_keyword)
#         time.sleep(0.5)
#         pr.start()
#         trader.append(pr)
#     for i in trader:
#         i.join()

if __name__=="__main__":
    # key_word=input("请输入您需要爬取的数据关键字：")
    # pro()
    download_keyword()
    # download_data("java",1)
