"""「衛生福利部【台灣ｅ院】醫療諮詢服務」的爬蟲程式

這個程式會從「衛生福利部【台灣ｅ院】醫療諮詢服務」爬問答集，並存到資料庫中。

"""

import getopt
import sys
import re
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
import requests

def retrive(drink):
    """從bs4 select的結果取出文字，並把big5轉成utf-8。

    Args:
        drink: bs4 select的結果。

    Returns:
        str: 取出的文字。

    """
    return drink.get_text().encode("latin1", "replace").decode("big5", "replace")

# 處理命令列參數
try:
    opts, args = getopt.getopt(sys.argv[1:], "s:e:", ["complete", "debug"])
except getopt.GetoptError:
    print("Usage: {} [-s|-e] [--debug]".format(sys.argv[0]))
    sys.exit(1);
firstQuestion = None
latestQuestion = None
complete = False
debug = False
for o, a in opts:
    if o in ("-s"):
        firstQuestion = int(a)
    if o in ("-e"):
        latestQuestion = int(a)
    if o in ("--complete"):
        complete = True
    if o in ("--debug"):
        debug = True

# 連線資料庫
print("正在連線資料庫……")
uri = "mongodb://username:password@localhost/?authSource=admin"
client = MongoClient(uri)
db = client["medical_qa"]
collection = db["lists"]

# 決定題號範圍
if firstQuestion is None:
    firstQuestion = 1
if not complete:
    firstQuestion = collection.find_one(sort=[("no", -1)])["no"] + 1
if latestQuestion is None:
    print("正在取得最新題號……")
    latestQuestionTag = "#newQA a"
    driver = webdriver.PhantomJS()
    driver.get("http://sp1.hso.mohw.gov.tw/doctor/Index1.php")
    pageSource = driver.page_source
    driver.close()
    soup = BeautifulSoup(pageSource, "html.parser")

    drink = soup.select(latestQuestionTag)[0]
    if drink.has_attr("href"):
        latestQuestion = int(drink["href"][24:])
    print("最新題號：#{}".format(latestQuestion))

# 爬問答
print("開始爬蟲……")
titleTag = "li.subject"
countTag = "li.count"
askerTag = "li.asker"
doctorTag = "li.doctor"
classTag = "li.doctor"
askTag = "li.ask"
ansTag = "li.ans"
typeTag = "li.count"
for q_no in range(firstQuestion, latestQuestion + 1):
    qa = {}
    print("\r爬蟲進度：#{}".format(q_no), end = "")
    while True:
        try:
            res = requests.get("http://sp1.hso.mohw.gov.tw/doctor/All/ShowDetail.php?q_no=" + str(q_no))
        except requests.exceptions.ConnectionError:
            # 連線失敗時，留在 while 迴圈內重試
            continue
        # 連線成功時，跳出 while 迴圈
        break
    soup = BeautifulSoup(res.text, "html.parser")

    # 若結果不存在就跳過
    if len(soup.select(titleTag)) == 0:
        continue

    # 問答編號
    qa["no"] = q_no

    # 標題
    drink = soup.select(titleTag)[0]
    match = re.search("#\d+ (.+)", retrive(drink))
    qa["title"] = match.group(1)

    # 閱覽次數
    drink = soup.select(countTag)[0]
    match = re.search("\d+", retrive(drink))
    qa["count"] = match.group()

    # 發問者
    drink = soup.select(askerTag)[0]
    match = re.search("發問者：([^／]+)", retrive(drink))
    qa["asker"] = match.group(1)

    # 醫院、科別、答復者
    drink = soup.select(doctorTag)[0]
    match = re.search("答復者：([^,]+)", retrive(drink))
    doctorList = match.group(1).split("／")
    if len(doctorList) != 3:
        qa["class"] = doctorList[0]
        qa["doctor"] = doctorList[1]
    else:
        qa["hospital"] = doctorList[0]
        qa["class"] = doctorList[1]
        qa["doctor"] = doctorList[2]

    # 詢問內容
    drink = soup.select(askTag)[0]
    qa["ask"] = retrive(drink)

    # 答復內容
    drink = soup.select(ansTag)[0]
    qa["ans"] = retrive(drink)

    # 相關分類
    try:
        # 不一定會有分類。
        drink = soup.select(typeTag)[1]
    except IndexError:
        # 如果沒有分類就跳過。
        pass
    else:
        # 如果有分類就存起來。
        match = re.search("相關分類 ：(.+)", retrive(drink))
        qa["type"] = match.group(1)

    if debug:
        print("\n{}".format(qa))
    else:
        # 存入資料庫
        lists = db.lists
        if len(qa) != 0:
            collection.replace_one({"no": qa["no"]}, qa, upsert=True)

# 提示使用者爬蟲已完成
print("\n爬蟲已完成！")
