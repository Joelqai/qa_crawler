"""「衛生福利部【台灣ｅ院】醫療諮詢服務」的爬蟲程式

這個程式會從「衛生福利部【台灣ｅ院】醫療諮詢服務」爬問答集，並存到資料庫中。

命令列選項：
    -s 指定起始題號。
    -e 指定結束題號。
    --complete 從第一題爬到最新一題。會覆寫 -s 與 -e 的設定。
    --debug 印出爬蟲結果，不存入資料庫。
    --help 印出說明訊息

"""

import getopt
import sys
import re
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
import requests

def usage():
    print("Usage: {} [-s|-e] [--complete|--debug|--help]".format(sys.argv[0]))
    print("""    -s 指定起始題號。
    -e 指定結束題號。
    --complete 從第一題爬到最新一題。會覆寫 -s 與 -e 的設定。
    --debug 印出爬蟲結果，不存入資料庫。
    --help 印出說明訊息""")

# 處理命令列參數
try:
    opts, args = getopt.getopt(sys.argv[1:], "s:e:", ["complete", "debug"])
except getopt.GetoptError:
    usage()
    sys.exit(1)
firstQuestion = None
latestQuestion = None
complete = False
debug = False
for o, a in opts:
    if o in ("--help"):
        usage()
        sys.exit(0)
    if o in ("-s"):
        firstQuestion = int(a)
    if o in ("-e"):
        latestQuestion = int(a)
    if o in ("--complete"):
        complete = True
    if o in ("--debug"):
        debug = True

# 連線資料庫
# DB連不上線，看不到東西。
print("正在連線資料庫……")
uri = "mongodb://username:password@localhost/?authSource=admin"
client = MongoClient(uri)
db = client["medical_qa"]
collection = db["lists"]

# 決定題號範圍
if firstQuestion is None:
    firstQuestion = collection.find_one(sort=[("no", -1)])["no"] + 1
if complete:
    firstQuestion = 1
if latestQuestion is None or complete:
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
    res.encoding = "big5hkscs"
    soup = BeautifulSoup(res.text, "html.parser")

    # 若結果不存在就跳過
    if len(soup.select(titleTag)) == 0:
        continue

    # 問答編號
    qa["no"] = q_no

    # 標題
    drink = soup.select(titleTag)[0]
    match = re.search("#\d+ (.+)", drink.get_text())
    qa["title"] = match.group(1)

    # 閱覽次數
    drink = soup.select(countTag)[0]
    match = re.search("\d+", drink.get_text())
    qa["count"] = int(match.group())

    # 發問者
    drink = soup.select(askerTag)[0]
    match = re.search("發問者：([^／]+)", drink.get_text())
    qa["asker"] = match.group(1)

    # 醫院、科別、答復者
    drink = soup.select(doctorTag)[0]
    match = re.search("答復者：([^,]+)", drink.get_text())
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
    qa["ask"] = drink.get_text()

    # 答復內容
    drink = soup.select(ansTag)[0]
    qa["ans"] = drink.get_text()

    # 相關分類
    try:
        # 不一定會有分類。
        drink = soup.select(typeTag)[1]
    except IndexError:
        # 如果沒有分類就跳過。
        pass
    else:
        # 如果有分類就存起來。
        match = re.search("相關分類 ：(.+)", drink.get_text())
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
