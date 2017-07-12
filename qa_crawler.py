import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import sys
import json
from pymongo import MongoClient
import re

def retrive(drink):
    return drink.get_text().encode("latin1", "replace").decode("big5", "replace")

# 連線資料庫
print("正在連線資料庫……")
uri = "mongodb://username:password@localhost/?authSource=admin"
client = MongoClient(uri)
db = client["medical_qa"]
collection = db["lists"]

# 取得最新題號
print("正在取得最新題號……")
latestQuestionTag = "#newQA a"
driver = webdriver.PhantomJS()
driver.get("http://sp1.hso.mohw.gov.tw/doctor/Index1.php")
pageSource = driver.page_source
driver.close()
soup = BeautifulSoup(pageSource, "lxml")

drink = soup.select(latestQuestionTag)[0]
if drink.has_attr("href"):
    latestQuestion = int(drink["href"][24:])
print("最新題號：#{}".format(latestQuestion))
if len(sys.argv) == 1:
    firstQuestion = 1
else:
    firstQuestion = int(sys.argv[1])

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
    res = requests.get("http://sp1.hso.mohw.gov.tw/doctor/All/ShowDetail.php?q_no=" + str(q_no))
    soup = BeautifulSoup(res.text, "lxml")

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
    except:
        # 如果沒有分類就跳過。
        pass
    else:
        # 如果有分類就存起來。
        match = re.search("相關分類 ：(.+)", retrive(drink))
        qa["type"] = match.group(1)

    # 存入資料庫
    lists = db.lists
    if len(qa) != 0:
        collection.replace_one({"no": qa["no"]}, qa)
