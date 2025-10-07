import time
import csv
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo
from supabase import create_client, Client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SENDER = os.environ.get("SENDER")
APP_PASSWORD = os.environ.get("APP_PASSWORD")

print("SUPABASE_URL:", os.environ.get("SUPABASE_URL"))
print("SUPABASE_KEY:", os.environ.get("SUPABASE_KEY"))


supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "MarketData"  # your table name

BINANCE_URL = "https://api.binance.com/api/v3/depth?symbol=PAXGTRY&limit=5"
FOREKS_URL = "https://www.foreks.com/altin-kuru/"
ONS_TO_GRAM = 31.1035  

def fetch_binance_paxg():
    resp = requests.get(BINANCE_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    ask = float(data["asks"][0][0])
    bid = float(data["bids"][0][0])
    return ask, bid

def fetch_foreks_spot(driver, wait):
    ask_elem = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "span[data-field='o14_a']")))
    bid_elem = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "span[data-field='o14_b']")))

    ask = float(ask_elem.text.replace(".", "").replace(",", "."))
    bid = float(bid_elem.text.replace(".", "").replace(",", "."))

    ask_gram = ask * ONS_TO_GRAM
    bid_gram = bid * ONS_TO_GRAM
    return ask_gram, bid_gram

def insert_to_supabase(row):
    """
    Insert one row into Supabase table
    """
    data = {
        "paxg_ask_try": row["paxg_ask"],
        "paxg_bid_try": row["paxg_bid"],
        "ons_gold_ask_try": row["gold_spot_ask_per_gram"],
        "ons_gold_bid_try": row["gold_spot_bid_per_gram"],
        "data_source": "Binance+Foreks",
        "paxg_timestamp": datetime.fromtimestamp(row["timestamp"], tz=ZoneInfo("Europe/Istanbul")).isoformat(),  # can convert int to timestamp if needed
        "ons_gold_timestamp": datetime.fromtimestamp(row["timestamp"], tz=ZoneInfo("Europe/Istanbul")).isoformat()  # same as above
    }

    response = supabase.table("MarketData").insert(data).execute()

    if response.data is None or len(response.data) == 0:
        print("Insert may have failed:", response)
    else:
        print("Row inserted successfully:", response.data)



def send_email(SENDER, TO, APP_PASSWORD, NO, TIME, SPREAD, PAXG_ASK, PAXG_BID, GOLD_ASK, GOLD_BID, IS_ARB_OPPORTUNITY_ENDED):

    msg = EmailMessage()
    msg["Subject"] = f"#{NO} Arbitraj Fırsatı Oluştu -> {SPREAD:.2f}% -> {TIME}" if not IS_ARB_OPPORTUNITY_ENDED else f"#{NO} Arbitraj Fırsatı Sona Erdi -> {TIME} -> {SPREAD:.2f}%"
    msg["From"] = SENDER
    msg["To"] = TO
    msg.set_content(f"Spread is between {'Gold Spot Alış & PAXG Satış' if SPREAD>0 else 'PAXG Alış & Gold Spot Satış'} \nPAXG Alış: {PAXG_ASK}\nPAXG Satış: {PAXG_BID}\nGold Spot Alış: {GOLD_ASK}\nGold Spot Satış: {GOLD_BID}\n\nArbitraj Fırsatı: %{SPREAD:.2f}")

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SENDER, APP_PASSWORD)
        smtp.send_message(msg)

    print("Gönderildi!")

def main_loop(interval=5):
    chrome_options = Options()
    #print("*-")
    chrome_options.add_argument("--no-sandbox")
    #print("**--")
    driver = webdriver.Chrome(options=chrome_options)
    #print("***---")
    wait = WebDriverWait(driver, 10)
    #print("****----")
    driver.get(FOREKS_URL)
    #print("*****-----")

    NO = 0
    arb_opportunity = False
    prev_opportunity_spread = 0.0
    prev_arb_opportunity = False
    while True:
        print("I am here")
        try:
            ts = int(time.time())
            paxg_ask, paxg_bid = fetch_binance_paxg()
            #print("I am here.")
            gold_ask, gold_bid = fetch_foreks_spot(driver, wait)
            #print("I am here..")
            row = {
                "timestamp": ts,
                "paxg_ask": paxg_ask,
                "paxg_bid": paxg_bid,
                "gold_spot_ask_per_gram": gold_ask,
                "gold_spot_bid_per_gram": gold_bid
            }
            #print("I am here...")

            print(row)
            insert_to_supabase(row)
            

            spread = ((paxg_ask - gold_bid) / gold_bid) * 100 if gold_bid > paxg_ask \
                        else ((paxg_bid - gold_ask) / gold_ask) * 100 if paxg_bid > gold_ask else \
                        (((paxg_bid + paxg_bid)/2 - (gold_ask + gold_bid)/2) / ((gold_ask + gold_bid)/2)) * 100
            
            if abs(spread) > 0.25:
                arb_opportunity = True
            else:
                arb_opportunity = False
            
            if not arb_opportunity and prev_arb_opportunity:
                prev_opportunity_spread = 0.0
                prev_arb_opportunity = arb_opportunity
                NO += 1
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="oguz.ozer2004@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=True
                )
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="arifevren3444@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=True
                )
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="deniztunug@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=True
                )

            if arb_opportunity and (not prev_arb_opportunity or (abs(spread) - abs(prev_opportunity_spread)) > 0.05):
                NO += 1
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="oguz.ozer2004@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=False
                )
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="arifevren3444@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=False
                )
                send_email(
                    SENDER="oguz.babadan.kaplar@gmail.com",
                    TO="deniztunug@gmail.com",
                    APP_PASSWORD=APP_PASSWORD,
                    NO=NO,
                    TIME=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    SPREAD=spread,
                    PAXG_ASK=paxg_ask,
                    PAXG_BID=paxg_bid,
                    GOLD_ASK=gold_ask,
                    GOLD_BID=gold_bid,
                    IS_ARB_OPPORTUNITY_ENDED=False
                )
                prev_opportunity_spread = spread
                prev_arb_opportunity = arb_opportunity
            
        except Exception as e:
            print("Error:", e)

        time.sleep(interval)

if __name__ == "__main__":
    main_loop()