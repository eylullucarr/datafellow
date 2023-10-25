from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import json
import boto3
import datetime

def lambda_handler(event,context):
    # Google Haberler sayfasını ziyaret etmek için kullanılacak URL'ler
    urls = [
        "https://news.google.com/topics/CAAqIggKIhxDQkFTRHdvSkwyMHZNREY2Ym1OZkVnSjBjaWdBUAE?hl=tr&gl=TR&ceid=TR%3Atr"
        
        
    ]

    for url in urls:
        # Sayfayı çek
        page = requests.get(url).text

        # BeautifulSoup kullanarak sayfayı parse et
        soup = BeautifulSoup(page, "html.parser")

        # Kategorileri çek
        result_category = soup.select("div .BPNpve")
        category = result_category[0].text if result_category else ""

        # Başlıkları çek
        result_tl = soup.select("article.IBr9hb .gPFEn")
        title = [t.text for t in result_tl]

        # Tarih ve zaman bilgilerini çek
        result_dt = soup.select("article.IBr9hb .UOVeFe .hvbAAd")
        timedate = [d["datetime"] for d in result_dt]

        # Kaynak URL'leri çek
        source_urls = []
        base_url = "https://news.google.com/"
        for i in soup.select("article.IBr9hb .WwrzSb"):
            ss = urljoin(base_url, i.get("href"))
            source_urls.append(ss)

        # Verileri toplu bir dizi içinde sakla
        data_length = len(title)
        multiple_array = []
        for index in range(data_length):
            data_entry = {
                "category": category,
                "title": title[index],
                "timedate": timedate[index],
                "source_url": source_urls[index],
                "index": (index + 1),
            }
            multiple_array.append(data_entry)
        

    # Tarih ve saat bilgisini al
    current_datetime = datetime.datetime.now()
    folder_date = current_datetime.strftime("%Y-%m-%d")
    folder_time = current_datetime.strftime("%H-%M")

    # JSON verisini hazırla
    json_data = json.dumps(multiple_array)

    # S3'ye JSON verisini yükle

    s3 = boto3.client("s3") 
    s3.put_object(
        Bucket="eylul.workspace.4",
        Key=f"google_news/exports/{folder_date}/{folder_time}.json",
        Body=json_data,
    )
    
    return {"statusCode": 200, "body": "Data inserted into the bucket.EYLO"}


