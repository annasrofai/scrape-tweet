import pandas as pd
import snscrape.modules.twitter as sntwitter
from datetime import date, timedelta
import pytz
from pymongo import MongoClient
# initialize list of lists

# Membuat koneksi dengan MongoDB lokal
connection = MongoClient('localhost', 27017, tz_aware=True)
# Memilih Database pada MongoDB, bernama TwitterStream
db = connection.TwitterStream
# Memilih Collection pada Database, bernama tweets
collection_tweets_2 = db.tweetsScrapeAll

#  daftar kota dengan lon lat radius
# -----------------------------------------------------------------------------
data_kota = [
    ['yogyakarta', '-7.801617293741421', '110.3650173448773', '20km', ],
    ['semarang', '-6.991588850804953', '110.42405775941086', '20km', ],
    ['jakarta', '-6.2353027635939515', '106.82491683959708', '20km', ],
    ['bandung', '-6.9121504970150305', '107.62242767349865', '20km', ],
    ['surabaya', '-7.2961658051105545', '112.72987808952337', '20km', ],
]
df_kota = pd.DataFrame(
    data_kota, columns=['kota', 'longitude', 'latitude', 'radius'])
print("\nDaftar Kota/Lokasi:")
print("------------------------------------------------------------------------")
print(df_kota)
print("------------------------------------------------------------------------")
# -----------------------------------------------------------------------------

# daftar provider dan query provider
# -----------------------------------------------------------------------------
data_provider = [
    ['pt_f', 'firstmedia OR @FirstMediaCares', ],
    ['pt_b', 'biznet OR @BiznetHome OR BiznetNetworks', ],
    ['pt_i', 'indihome OR @IndiHomeCare', ],
]
df_provider = pd.DataFrame(
    data_provider, columns=['provider', 'keywords', ])
print("\nDaftar ISP:")
print("------------------------------------------------------------------------")
print(df_provider)
print("------------------------------------------------------------------------")
# -----------------------------------------------------------------------------

# pembuatan query waktu harian
# -----------------------------------------------------------------------------
today_gmt_7 = date.today()
yesterday_gmt_7 = date.today() - timedelta(1)
tommorow_gmt_7 = date.today() + timedelta(1)

today_str_gmt_7 = date.strftime(today_gmt_7, '%Y-%m-%d')
yesterday_str_gmt_7 = date.strftime(yesterday_gmt_7, '%Y-%m-%d')
tommorow_str_gmt_7 = date.strftime(tommorow_gmt_7, '%Y-%m-%d')

# today_str_gmt_7 = "2022-11-29"
# yesterday_str_gmt_7 = "2022-11-28"
# tommorow_str_gmt_7 = "2022-11-30"
# -----------------------------------------------------------------------------

for index, row in df_kota.iterrows():

    # pembuatan query lokasi
    # -----------------------------------------------------------------------------
    nama_kota_str = str(row["kota"])
    longitude_str = str(row["longitude"])
    latitude_str = str(row["latitude"])
    radius_str = str(row["radius"])
    geolocation_str = longitude_str + ", " + latitude_str + ", " + radius_str
    # -----------------------------------------------------------------------------

    for index, row in df_provider.iterrows():

        # pembuatan query
        # -----------------------------------------------------------------------------
        nama_provider = str(row['provider'])
        keywords = str(row['keywords'])
        # -----------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        print("\n------------------------------------------------------------------------")
        print("Mengambil Tweet pada terkait {nama_provider} yang berlokasi di {nama_kota} pada tanggal {today}".format(
            nama_provider=nama_provider, nama_kota=nama_kota_str, today=today_str_gmt_7))
        print("------------------------------------------------------------------------")
        # -----------------------------------------------------------------------------

        # scraping tweet dengan query
        # -----------------------------------------------------------------------------
        df_tweets = pd.DataFrame(
            sntwitter.TwitterSearchScraper(
                '{keywords} geocode:"{loc}" lang:id until:{tommorow} since:{yesterday} '.format(
                    keywords=keywords, loc=geolocation_str, tommorow=tommorow_str_gmt_7, yesterday=yesterday_str_gmt_7)
            ).get_items()
        )
        # -----------------------------------------------------------------------------

        if df_tweets.empty == False:
            # mengambil username
            # -----------------------------------------------------------------------------
            cols = ['user']
            df_tweets["username"] = df_tweets[cols].applymap(
                lambda x: x['username'])
            # -----------------------------------------------------------------------------

            # define kolom awal
            # -----------------------------------------------------------------------------
            df_tweets = df_tweets[['id', 'date',
                                   'username', 'content', 'lang']]
            df_tweets = df_tweets.rename(columns={
                'id': 'tweet_id', 'date': 'created', 'content': 'text', 'lang': 'language'})
            # -----------------------------------------------------------------------------

            # assign lokasi tweet dan nama provider
            # -----------------------------------------------------------------------------
            df_tweets['lokasi'] = nama_kota_str
            df_tweets['provider'] = nama_provider
            # -----------------------------------------------------------------------------

            # manipulasi dan memilih tweet berdasarkan waktu
            # -----------------------------------------------------------------------------
            # membuat kolom baru dan convert datetime ke GMT+7
            df_tweets["created_tz_indo"] = df_tweets['created'].dt.tz_convert(
                pytz.timezone('Asia/Jakarta'))
            # membuat kolom baru untuk mendapat tanggal pada GMT+7
            df_tweets["created_tz_indo_date"] = df_tweets['created_tz_indo'].dt.date
            # ubah dari object ke date
            df_tweets["created_tz_indo_date"] = pd.to_datetime(
                df_tweets["created_tz_indo_date"], format="%Y/%m/%d")
            # filter tweet dengan tanggal hari ini
            df_tweets = df_tweets.loc[(
                df_tweets['created_tz_indo_date'] == today_str_gmt_7)]
            # reset index
            df_tweets = df_tweets.reset_index(drop=True)
            # drop kolom datetime dan date GMT+7
            df_tweets.drop(
                df_tweets.columns[8], axis=1, inplace=True)
            df_tweets.drop(
                df_tweets.columns[7], axis=1, inplace=True)
            # print(df_tweets.dtypes)
            # print(df_tweets)
            # print(today_str_gmt_7)
            print(
                "\n------------------------------------------------------------------------")
            print(df_tweets)
            print(
                "------------------------------------------------------------------------")
            # -----------------------------------------------------------------------------
            if df_tweets.empty == False:
                # masukkan ke MongoDB
                # -----------------------------------------------------------------------------
                data_dict = df_tweets.to_dict("records")
                collection_tweets_2 = db.tweetsScrapeAll
                collection_tweets_2.insert_many(data_dict)
                # -----------------------------------------------------------------------------
            else:
                print(
                    "\n------------------------------------------------------------------------")
                print("Tidak ada Tweet terkait {nama_provider} dengan Lokasi {nama_kota} pada tanggal {today} setelah seleksi".format(
                    nama_provider=nama_provider, nama_kota=nama_kota_str, today=today_str_gmt_7))
                print(
                    "------------------------------------------------------------------------\n")
        else:
            print(
                "\n------------------------------------------------------------------------")
            print("Tidak ada Tweet terkait {nama_provider} dengan Lokasi {nama_kota} pada tanggal {today}".format(
                nama_provider=nama_provider, nama_kota=nama_kota_str, today=today_str_gmt_7))
            print(
                "------------------------------------------------------------------------\n")


for index, row in df_provider.iterrows():

    # pembuatan query
    # -----------------------------------------------------------------------------
    nama_provider = str(row['provider'])
    keywords = str(row['keywords'])
    # -----------------------------------------------------------------------------

    # -----------------------------------------------------------------------------
    print("\n------------------------------------------------------------------------")
    print("Mengambil Tweet pada terkait {nama_provider} yang berlokasi Bebas pada tanggal {today}".format(
        nama_provider=nama_provider, today=today_str_gmt_7))
    print("------------------------------------------------------------------------")
    # -----------------------------------------------------------------------------

    # scraping tweet dengan query
    # -----------------------------------------------------------------------------
    df_tweets = pd.DataFrame(
        sntwitter.TwitterSearchScraper(
            '{keywords} lang:id until:{tommorow} since:{yesterday} '.format(
                keywords=keywords, tommorow=tommorow_str_gmt_7, yesterday=yesterday_str_gmt_7)
        ).get_items()
    )
    # -----------------------------------------------------------------------------

    if df_tweets.empty == False:
        # mengambil username
        # -----------------------------------------------------------------------------
        cols = ['user']
        df_tweets["username"] = df_tweets[cols].applymap(
            lambda x: x['username'])
        # -----------------------------------------------------------------------------

        # define kolom awal
        # -----------------------------------------------------------------------------
        df_tweets = df_tweets[['id', 'date',
                               'username', 'content', 'lang']]
        df_tweets = df_tweets.rename(columns={
            'id': 'tweet_id', 'date': 'created', 'content': 'text', 'lang': 'language'})
        # -----------------------------------------------------------------------------

        # assign lokasi tweet dan nama provider
        # -----------------------------------------------------------------------------
        df_tweets['lokasi'] = "bebas"
        df_tweets['provider'] = nama_provider
        # -----------------------------------------------------------------------------

        # manipulasi dan memilih tweet berdasarkan waktu
        # -----------------------------------------------------------------------------
        # membuat kolom baru dan convert datetime ke GMT+7
        df_tweets["created_tz_indo"] = df_tweets['created'].dt.tz_convert(
            pytz.timezone('Asia/Jakarta'))
        # membuat kolom baru untuk mendapat tanggal pada GMT+7
        df_tweets["created_tz_indo_date"] = df_tweets['created_tz_indo'].dt.date
        # ubah dari object ke date
        df_tweets["created_tz_indo_date"] = pd.to_datetime(
            df_tweets["created_tz_indo_date"], format="%Y/%m/%d")
        # filter tweet dengan tanggal hari ini
        df_tweets = df_tweets.loc[(
            df_tweets['created_tz_indo_date'] == today_str_gmt_7)]
        # reset index
        df_tweets = df_tweets.reset_index(drop=True)
        # drop kolom datetime dan date GMT+7
        df_tweets.drop(
            df_tweets.columns[8], axis=1, inplace=True)
        df_tweets.drop(
            df_tweets.columns[7], axis=1, inplace=True)
        # print(df_tweets.dtypes)
        # print(df_tweets)
        # print(today_str_gmt_7)
        print(
            "\n------------------------------------------------------------------------")
        print(df_tweets)
        print(
            "------------------------------------------------------------------------")
        # -----------------------------------------------------------------------------

        if df_tweets.empty == False:
            # masukkan ke MongoDB
            # -----------------------------------------------------------------------------
            data_dict = df_tweets.to_dict("records")
            collection_tweets_2 = db.tweetsScrapeAll
            collection_tweets_2.insert_many(data_dict)
            # -----------------------------------------------------------------------------
        else:
            print(
                "\n------------------------------------------------------------------------")
            print("Tidak ada Tweet terkait {nama_provider} dengan Lokasi {nama_kota} pada tanggal {today} setelah seleksi".format(
                nama_provider=nama_provider, nama_kota=nama_kota_str, today=today_str_gmt_7))
            print(
                "------------------------------------------------------------------------\n")

    else:
        print(
            "\n------------------------------------------------------------------------")
        print("Tidak ada Tweet terkait {nama_provider} dengan Lokasi bebas pada tanggal {today}".format(
            nama_provider=nama_provider,  today=today_str_gmt_7))
        print(
            "------------------------------------------------------------------------\n")

