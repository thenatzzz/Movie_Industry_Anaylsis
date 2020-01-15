from pyspark.sql import SparkSession, functions, types
from lxml.html import parse
from urllib.request import urlopen
import pandas as pd




def main():
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')

    generate_cast_csv()
    generate_year_csv()

    df_casts = spark.read.load("726Dataset/casts(withoutYear).csv",format="csv",sep=",",header="true",escape='"',index ="true")
    df_year = spark.read.load("726Dataset/year.csv", format="csv", sep=",", header="true", escape='"', index="true")
    df_casts_year=df_casts.join(df_year,df_casts.id==df_year.id).select(df_casts.id,df_casts.title,df_year.year,df_casts.director,
                                                                    df_casts.actor_1,df_casts.actor_2,df_casts.actor_3,
                                                                    df_casts.actor_4,df_casts.actor_5)
    df_casts_year.write.csv("726Dataset/cast(year).csv", mode='overwrite',header="true")
    df_casts_year = spark.read.load("726Dataset/cast(year).csv", format="csv", sep=",", header="true", escape='"',index="true")
    df_movies = spark.read.load("726Dataset/movie_meta.csv",format="csv",sep=",",header="true",escape='"',index ="true")
    df_movies1 = df_movies.select(df_movies.id, df_movies.title, get_year(df_movies.release_date).alias("year"))
    cast = df_casts_year.join(df_movies1,[df_casts_year.year == df_movies1.year, df_casts_year.title == df_movies1.title])\
        .select(df_movies1.id, df_casts_year.title, df_casts_year.director, df_casts_year.actor_1, df_casts_year.actor_2, df_casts_year.actor_3,
                df_casts_year.actor_4,df_casts_year.actor_5)
    cast.repartition(1).write.csv("726Dataset/cast_past.csv", mode='overwrite', header="true")


def get_year(data):
    return data[0:4]

def generate_cast_csv():
    parsed = parse(urlopen('file:///Users/lanyihan/PycharmProjects/732Project/726Dataset/casts.html'))
    doc = parsed.getroot()
    all_actors = []
    last_id = ''
    last_title = ''
    last_director = ''
    dict_casts = {"id" :[],
                 "title": [],
                 "director": [],
                 "actor_1": [],
                 "actor_2": [],
                 "actor_3": [],
                 "actor_4": [],
                 "actor_5": []}
    tables = doc.findall('.//table')
    for index, table in enumerate(tables):
        if index == 0:
            continue
        else:
            trs = table.findall('.//tr')
            for indextr, tr in enumerate(trs):
                if indextr ==0:
                    ths = tr.findall('.//th')
                    D = ths[1].text_content().split(":",1) # delete sign 'D:'
                    director= D[1]
                else:
                    tds = tr.findall('.//td')
                    id = tds[0].text_content()
                    T = tds[1].text_content().split(":", 1) # delete sign 'T:'
                    title = T[1]
                    actor = tds[2].text_content()
                    if last_id !='' and id !=last_id:
                        dict_casts["id"].append(last_id)
                        dict_casts["title"].append(last_title)
                        dict_casts["director"].append(last_director)
                        count=0
                        i = 0
                        while count <5:
                            if i<len(all_actors):
                                if all_actors[i]==" s a":# delete actor named 's a', useless data
                                    i+=1
                                    continue
                                dict_casts["actor_" + str(count+1)].append(all_actors[i])
                            else:
                                dict_casts["actor_" + str(count+1)].append(" Unknown")
                            i+=1
                            count+=1
                        all_actors.clear()
                    last_id = id
                    last_title = title
                    last_director = director
                    all_actors.append(actor)
    pdf_casts = pd.DataFrame(dict_casts)
    pdf_casts.to_csv(r'726Dataset/casts(withoutYear).csv', mode='a', encoding='utf_8_sig', header=1, index=0)

def generate_year_csv():
    parsed = parse(urlopen('file:///Users/lanyihan/PycharmProjects/732Project/726Dataset/main.html'))
    doc = parsed.getroot()
    dict_year = {"id" :[],
                 "year": []}
    tables = doc.findall('.//table')
    for table in tables:
        trs = table.findall('.//tr')
        for indextr, tr in enumerate(trs):
            if indextr == 0:
                continue
            else:
                tds = tr.findall('.//td')
                id = tds[0].text_content()
                year = tds[2].text_content()
                dict_year["id"].append(id)
                dict_year["year"].append(year)

    pdf_year = pd.DataFrame(dict_year)
    pdf_year.to_csv(r'726Dataset/year.csv', mode='a', encoding='utf_8_sig', header=1, index=0)

if __name__ == '__main__':
    main()

