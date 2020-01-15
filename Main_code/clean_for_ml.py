
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

country_lst = ['UnitedStatesofAmerica', 'France', 'UnitedKingdom', 'Germany', 'Canada', 'Japan', 'India', 'Italy', 'Spain', 'Australia', 'Russia', 'SouthKorea', 'Sweden', 'China', 'Denmark', 'Brazil', 'Ireland', 'Netherlands', 'Belgium', 'Poland', 'Mexico', 'Finland', 'HongKong', 'Norway', 'Thailand', 'Switzerland', 'Argentina', 'Romania', 'Israel', 'Hungary']
company_lst = ['WarnerBros.', 'ParamountPictures', 'UniversalPictures', 'Metro-Goldwyn-Mayer(MGM)', 'TwentiethCenturyFoxFilmCorporation', 'Canal+', 'The', 'StudioCanal', 'ColumbiaPictures', 'WaltDisneyPictures', 'Lionsgate', 'NewLineCinema', 'RKORadioPictures', 'Film4', 'ColumbiaPicturesCorporation', 'Ciné+', 'UnitedArtists', 'TouchstonePictures', 'EuropaCorp', 'CinéCinéma', 'Gaumont', 'CJEntertainment', 'MiramaxFilms', 'LionsGateFilms', 'Pathé', 'BBCFilms', 'DimensionFilms', 'DreamWorksSKG', 'TheAsylum', 'FoxSearchlightPictures']
cast_lst = ['Dakota Fanning', 'Julianne Moore', 'Bruce Willis', 'Dustin Hoffman', 'James Franco', 'John Goodman', 'Humphrey Bogart', 'Robert De Niro', 'Danny Glover', 'Steven Seagal', 'Gene Hackman', 'Samuel L. Jackson', 'Jason Statham', 'Bill Murray', 'Tom Wilkinson', 'Dennis Quaid', 'Russell Crowe', 'Kristen Stewart', 'Meryl Streep', 'Ray Liotta', 'Tom Cruise', 'Pierce Brosnan', 'Christian Bale', 'Laura Linney', 'Isabelle Huppert', 'Michael Caine', 'Ben Kingsley', 'Jack Black', 'Kate Beckinsale', 'Peter Stormare']
director_lst = ['Hitchcock', 'Takashi Miike', 'Curtiz', 'Johnnie To', 'Woody Allen', 'J.Ford', 'John~Huston', 'Steven Soderbergh', 'Uwe Boll', 'Wyler', 'Michael Winterbottom', 'Woody~Allan', 'Clint Eastwood', 'Ridley Scott', 'Lumet', 'Tyler Perry', 'B.Wilder', 'B.dePalma', 'G.Stevens', 'Scorsese', 'Robert Rodriguez', 'John Stockwell', 'Cl.Brown', 'Stephen Frears', 'Steven Spielberg', 'Sion Sono', 'Cukor', 'François Ozon', 'Edwards', 'Ron Howard']

def recode(lst, find_lst):
        key = lst[0]
        value = lst[1]
        if value in find_lst:
                return (key,value)
        else:
                return (key,'others')
        



def main(inputs, outputs, outputs_2):
    df_input = spark.read.csv(inputs, header = True)
    print(df_input.count())
    df_input = df_input.filter(df_input['id'] != 'id')
    print(df_input.count())
    df_test = df_input.groupby('id').agg(F.count('id')).sort(F.desc('count(id)'))
    print(df_test.show())
    df_input = df_input.drop_duplicates(subset=['id'])
    df_test = df_input.groupby('id').agg(F.count('id')).sort(F.desc('count(id)'))
    print(df_test.show())
    
    # change country
    rdd_country = df_input.select('id','production_countries').rdd
    rdd_country = rdd_country.map(lambda k : recode(k, country_lst))
    df_country = rdd_country.toDF(['id','production_countries'])


    # change company
    rdd_company = df_input.select('id','production_companies').rdd
    rdd_company = rdd_company.map(lambda k : recode(k, company_lst))
    df_company = rdd_company.toDF(['id','production_companies'])

    # change cast
    rdd_cast = df_input.select('id','cast').rdd
    rdd_cast = rdd_cast.map(lambda k : recode(k, cast_lst))
    df_cast = rdd_cast.toDF(['id','cast'])

    # change director
    rdd_director = df_input.select('id','director').rdd
    rdd_director = rdd_director.map(lambda k : recode(k, director_lst))
    df_director = rdd_director.toDF(['id','director'])

    df_input = df_input.withColumn('profit', df_input['revenue'] - df_input['budget'])
    df_input = df_input.drop('production_countries','production_companies','cast','director','revenue')
    



    
    # join
    df = df_input.join(df_country, 'id').select(df_input['*'], df_country['production_countries'])
    print(df.count())

    df = df.join(df_company, 'id').select(df['*'], df_company['production_companies'])
    print(df.count())

    df = df.join(df_cast, 'id').select(df['*'], df_cast['cast'])
    print(df.count())
    df = df.join(df_director, 'id').select(df['*'], df_director['director'])
    print(df.count())

    df.coalesce(1).write.csv(outputs, mode = 'overwrite',header = True)

    # insert key value
    df = df.withColumn('key_country', F.when(df['production_countries'] == 'others', '0').otherwise('1'))
    df = df.withColumn('key_company', F.when(df['production_companies'] == 'others', '0').otherwise('1'))
    df = df.withColumn('key_cast', F.when(df['cast'] == 'others', '0').otherwise('1'))
    df = df.withColumn('key_director', F.when(df['director'] == 'others', '0').otherwise('1'))
    print(df.count())

    df = df.drop('production_countries','production_companies','cast','director')

    #print(df.show())

    df.coalesce(1).write.csv(outputs_2, mode = 'overwrite',header = True)


    


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    outputs_2 = sys.argv[3]
    main(inputs, outputs, outputs_2)


    
