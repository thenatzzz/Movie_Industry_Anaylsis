import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as fn
spark = SparkSession.builder.appName('add categorical index to features').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.sql.types import StructType,ArrayType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType,FloatType,DoubleType

NUM_TOP = 31
MODE_BINARY = False

movie_schema = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('production_companies', types.StringType(),nullable=True),
    types.StructField('production_countries', types.StringType()),
    types.StructField('genres', types.StringType()),
    types.StructField('release_date', types.TimestampType()),#types.DateType()),
    types.StructField('revenue', types.FloatType()),
    types.StructField('budget', types.FloatType()),
    types.StructField('avg_rating', types.FloatType()),
    types.StructField('runtime', types.FloatType()),
    types.StructField('cast', types.StringType()),
    types.StructField('director', types.StringType())
])
def recode(lst, find_lst):
    key = lst[0]
    value = lst[1]
    if value in find_lst:
            # return [key,value]

            return (key,value)
    else:
            # return [key,'others']
            return (key,'others')
def record_with_index(lst,find_lst):
    key = lst[0]
    value = lst[1]
    if value in find_lst:
            index_at_val = find_lst.index(value)
            return (key,index_at_val)
    else:
            return (key,str(NUM_TOP))
def main():
    inputs= 'new_clean_join_dataset/clean_join_top/clean_join_top_movie_cast.csv'

    df= spark.read.option("encoding", "UTF-8").schema(movie_schema).load(inputs,format="csv", sep=",", header="true",escape='"').cache()
    # top= df.groupBy('id').count().sort(fn.desc("count"))
    # print(top.show(20))
    # print("top count",top.count())
    # print("zero df:",df.count())

    df = df.dropDuplicates(['id'])
    df = df.filter(df['id'] != 'id')
    # print("second df:",df.count())
    print(df.show(5))

    # get list top companies
    # top_production_companies= df.groupBy('production_companies').count().sort(fn.desc("count"))
    # list_top_production_companies =top_production_companies.select("production_companies").rdd.flatMap(lambda x: x).collect()[:NUM_TOP]
    list_top_production_companies=['WarnerBros.', 'ParamountPictures', 'UniversalPictures', 'Metro-Goldwyn-Mayer(MGM)', 'TwentiethCenturyFoxFilmCorporation', 'Canal+', 'The', None, 'StudioCanal', 'ColumbiaPictures', 'Lionsgate', 'NewLineCinema', 'WaltDisneyPictures', 'RKORadioPictures', 'Film4', 'ColumbiaPicturesCorporation', 'Cin├®+', 'TouchstonePictures', 'UnitedArtists', 'EuropaCorp', 'Cin├®Cin├®ma', 'Gaumont', 'CJEntertainment', 'MiramaxFilms', 'DimensionFilms', 'Path├®', 'BBCFilms', 'TLAReleasing', 'TheAsylum', 'DreamWorksAnimation', 'DreamWorksSKG']
    # print(list_top_production_companies)

    # get list top countries
    # top_production_countries= df.groupBy('production_countries').count().sort(fn.desc("count"))
    # list_top_production_countries =top_production_countries.select("production_countries").rdd.flatMap(lambda x: x).collect()[:NUM_TOP]
    list_top_production_countries = ['UnitedStatesofAmerica', 'France', 'UnitedKingdom', 'Germany', 'Canada', 'Japan', 'India', 'Italy', 'Spain', 'Australia', 'Russia', 'SouthKorea', 'Sweden', 'China', 'Denmark', 'Brazil', 'Ireland', 'Netherlands', 'Belgium', 'Poland', 'Mexico', 'HongKong', 'Finland', 'Norway', 'Thailand', 'Switzerland', 'Argentina', 'Romania', 'Israel', 'Hungary', 'Austria']
    # print(list_top_production_countries)

    # get list top cast
    # top_cast= df.groupBy('cast').count().sort(fn.desc("count"))
    # list_top_cast =top_cast.select("cast").rdd.flatMap(lambda x: x).collect()[:NUM_TOP]
    list_top_cast=['Samuel L. Jackson', 'James Franco', 'Dolph Lundgren', 'Val Kilmer', 'Ray Liotta', 'Owen Wilson', 'Woody Harrelson', 'Keanu Reeves', 'Salma Hayek', 'G├®rard Depardieu', 'Danny Glover', 'Willem Dafoe', 'Nicolas Cage', 'Johnny Depp', 'Rosario Dawson', 'Dennis Hopper', 'Morgan Freeman', 'Michael Caine', 'Harvey Keitel', 'Scarlett Johansson', 'Stephen Rea', 'Susan Sarandon', 'Naomi Watts', 'John Goodman', 'Clive Owen', 'Will Ferrell', 'Liev Schreiber', 'Meryl Streep', 'Gary Oldman', 'Ben Affleck', 'Isabelle Huppert']
    # print(list_top_cast)

    # get list top directors
    # top_director= df.groupBy('director').count().sort(fn.desc("count"))
    # list_top_director =top_director.select("director").rdd.flatMap(lambda x: x).collect()[:NUM_TOP]
    list_top_director=['Hitchcock', 'Takashi Miike', 'Curtiz', 'Johnnie To', 'Woody Allen', 'J.Ford', 'John~Huston', 'Steven Soderbergh', 'Uwe Boll', 'Wyler', 'Michael Winterbottom', 'Woody~Allan', 'Clint Eastwood', 'Ridley Scott', 'Lumet', 'Tyler Perry', 'B.Wilder', 'B.dePalma', 'G.Stevens', 'Robert Rodriguez', 'Scorsese', 'John Stockwell', 'Cl.Brown', 'Stephen Frears', 'Steven Spielberg', 'Sion Sono', 'Cukor', 'Fran├ºois Ozon', 'Edwards', 'Ron Howard', 'Shawn Levy']
    # print(list_top_director)

    # change country
    rdd_country = df.select('id','production_countries').rdd
    if MODE_BINARY:
        rdd_country = rdd_country.map(lambda k : recode(k, list_top_production_countries))
        col_name_country = 'production_countries'
    else:
        rdd_country = rdd_country.map(lambda k : record_with_index(k, list_top_production_countries))
        col_name_country = 'key_country'
    df_country = rdd_country.toDF(['id_temp',col_name_country]).cache()

    # change company
    rdd_company = df.select('id','production_companies').rdd
    if MODE_BINARY:
        rdd_company = rdd_company.map(lambda k : recode(k, list_top_production_companies))
        col_name_company = 'production_companies'
    else:
        rdd_company = rdd_company.map(lambda k : record_with_index(k, list_top_production_companies))
        col_name_company ='key_company'
    df_company = rdd_company.toDF(['id',col_name_company]).cache()

    # change cast
    rdd_cast = df.select('id','cast').rdd
    if MODE_BINARY:
        rdd_cast = rdd_cast.map(lambda k : recode(k, list_top_cast))
        col_name_cast = 'cast'
    else:
        rdd_cast = rdd_cast.map(lambda k : record_with_index(k, list_top_cast))
        col_name_cast='key_cast'
    df_cast = rdd_cast.toDF(['id',col_name_cast]).cache()

    # change director
    rdd_director = df.select('id','director').rdd
    if MODE_BINARY:
        rdd_director = rdd_director.map(lambda k : recode(k, list_top_director))
        col_name_director = 'director'
    else:
        rdd_director = rdd_director.map(lambda k : record_with_index(k, list_top_director))
        col_name_director = 'key_director'
    df_director = rdd_director.toDF(['id',col_name_director]).cache()

    if MODE_BINARY:
        df = df.drop('production_countries','production_companies','cast','director')

    # join dataframes back
    new_df = df.join(df_country, df.id == df_country.id_temp).drop(df_country.id_temp)
    new_df = new_df.join(df_company, 'id').select(new_df['*'], df_company[col_name_company])
    new_df = new_df.join(df_cast, 'id').select(new_df['*'], df_cast[col_name_cast])
    new_df = new_df.join(df_director, 'id').select(new_df['*'], df_director[col_name_director])

    # insert key value
    if MODE_BINARY:
        new_df = new_df.withColumn('key_country', fn.when(new_df['production_countries'] == 'others', '0').otherwise('1'))
        new_df = new_df.withColumn('key_company', fn.when(new_df['production_companies'] == 'others', '0').otherwise('1'))
        new_df = new_df.withColumn('key_cast', fn.when(new_df['cast'] == 'others', '0').otherwise('1'))
        new_df = new_df.withColumn('key_director', fn.when(new_df['director'] == 'others', '0').otherwise('1'))
        new_df = new_df.fillna(0,subset=['key_country','key_company','key_cast','key_director'])
    else:
        new_df = new_df.fillna(NUM_TOP,subset=['key_country','key_company','key_cast','key_director'])

    # add profit column
    new_df = new_df.withColumn('profit', new_df['revenue'] - new_df['budget'])
    # rearrang column names
    order_col_names = ["id","title","genres","release_date","revenue","budget",'profit',"avg_rating","runtime","production_countries","production_companies", "cast","director","key_country","key_company","key_cast","key_director"]
    new_df = new_df.select(order_col_names)
    print(new_df.show(10))
    print(new_df.count())

    outputs = 'new_clean_join_dataset_index/clean_join_top_with_index_numerical'
    # outputs = 'new_clean_join_dataset_index/clean_join_top_with_index_binary'

    # new_df.coalesce(1).write.csv(outputs, mode = 'overwrite',header = True)

if __name__ == '__main__':
    main()
