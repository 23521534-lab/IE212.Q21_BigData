from pyspark import SparkContext

sc = SparkContext("local", "Lab3_Bai2")
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("./movies.txt")
ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

# Map MovieID -> [genres]
movie_genres = movies_rdd.map(lambda line: line.split(",")) \
                         .map(lambda x: (x[0], x[2].split("|"))) \
                         .collectAsMap()

# FlatMap ra (genre, (rating, 1))
genre_ratings = ratings_rdd.map(lambda line: line.split(",")) \
    .flatMap(lambda x: [(genre, (float(x[2]), 1)) for genre in movie_genres.get(x[1], [])])

# Tính trung bình theo genre
genre_avg = genre_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                         .map(lambda x: (x[0], round(x[1][0] / x[1][1], 4), x[1][1])) \
                         .sortBy(lambda x: -x[1])

print("="*60)
print("BÀI 2: Điểm trung bình theo thể loại phim")
print("="*60)
for genre, avg, cnt in genre_avg.collect():
    print(f"{genre:<25} | Avg: {avg:.4f} | Lượt: {cnt}")

sc.stop()
