from pyspark import SparkContext

sc = SparkContext("local", "Lab3_Bai3")
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("./movies.txt")
ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")
users_rdd = sc.textFile("./users.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

# Map MovieID -> Title
movie_map = movies_rdd.map(lambda line: line.split(",")) \
                      .map(lambda x: (x[0], x[1])) \
                      .collectAsMap()

# Map UserID -> Gender
user_gender_map = users_rdd.map(lambda line: line.split(",")) \
                           .map(lambda x: (x[0], x[1])) \
                           .collectAsMap()

# ((movieID, gender), (rating, 1))
gender_movie_ratings = ratings_rdd.map(lambda line: line.split(",")) \
    .map(lambda x: ((x[1], user_gender_map.get(x[0], "U")), (float(x[2]), 1)))

# Tính trung bình
gender_movie_avg = gender_movie_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .map(lambda x: (x[0][0], x[0][1], round(x[1][0] / x[1][1], 4), x[1][1]))

print("="*60)
print("BÀI 3: Top 5 phim điểm cao nhất theo giới tính")
print("="*60)

print("\nNam (M):")
top_male = gender_movie_avg.filter(lambda x: x[1] == "M").sortBy(lambda x: -x[2]).take(5)
for mid, gender, avg, cnt in top_male:
    print(f"  [{mid}] {movie_map.get(mid, 'Unknown'):<50} | Avg: {avg:.4f} | Count: {cnt}")

print("\nNữ (F):")
top_female = gender_movie_avg.filter(lambda x: x[1] == "F").sortBy(lambda x: -x[2]).take(5)
for mid, gender, avg, cnt in top_female:
    print(f"  [{mid}] {movie_map.get(mid, 'Unknown'):<50} | Avg: {avg:.4f} | Count: {cnt}")

sc.stop()
