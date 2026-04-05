from pyspark import SparkContext

sc = SparkContext("local", "Lab3_Bai4")
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("./movies.txt")
ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")
users_rdd = sc.textFile("./users.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

movie_map = movies_rdd.map(lambda line: line.split(",")) \
                      .map(lambda x: (x[0], x[1])) \
                      .collectAsMap()

def get_age_group(age):
    age = int(age)
    if age < 18: return "Under 18"
    elif age <= 24: return "18-24"
    elif age <= 34: return "25-34"
    elif age <= 44: return "35-44"
    elif age <= 54: return "45-54"
    else: return "55+"

user_age_map = users_rdd.map(lambda line: line.split(",")) \
                        .map(lambda x: (x[0], get_age_group(x[2]))) \
                        .collectAsMap()

# ((movieID, ageGroup), (rating, 1))
age_movie_ratings = ratings_rdd.map(lambda line: line.split(",")) \
    .map(lambda x: ((x[1], user_age_map.get(x[0], "Unknown")), (float(x[2]), 1)))

age_movie_avg = age_movie_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .map(lambda x: (x[0][1], x[0][0], round(x[1][0] / x[1][1], 4), x[1][1]))

# Phim cao nhất mỗi nhóm tuổi
best_per_age = age_movie_avg.map(lambda x: (x[0], (x[2], x[1], x[3]))) \
    .reduceByKey(lambda a, b: a if a[0] >= b[0] else b) \
    .sortBy(lambda x: x[0])

print("="*60)
print("BÀI 4: Phim có điểm cao nhất theo từng nhóm tuổi")
print("="*60)
for group, (avg, mid, cnt) in best_per_age.collect():
    print(f"{group:<12} | [{mid}] {movie_map.get(mid, 'Unknown'):<45} | Avg: {avg:.4f} | Count: {cnt}")

sc.stop()
