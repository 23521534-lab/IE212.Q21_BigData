from pyspark import SparkContext

sc = SparkContext("local", "Lab3_Bai5")
sc.setLogLevel("ERROR")

ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")
users_rdd = sc.textFile("./users.txt")
occupation_rdd = sc.textFile("./occupation.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

# Map OccupationID -> OccupationName
occ_map = occupation_rdd.map(lambda line: line.split(",")) \
                        .map(lambda x: (x[0], x[1])) \
                        .collectAsMap()

# Map UserID -> OccupationName
user_occ_map = users_rdd.map(lambda line: line.split(",")) \
                        .map(lambda x: (x[0], occ_map.get(x[3], "Unknown"))) \
                        .collectAsMap()

# (occupation, (rating, 1))
occ_ratings = ratings_rdd.map(lambda line: line.split(",")) \
                         .map(lambda x: (user_occ_map.get(x[0], "Unknown"), (float(x[2]), 1)))

occ_avg = occ_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                     .map(lambda x: (x[0], round(x[1][0] / x[1][1], 4), x[1][1])) \
                     .sortBy(lambda x: -x[1])

print("="*60)
print("BÀI 5: Điểm trung bình theo nghề nghiệp")
print("="*60)
for occ, avg, cnt in occ_avg.collect():
    print(f"{occ:<15} | Avg: {avg:.4f} | Lượt: {cnt}")

sc.stop()
