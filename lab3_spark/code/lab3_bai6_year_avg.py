from pyspark import SparkContext
from datetime import datetime

sc = SparkContext("local", "Lab3_Bai6")
sc.setLogLevel("ERROR")

ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

def timestamp_to_year(ts):
    return str(datetime.fromtimestamp(int(ts)).year)

# (year, (rating, 1))
year_ratings = ratings_rdd.map(lambda line: line.split(",")) \
                          .map(lambda x: (timestamp_to_year(x[3]), (float(x[2]), 1)))

year_avg = year_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                       .map(lambda x: (x[0], round(x[1][0] / x[1][1], 4), x[1][1])) \
                       .sortBy(lambda x: x[0])

print("="*60)
print("BÀI 6: Tổng lượt đánh giá và điểm trung bình theo năm")
print("="*60)
for year, avg, cnt in year_avg.collect():
    print(f"Năm {year} | Avg: {avg:.4f} | Lượt: {cnt}")

sc.stop()
