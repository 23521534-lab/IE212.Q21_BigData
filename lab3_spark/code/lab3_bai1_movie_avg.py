from pyspark import SparkContext
import os

sc = SparkContext("local", "Lab3_Bai1")
sc.setLogLevel("ERROR")

movies_rdd = sc.textFile("./movies.txt")
ratings1_rdd = sc.textFile("./ratings_1.txt")
ratings2_rdd = sc.textFile("./ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

# Map MovieID -> Title
movie_map = movies_rdd.map(lambda line: line.split(",")) \
                      .map(lambda x: (x[0], x[1])) \
                      .collectAsMap()

# (movieID, (rating, 1))
ratings_kv = ratings_rdd.map(lambda line: line.split(",")) \
                        .map(lambda x: (x[1], (float(x[2]), 1)))

# Tính tổng điểm và số lượt
movie_stats = ratings_kv.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Lọc phim có ít nhất 50 lượt
movie_avg = movie_stats.filter(lambda x: x[1][1] >= 50) \
                       .map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))

# Tạo thư mục output
os.makedirs("./output", exist_ok=True)

# Ghi kết quả ra file
with open("./output/bai1_result.txt", 'w') as f:
    f.write("="*60 + "\n")
    f.write("BÀI 1: Phim có điểm trung bình cao nhất (>= 50 lượt)\n")
    f.write("="*60 + "\n")
    
    # Kiểm tra nếu có phim đủ 50 lượt
    if movie_avg.count() == 0:
        f.write("\n⚠️ KHÔNG CÓ PHIM NÀO ĐỦ 50 LƯỢT ĐÁNH GIÁ!\n")
        f.write(f"\nTổng số phim có đánh giá: {movie_stats.count()}\n")
        
        # Hiển thị top 5 phim có nhiều lượt nhất
        f.write("\nTop 5 phim có nhiều lượt đánh giá nhất:\n")
        top5 = movie_stats.map(lambda x: (x[0], x[1][1])) \
                          .sortBy(lambda x: -x[1]) \
                          .take(5)
        for mid, cnt in top5:
            f.write(f"  [{mid}] {movie_map.get(mid, 'Unknown')} - {cnt} lượt\n")
    else:
        best_movie = movie_avg.sortBy(lambda x: -x[1]).first()
        mid, avg, cnt = best_movie
        f.write(f"\nPhim có điểm cao nhất:\n")
        f.write(f"  ID: {mid}\n")
        f.write(f"  Tên: {movie_map.get(mid, 'Unknown')}\n")
        f.write(f"  Điểm TB: {avg:.2f}\n")
        f.write(f"  Số lượt: {cnt}\n")

print("✅ Đã xuất kết quả ra file: ./output/bai1_result.txt")
sc.stop()
