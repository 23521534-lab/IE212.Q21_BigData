-- Bài 2: Thống kê dữ liệu đánh giá khách sạn
-- Sử dụng dữ liệu đã qua xử lý từ Bài 1

-- Đọc dữ liệu từ output của Bài 1
data = LOAD 'file:///Users/dinhnguyenanhthu/output/bai1/part-r-00000' 
       USING PigStorage('\t') 
       AS (id:chararray, tokens:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- ============================================
-- Bài 2a: Thống kê tần số xuất hiện của các từ
-- ============================================
-- Tách các từ trong bình luận
words = FOREACH data GENERATE FLATTEN(TOKENIZE(tokens)) AS word;

-- Đếm tần số xuất hiện của từng từ
word_count = FOREACH (GROUP words BY word) GENERATE group AS word, COUNT(words) AS frequency;

-- Lưu kết quả đếm từ
STORE word_count INTO 'file:///Users/dinhnguyenanhthu/output/bai2a_temp' USING PigStorage('\t');

-- ============================================
-- Bài 2b: Thống kê số bình luận theo từng phân loại (category)
-- ============================================
category_count = FOREACH (GROUP data BY category) GENERATE group AS category, COUNT(data) AS total;

-- Lưu kết quả thống kê category
STORE category_count INTO 'file:///Users/dinhnguyenanhthu/output/bai2b_pig' USING PigStorage('\t');

-- ============================================
-- Bài 2c: Thống kê số bình luận theo từng khía cạnh đánh giá (aspect)
-- ============================================
aspect_count = FOREACH (GROUP data BY aspect) GENERATE group AS aspect, COUNT(data) AS total;

-- Lưu kết quả thống kê aspect
STORE aspect_count INTO 'file:///Users/dinhnguyenanhthu/output/bai2c_pig' USING PigStorage('\t');
