/*
 * Lab 02 - Task 2 (assignments.ipynb)
 * - Word frequencies; keep words with count > 500
 * - Number of comment lines per category
 * - Number of comment lines per aspect
 */

reviews_raw = LOAD '$INPUT_REVIEW' USING PigStorage(';') AS (
    review_id: int,
    comment: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

comment_norm = FOREACH reviews_raw GENERATE
    review_id,
    category,
    aspect,
    sentiment,
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(LOWER(TRIM(comment)), ',', ' '),
            '\\.', ' '),
          '!', ' '),
        '\\?', ' '),
      ';', ' ') AS comment_lc;

words = FOREACH comment_norm GENERATE
    review_id,
    category,
    aspect,
    sentiment,
    FLATTEN(TOKENIZE(comment_lc)) AS word;

words = FILTER words BY word IS NOT NULL AND TRIM(word) != '';

stopwords_raw = LOAD '$INPUT_STOP' USING TextLoader() AS (line:chararray);
stop_trim = FOREACH (FILTER stopwords_raw BY TRIM(line) != '') GENERATE TRIM(LOWER(line)) AS stop;

cog = COGROUP words BY word, stop_trim BY stop;
filtered = FILTER cog BY IsEmpty(stop_trim);
words_clean = FOREACH filtered GENERATE FLATTEN(words);

-- Word frequencies after stopword removal; keep frequent words (count > 500).
by_word = GROUP words_clean BY word;
word_counts = FOREACH by_word GENERATE group AS word, COUNT(words_clean) AS cnt;
frequent_words = FILTER word_counts BY cnt > 500;
STORE frequent_words INTO '$OUT_TASK2A' USING PigStorage('\t');

-- Each input row is one labeled comment segment (count segments per category).
by_category = GROUP reviews_raw BY category;
counts_by_category = FOREACH by_category GENERATE group AS category, COUNT(reviews_raw) AS cnt;
STORE counts_by_category INTO '$OUT_TASK2B' USING PigStorage('\t');

by_aspect = GROUP reviews_raw BY aspect;
counts_by_aspect = FOREACH by_aspect GENERATE group AS aspect, COUNT(reviews_raw) AS cnt;
STORE counts_by_aspect INTO '$OUT_TASK2C' USING PigStorage('\t');
