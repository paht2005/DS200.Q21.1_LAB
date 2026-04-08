/*
 * Lab 02 - Task 5 (assignments.ipynb)
 * For each comment category, find the 5 words that occur most often overall
 * (all sentiments), after stopword removal — "most related" via highest within-category frequency.
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

by_cat_word = GROUP words_clean BY (category, word);
cnt_all = FOREACH by_cat_word GENERATE group.category AS category, group.word AS word, COUNT(words_clean) AS cnt;
by_cat = GROUP cnt_all BY category;
top5 = FOREACH by_cat {
    sorted = ORDER cnt_all BY cnt DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top;
};
flat = FOREACH top5 GENERATE category, FLATTEN(top.(word, cnt));
STORE flat INTO '$OUT_TASK5' USING PigStorage('\t');
