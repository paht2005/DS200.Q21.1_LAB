/*
 * Lab 02 - Task 4 (assignments.ipynb)
 * For each comment category, find the 5 most frequent non-stopwords in:
 *  - positive segments
 *  - negative segments
 * (Interpretation: strongest association with that polarity within the category by raw frequency.)
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
            '.', ' '),
          '!', ' '),
        '?', ' '),
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

-- Positive segments: top 5 words per category by frequency.
pos_words = FILTER words_clean BY sentiment == 'positive';
by_cat_word_pos = GROUP pos_words BY (category, word);
cnt_pos = FOREACH by_cat_word_pos GENERATE group.category AS category, group.word AS word, COUNT(pos_words) AS cnt;
by_cat_pos = GROUP cnt_pos BY category;
top_pos = FOREACH by_cat_pos {
    sorted = ORDER cnt_pos BY cnt DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top;
};
flat_pos = FOREACH top_pos GENERATE category, FLATTEN(top.(word, cnt));
STORE flat_pos INTO '$OUT_TASK4A' USING PigStorage('\t');

-- Negative segments: top 5 words per category by frequency.
neg_words = FILTER words_clean BY sentiment == 'negative';
by_cat_word_neg = GROUP neg_words BY (category, word);
cnt_neg = FOREACH by_cat_word_neg GENERATE group.category AS category, group.word AS word, COUNT(neg_words) AS cnt;
by_cat_neg = GROUP cnt_neg BY category;
top_neg = FOREACH by_cat_neg {
    sorted = ORDER cnt_neg BY cnt DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top;
};
flat_neg = FOREACH top_neg GENERATE category, FLATTEN(top.(word, cnt));
STORE flat_neg INTO '$OUT_TASK4B' USING PigStorage('\t');
