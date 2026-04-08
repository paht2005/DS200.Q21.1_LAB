/*
 * Lab 02 - Task 1 (assignments.ipynb)
 * - Lowercase all characters
 * - Split each comment line into words (whitespace tokenization)
 * - Remove stop words listed in stopwords.txt
 *
 * Run (from repo root, after params.properties is set):
 *   pig -x local -param_file DS200.Q21.1_Lab02/pig/params.properties -f DS200.Q21.1_Lab02/pig/task01_preprocess.pig
 */

-- Raw hotel review segments: id;comment;category;aspect;sentiment
reviews_raw = LOAD '$INPUT_REVIEW' USING PigStorage(';') AS (
    review_id: int,
    comment: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Lowercase and replace common punctuation with spaces to reduce glued tokens (e.g. "word,").
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

-- One row per (segment, token); tokens are split on whitespace by TOKENIZE.
words = FOREACH comment_norm GENERATE
    review_id,
    category,
    aspect,
    sentiment,
    FLATTEN(TOKENIZE(comment_lc)) AS word;

words = FILTER words BY word IS NOT NULL AND TRIM(word) != '';

-- Load stop words (one phrase or word per line).
stopwords_raw = LOAD '$INPUT_STOP' USING TextLoader() AS (line:chararray);
stop_trim = FOREACH (FILTER stopwords_raw BY TRIM(line) != '') GENERATE TRIM(LOWER(line)) AS stop;

-- Anti-join: keep tokens that do not appear in the stopword list (exact token match).
cog = COGROUP words BY word, stop_trim BY stop;
filtered = FILTER cog BY IsEmpty(stop_trim);
words_clean = FOREACH filtered GENERATE FLATTEN(words);

STORE words_clean INTO '$OUT_TASK1' USING PigStorage('\t');
