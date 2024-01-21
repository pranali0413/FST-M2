-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/nikita/input.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group, COUNT(words);
-- Store the result in HDFSSTORE cntd INTO 'hdfs:///user/nikita/output1';	








