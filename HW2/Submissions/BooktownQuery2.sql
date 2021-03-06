-- SELECT SUBJECT
-- FROM SUBJECTS
-- WHERE SUBJECT_ID NOT IN (
-- 	SELECT DISTINCT SUBJECT_ID 
-- 	FROM BOOKS 
-- )
-- ORDER BY SUBJECT ASC;

-- SELECT DISTINCT S.SUBJECT
-- FROM SUBJECTS S
-- LEFT JOIN BOOKS B ON S.SUBJECT_ID = B.SUBJECT_ID
-- WHERE B.BOOK_ID IS NULL
-- ORDER BY S.SUBJECT ASC;

SELECT *
FROM (
	SELECT DISTINCT SUBJECT
	FROM SUBJECTS
	MINUS
	SELECT DISTINCT S.SUBJECT
	FROM SUBJECTS S 
	JOIN BOOKS B ON S.SUBJECT_ID = B.SUBJECT_ID
) T
ORDER BY T.SUBJECT ASC;