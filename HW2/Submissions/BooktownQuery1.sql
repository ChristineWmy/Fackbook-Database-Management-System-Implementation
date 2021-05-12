SELECT B.Author_ID
FROM Authors A 
JOIN Books B ON A.Author_ID = B.Author_ID
GROUP BY B.Author_ID
HAVING COUNT(B.Book_ID) = 1
ORDER BY B.Author_ID;