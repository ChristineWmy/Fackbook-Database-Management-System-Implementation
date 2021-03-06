SELECT DISTINCT E1.SID AS SID
FROM ENROLLMENTS E1 
INNER JOIN ENROLLMENTS E2 ON E1.SID = E2.SID 
INNER JOIN ENROLLMENTS E3 ON E2.SID = E3.SID 
INNER JOIN COURSES C1 ON E1.CID = C1.CID 
INNER JOIN COURSES C2 ON E2.CID = C2.CID 
INNER JOIN COURSES C3 ON E3.CID = C3.CID 
WHERE C1.C_NAME = 'EECS442' AND C2.C_NAME = 'EECS445' AND C3.C_NAME = 'EECS492'
UNION 
SELECT DISTINCT E1.SID AS SID
FROM ENROLLMENTS E1 
INNER JOIN ENROLLMENTS E2 ON E1.SID = E2.SID 
INNER JOIN COURSES C1 ON E1.CID = C1.CID 
INNER JOIN COURSES C2 ON E2.CID = C2.CID 
WHERE C1.C_NAME = 'EECS482' AND C2.C_NAME = 'EECS486' 
UNION 
SELECT DISTINCT E.SID AS SID
FROM ENROLLMENTS E 
INNER JOIN COURSES C ON E.CID = C.CID 
WHERE C.C_NAME = 'EECS281' 
ORDER BY SID ASC ;

