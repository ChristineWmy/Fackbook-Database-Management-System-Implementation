CREATE VIEW VIEW_USER_INFORMATION AS
SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME, U.YEAR_OF_BIRTH, U.MONTH_OF_BIRTH, U.DAY_OF_BIRTH, U.GENDER, 
       C1.CITY_NAME AS CURRENT_CITY, C1.STATE_NAME AS CURRENT_STATE, C1.COUNTRY_NAME AS CURRENT_COUNTRY,
       C2.CITY_NAME AS HOMETOWN_CITY, C2.STATE_NAME AS HOMETOWN_STATE, C2.COUNTRY_NAME AS HOMETOWN_COUNTRY, 
       P.INSTITUTION AS INSTITUTION_NAME, E.PROGRAM_YEAR, P.CONCENTRATION AS PROGRAM_CONCENTRATION, 
       P.DEGREE AS PROGRAM_DEGREE
FROM USERS U
LEFT JOIN USER_CURRENT_CITIES UCC ON U.USER_ID = UCC.USER_ID
LEFT JOIN USER_HOMETOWN_CITIES UHC ON U.USER_ID = UHC.USER_ID 
LEFT JOIN CITIES C1 ON C1.CITY_ID = UCC.CURRENT_CITY_ID 
LEFT JOIN CITIES C2 ON C2.CITY_ID = UHC.HOMETOWN_CITY_ID
LEFT JOIN EDUCATION E ON U.USER_ID = E.USER_ID 
LEFT JOIN PROGRAMS P ON P.PROGRAM_ID = E.PROGRAM_ID ;

CREATE VIEW VIEW_ARE_FRIENDS AS
SELECT USER1_ID, USER2_ID
FROM FRIENDS;

CREATE VIEW VIEW_PHOTO_INFORMATION AS
SELECT A.ALBUM_ID,  A.ALBUM_OWNER_ID AS OWNER_ID, A.COVER_PHOTO_ID, A.ALBUM_NAME, A.ALBUM_CREATED_TIME,
	   A.ALBUM_MODIFIED_TIME, A.ALBUM_LINK, A.ALBUM_VISIBILITY, P.PHOTO_ID, P.PHOTO_CAPTION, 
	   P.PHOTO_CREATED_TIME, P.PHOTO_MODIFIED_TIME, P.PHOTO_LINK
FROM PHOTOS P
JOIN ALBUMS A ON P.ALBUM_ID = A.ALBUM_ID;

CREATE VIEW VIEW_EVENT_INFORMATION AS
SELECT UE.EVENT_ID, UE.EVENT_CREATOR_ID, UE.EVENT_NAME, UE.EVENT_TAGLINE, UE.EVENT_DESCRIPTION, UE.EVENT_HOST, 
       UE.EVENT_TYPE, UE.EVENT_SUBTYPE, UE.EVENT_ADDRESS, C.CITY_NAME AS EVENT_CITY, C.STATE_NAME AS EVNET_STATE,
       C.COUNTRY_NAME AS EVENT_COUNTRY, UE.EVENT_START_TIME, UE.EVENT_END_TIME
FROM USER_EVENTS UE
JOIN CITIES C ON UE.EVENT_CITY_ID = C.CITY_ID;

CREATE VIEW VIEW_TAG_INFORMATION AS
SELECT DISTINCT TAG_PHOTO_ID AS PHOTO_ID, TAG_SUBJECT_ID, TAG_CREATED_TIME, TAG_X AS TAG_X_COORDINATE, TAG_Y AS TAG_Y_COORDINATE
FROM TAGS;

