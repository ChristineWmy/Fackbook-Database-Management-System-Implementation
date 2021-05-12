package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
           

            // Get the most common first name
            ResultSet rst = stmt.executeQuery(
            	"SELECT COUNT(*), FIRST_NAME " +
            	"FROM " + UsersTable + " " +
            	"GROUP BY FIRST_NAME " + 
            	"ORDER BY COUNT(*) DESC");

            int numberOfMostCommonName = 0;

            while (rst.next()) {
            	if (rst.isFirst()) {
            		numberOfMostCommonName = rst.getInt(1);
            	}
            }

            FirstNameInfo info = new FirstNameInfo();
            info.setCommonNameCount(numberOfMostCommonName);

            rst = stmt.executeQuery(
            	"SELECT DISTINCT FIRST_NAME " +
            	"FROM " + UsersTable + " " +
            	"GROUP BY FIRST_NAME " + 
            	"HAVING COUNT(*) = " + numberOfMostCommonName +
            	"ORDER BY FIRST_NAME ASC");

            while (rst.next()) {
            	info.addCommonName(rst.getString(1));
            }



            // Get the Longest name
            rst = stmt.executeQuery(
            	"SELECT DISTINCT FIRST_NAME " +
            	"FROM " + UsersTable + " " + 
            	"WHERE LENGTH(FIRST_NAME) >= ALL( " + 
            	"SELECT LENGTH(FIRST_NAME) " + 
            	"FROM " + UsersTable + " " +
            	"GROUP BY LENGTH(FIRST_NAME)) " +
            	"ORDER BY FIRST_NAME ASC");


            while (rst.next()) {
            	info.addLongName(rst.getString(1));
            }


            // Get the shortest name
            rst = stmt.executeQuery(
            	"SELECT DISTINCT FIRST_NAME " + 
            	"FROM " + UsersTable + " " +
            	"WHERE LENGTH(FIRST_NAME) <= ALL( " +
            	"SELECT LENGTH(FIRST_NAME) " + 
           	    "FROM " + UsersTable + " " +
            	"GROUP BY LENGTH(FIRST_NAME)) " + 
            	"ORDER BY FIRST_NAME ASC");

            while (rst.next()) {
            	info.addShortName(rst.getString(1));
            }


            rst.close();
            stmt.close();

            return info;

            // return new FirstNameInfo();                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT U1.USER_ID, FIRST_NAME, LAST_NAME " + 
                "FROM " + UsersTable + " U1, ( " + 
                    "SELECT USER_ID " + 
                    "FROM " + UsersTable + " " +
                    "MINUS " + 
                    "(SELECT DISTINCT USER1_ID " + 
                    "FROM " + FriendsTable + " " +
                    "UNION " + 
                    "SELECT DISTINCT USER2_ID " +
                    "FROM " + FriendsTable + " ) " +
                ") U2 " + 
                "WHERE U1.USER_ID = U2.USER_ID " + 
                "ORDER BY U1.USER_ID ASC");


            // UserInfo info = new UserInfo();

            while (rst.next()) {
                results.add(new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3)));
            }

            rst.close();
            stmt.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " + 
                "FROM " + UsersTable + " U, " + HometownCitiesTable + " H, " + CurrentCitiesTable + " C " + 
                "WHERE U.USER_ID = H.USER_ID AND U.USER_ID = C.USER_ID AND H.HOMETOWN_CITY_ID != C.CURRENT_CITY_ID " + 
                "ORDER BY U.USER_ID ASC");

            while (rst.next()) {
                results.add(new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3)));
            }


            rst.close();
            stmt.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            Statement stmt1 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " + 
                "FROM " + PhotosTable + " P " + 
                "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " + 
                "JOIN ( " + 
                    "SELECT TAG_PHOTO_ID " + 
                    "FROM ( " + 
                        "SELECT TAG_PHOTO_ID, COUNT(*) " + 
                        "FROM " + TagsTable + " " + 
                        "GROUP BY TAG_PHOTO_ID " + 
                        "ORDER BY COUNT(*) DESC, TAG_PHOTO_ID ASC) " + 
                    "WHERE ROWNUM <= " + num + " " + 
                ") T ON P.PHOTO_ID = T.TAG_PHOTO_ID " + 
                "ORDER BY P.PHOTO_ID ASC");


            while (rst.next()) {
                PhotoInfo p = new PhotoInfo(rst.getInt(1), rst.getInt(2), rst.getString(3), rst.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

                // while (rst1.next()) {
                //     if (rst.getInt(1) == rst1.getInt(1)) {
                //         tp.addTaggedUser(new UserInfo(rst1.getInt(2), rst1.getString(3), rst1.getString(4)));
                //     }
                // }

                ResultSet rst1 = stmt1.executeQuery(
                    "SELECT T1.TAG_PHOTO_ID, T.TAG_SUBJECT_ID, U.FIRST_NAME, U.LAST_NAME " + 
                    "FROM " + UsersTable + " U " +
                    "JOIN " + TagsTable + " T ON U.USER_ID = TAG_SUBJECT_ID " + 
                    "JOIN ( " + 
                        "SELECT TAG_PHOTO_ID " + 
                        "FROM ( " + 
                        "SELECT TAG_PHOTO_ID, COUNT(*) " + 
                        "FROM " + TagsTable + " " + 
                        "GROUP BY TAG_PHOTO_ID " + 
                        "ORDER BY COUNT(*) DESC, TAG_PHOTO_ID ASC) " + 
                        "WHERE ROWNUM <= " + num + " " + 
                    ") T1 ON T.TAG_PHOTO_ID = T1.TAG_PHOTO_ID " + 
                    "WHERE T1.TAG_PHOTO_ID = " + rst.getInt(1) + " " +
                    "ORDER BY T1.TAG_PHOTO_ID ASC, T.TAG_SUBJECT_ID ASC"
                    );

                while (rst1.next()) {
                    tp.addTaggedUser(new UserInfo(rst1.getInt(2), rst1.getString(3), rst1.getString(4)));
                }
                results.add(tp);
                rst1.close();
            }

            rst.close();
            stmt.close();
            stmt1.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            Statement stmt1 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT C.USER_ID1, C.USER_ID2, COUNT(T1.TAG_PHOTO_ID) " + 
                "FROM " + TagsTable + " T1 " +
                "JOIN " + TagsTable + " T2 ON T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " + 
                "JOIN ( " + 
                    "SELECT T.USER_ID1, T.USER_ID2 " + 
                    "FROM ( " + 
                        "SELECT DISTINCT T1.TAG_SUBJECT_ID USER_ID1, T2.TAG_SUBJECT_ID USER_ID2 " + 
                        "FROM " + TagsTable + " T1, " + TagsTable + " T2 " + 
                        "WHERE T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID " + 
                        "INTERSECT " + 
                        "(SELECT DISTINCT U1.USER_ID USER_ID1, U2.USER_ID USER_ID2 " + 
                        "FROM " + UsersTable + " U1, " + UsersTable + " U2 " + 
                        "WHERE U1.USER_ID < U2.USER_ID AND U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " " + 
                        // "WHERE U1.USER_ID < U2.USER_ID AND U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= 5 " + 
                        "MINUS " + 
                        "SELECT * " + 
                        "FROM "+ FriendsTable + " ) " + 
                    ") T " + 
                    "WHERE ROWNUM <= " + num + " " + 
                    // "WHERE ROWNUM <= 5 " + 
                    "ORDER BY T.USER_ID1 ASC, T.USER_ID2 ASC " + 
                ") C ON T1.TAG_SUBJECT_ID = C.USER_ID1 AND T2.TAG_SUBJECT_ID = C.USER_ID2 " + 
                "GROUP BY (C.USER_ID1, C.USER_ID2) " + 
                "ORDER BY COUNT(T1.TAG_PHOTO_ID) DESC, C.USER_ID1 ASC, C.USER_ID2 ASC ");
            

            // Test
            // int rownum = 0;
            // while (rst.next()) {
            //     rownum += 1;
            // }

            // System.out.println(rownum);


            while (rst.next()) {

                int userId1 = rst.getInt(1);
                int userId2 = rst.getInt(2);

                ResultSet rst1 = stmt1.executeQuery(
                    "SELECT USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH " + 
                    "FROM " + UsersTable + " " +
                    "WHERE USER_ID = " + userId1 + " ");

                String userFirstName1 = "";
                String userLastName1 = "";
                int userYearOfBirth1 = 0;

                while (rst1.next()) {
                    userFirstName1 = rst1.getString(2);
                    userLastName1 = rst1.getString(3);
                    userYearOfBirth1 = rst1.getInt(4);
                }

                ResultSet rst2 = stmt2.executeQuery(
                    "SELECT USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH " + 
                    "FROM " + UsersTable + " " +
                    "WHERE USER_ID = " + userId2 + " ");

                String userFirstName2 = "";
                String userLastName2 = "";
                int userYearOfBirth2 = 0;

                while (rst2.next()) {
                    userFirstName2 = rst2.getString(2);
                    userLastName2 = rst2.getString(3);
                    userYearOfBirth2 = rst2.getInt(4);
                }

                rst2.close();

                UserInfo u1 = new UserInfo(userId1, userFirstName1, userLastName1);
                UserInfo u2 = new UserInfo(userId2, userFirstName2, userLastName2);

                MatchPair mp = new MatchPair(u1, userYearOfBirth1, u2, userYearOfBirth2);

                rst1 = stmt1.executeQuery(
                    "SELECT T1.TAG_PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " + 
                    "FROM " + TagsTable + " T1 " +
                    "JOIN " + TagsTable + " T2 ON T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                    "JOIN " + PhotosTable + " P ON T1.TAG_PHOTO_ID = P.PHOTO_ID " +
                    "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                    "WHERE T1.TAG_SUBJECT_ID = " + userId1 + " AND T2.TAG_SUBJECT_ID = " + userId2 + " " +
                    "ORDER BY T1.TAG_PHOTO_ID ASC ");

                while (rst1.next()) {
                    PhotoInfo p = new PhotoInfo(rst1.getInt(1), rst1.getInt(2), rst1.getString(3), rst1.getString(4));
                    mp.addSharedPhoto(p);
                    // results.add(mp);
                }
                results.add(mp);

                rst1.close();
            }

            rst.close();
            stmt.close();
            stmt1.close();
            stmt2.close();


        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            Statement stmt1 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */

            // stmt.executeUpdate("DROP VIEW FRIEND_MUTUAL");

            ResultSet rst = stmt.executeQuery(
                "CREATE VIEW FRIEND_MUTUAL AS ( " + 
                    "SELECT DISTINCT T1.U1 AS FRIEND_A, T1.U2 AS FRIEND_B, T2.U2 AS FRIEND_C " + 
                    "FROM (SELECT DISTINCT F1.USER1_ID AS U1, F1.USER2_ID AS U2 " + 
                        "FROM " + FriendsTable + " F1 " + 
                        "UNION " + 
                        "SELECT DISTINCT F2.USER2_ID AS U1, F2.USER1_ID AS U2 " + 
                        "FROM " + FriendsTable + " F2) T1 " + 
                    "JOIN (SELECT DISTINCT F1.USER1_ID AS U1, F1.USER2_ID AS U2 " + 
                        "FROM " + FriendsTable + " F1 " + 
                        "UNION " + 
                        "SELECT DISTINCT F2.USER2_ID AS U1, F2.USER1_ID AS U2 " + 
                        "FROM " + FriendsTable + " F2) T2 ON T1.U2 = T2.U1 AND T1.U1 < T2.U2 " + 
                    "WHERE (T1.U1, T2.U2) NOT IN ( " + 
                        "SELECT USER1_ID, USER2_ID " + 
                        "FROM " + FriendsTable + " " + 
                    ") " + 
                ") " );

            rst = stmt.executeQuery(
                "SELECT * " + 
                "FROM ( " + 
                    "SELECT FM.FRIEND_A, FM.FRIEND_C " + 
                    "FROM FRIEND_MUTUAL FM  " + 
                    "GROUP BY FM.FRIEND_A, FM.FRIEND_C " + 
                    "ORDER BY COUNT(FM.FRIEND_B) DESC, FM.FRIEND_A ASC, FRIEND_C ASC " + 
                ") " + 
                "WHERE ROWNUM <= " + num + " " );

            while (rst.next()) {
                int userId1 = rst.getInt(1);
                int userId2 = rst.getInt(2);

                // Get the userinfo of friend_A

                ResultSet rst1 = stmt1.executeQuery(
                    "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME " + 
                    "FROM " + UsersTable + " U1, " + UsersTable + " U2 " +
                    "WHERE U1.USER_ID = " + userId1 + " AND U2.USER_ID = " + userId2 + " ");

                // ResultSet rst1 = stmt1.executeQuery(
                //     "SELECT USER_ID, FIRST_NAME, LAST_NAME " + 
                //     "FROM " + UsersTable + " " + 
                //     "WHERE USER_ID = " + userId1 + " ");

                String firstName1 = "", lastName1 = "";
                String firstName2 = "", lastName2 = "";

                if (rst1.next()) {
                    firstName1 = rst1.getString(2);
                    lastName1 = rst1.getString(3);
                    firstName2 = rst1.getString(5);
                    lastName2 = rst1.getString(6);
                }

                // Get the userinfo of friend_B
                // rst1 = stmt1.executeQuery(
                //     "SELECT USER_ID, FIRST_NAME, LAST_NAME " + 
                //     "FROM " + UsersTable + " " + 
                //     "WHERE USER_ID = " + userId2 + " ");

                

                // if (rst1.next()) {
                //     firstName2 = rst1.getString(2);
                //     lastName2 = rst1.getString(3);
                // }

                UserInfo u1 = new UserInfo(userId1, firstName1, lastName1);
                UserInfo u2 = new UserInfo(userId2, firstName2, lastName2);

                UsersPair up = new UsersPair(u1, u2);


                rst1 = stmt1.executeQuery(
                    "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " + 
                    "FROM " + UsersTable + " U " + 
                    "JOIN FRIEND_MUTUAL FM ON U.USER_ID = FM.FRIEND_B " + 
                    "WHERE FM.FRIEND_A = " + userId1 + " AND FM.FRIEND_C = " + userId2 + " " + 
                    "ORDER BY U.USER_ID ASC ");

                while (rst1.next()) {
                    UserInfo u3 = new UserInfo(rst1.getInt(1), rst1.getString(2), rst1.getString(3));
                    up.addSharedFriend(u3);
                }
                results.add(up);
                rst1.close();
            }

            stmt.executeUpdate("DROP VIEW FRIEND_MUTUAL");

            rst.close();
            stmt.close();
            stmt1.close();




        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT MAX(COUNT(*)) " + 
                "FROM " + EventsTable + " E " + 
                "JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " + 
                "GROUP BY C.STATE_NAME ");

            int maxMonth = 0;

            while (rst.next()) {
                maxMonth += rst.getInt(1);
            }
          
           EventStateInfo info = new EventStateInfo(maxMonth);

           rst = stmt.executeQuery(
                "SELECT DISTINCT C.STATE_NAME " + 
                "FROM " + EventsTable + " E " + 
                "JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " + 
                "GROUP BY C.STATE_NAME " + 
                "HAVING COUNT(*) = " + maxMonth + " ");

            while (rst.next()) {
                info.addState(rst.getString(1));
            }

            rst.close();
            stmt.close();

            return info;

            // return new EventStateInfo(-1);                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH " + 
                "FROM " + UsersTable + " U  " + 
                "JOIN ( " + 
                    "SELECT DISTINCT USER2_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER1_ID = " + userID + " " + 
                    "UNION " +
                    "SELECT DISTINCT USER1_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER2_ID = " + userID + " " + 
                ") T ON U.USER_ID = T.USER_ID " + " " + 
                "WHERE YEAR_OF_BIRTH IS NOT NULL AND MONTH_OF_BIRTH IS NOT NULL AND DAY_OF_BIRTH IS NOT NULL" + " " + 
                "ORDER BY YEAR_OF_BIRTH ASC, MONTH_OF_BIRTH ASC, DAY_OF_BIRTH ASC ");

            int yearMin = 0, monthMin = 0, dayMin = 0;
            int yearMax = 0, monthMax = 0, dayMax = 0;

            while (rst.next()) {
                if (rst.isFirst()) {
                    yearMin = rst.getInt(1);
                    monthMin = rst.getInt(2);
                    dayMin = rst.getInt(3);
                }
                if (rst.isLast()) {
                    yearMax = rst.getInt(1);
                    monthMax = rst.getInt(2);
                    dayMax = rst.getInt(3);
                }
            }

            rst = stmt.executeQuery(
                "SELECT U.USER_ID, FIRST_NAME, LAST_NAME " + 
                "FROM " + UsersTable + " U  " + 
                "JOIN ( " + 
                    "SELECT DISTINCT USER2_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER1_ID = " + userID + " " +  
                    "UNION " +
                    "SELECT DISTINCT USER1_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER2_ID = " + userID + " " + 
                ") T ON U.USER_ID = T.USER_ID " + 
                "WHERE YEAR_OF_BIRTH = " + yearMax + " AND MONTH_OF_BIRTH = " + monthMax + " AND DAY_OF_BIRTH = " + dayMax + " " + 
                "ORDER BY U.USER_ID DESC");

            int youngUserId = 0;
            String youngFirstName = "", youngLastName = "";

            while (rst.next()) {
                if (rst.isFirst()) {
                    youngUserId = rst.getInt(1);
                    youngFirstName = rst.getString(2);
                    youngLastName = rst.getString(3);  
                }
            }

        
            rst = stmt.executeQuery(
                "SELECT U.USER_ID, FIRST_NAME, LAST_NAME " + 
                "FROM " + UsersTable + " U  " + 
                "JOIN ( " + 
                    "SELECT DISTINCT USER2_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER1_ID = " + userID + " " + 
                    "UNION " +
                    "SELECT DISTINCT USER1_ID AS USER_ID " + 
                    "FROM " + FriendsTable + " " + 
                    "WHERE USER2_ID = " + userID + " " + 
                ") T ON U.USER_ID = T.USER_ID " + 
                "WHERE YEAR_OF_BIRTH = " + yearMin + " AND MONTH_OF_BIRTH = " + monthMin + " AND DAY_OF_BIRTH = " + dayMin + " " + 
                "ORDER BY U.USER_ID DESC");

            int oldUserId = 0;
            String oldFirstName = "", oldLastName = "";


            while (rst.next()) {
                if (rst.isFirst()) {
                    oldUserId = rst.getInt(1);
                    oldFirstName = rst.getString(2);
                    oldLastName = rst.getString(3);
                }
            }

            UserInfo old = new UserInfo(oldUserId, oldFirstName, oldLastName);
            UserInfo young = new UserInfo(youngUserId, youngFirstName, youngLastName);

            rst.close();
            stmt.close();



            return new AgeInfo(old, young);                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME " + 
                "FROM " + UsersTable + " U1 " + 
                "JOIN " + UsersTable + " U2 ON  U1.USER_ID < U2.USER_ID " + 
                "JOIN " + HometownCitiesTable + " HC1 ON U1.USER_ID = HC1.USER_ID " + 
                "JOIN " + HometownCitiesTable + " HC2 ON U2.USER_ID = HC2.USER_ID " + 
                "JOIN " + FriendsTable + " F ON U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID " + 
                "WHERE ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 AND HC1.HOMETOWN_CITY_ID = HC2.HOMETOWN_CITY_ID " + 
                      "AND HC1.HOMETOWN_CITY_ID IS NOT NULL AND HC2.HOMETOWN_CITY_ID IS NOT NULL AND U1.LAST_NAME = U2.LAST_NAME " + 
                "ORDER BY U1.USER_ID ASC ");

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }

            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
