
// query 4: find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// "user_id" is the field from the users collection that you should use. 
// Do not use the "_id" field in the users collection.
  
function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);
    var pairs = [];
    // TODO: implement suggest friends
    // Return an array of arrays.
    var male = db.users.find({gender: "male", hometown: {"$ne": null}});
    male.forEach(function(userA){
    	var female = db.users.find({gender: "female", hometown: {"$ne": null}});
    	female.forEach(function(userB){
    		if (Math.abs(userA.YOB - userB.YOB) < year_diff)
    		{
    			if (userA.hometown.city == userB.hometown.city)
    			{
    				if (userA.user_id < userB.user_id)
    				{
    					var indexB = userA.friends.indexOf(userB.user_id);
    					if (indexB == -1)
    					{
    						pairs.push([userA.user_id, userB.user_id]);
    					}
    				}
    				else
    				{
    					var indexA = userB.friends.indexOf(userA.user_id);
    					if (indexA == -1)
    					{
    						pairs.push([userA.user_id, userB.user_id]);
    					}
    				}
    			}
    		}
    	})
    });
    return pairs;
}
