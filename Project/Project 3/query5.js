// Find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age. If there is a tie, use the friend with the smallest user_id
// Return a javascript object : the keys should be user_ids and the value for each user_id is their oldest friend's user_id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify the users collection.
//
//You should return something like this:(order does not matter)
//{user1:oldestFriend1, user2:oldestFriend2, user3:oldestFriend3,...}
function oldest_friend(dbname){
	db = db.getSiblingDB(dbname);
  	var results = {};
  // TODO: implement oldest friends
  // return an javascript object described above

  db.flat_users.drop();
    db.users.aggregate([
	{ 
		$unwind: "$friends"}, 
    { 
    	$project: {
    		user_id: 1,
    		friends: 1,
    		_id: 0,
    }}, 
    { 
    	$out: "flat_users"
    }]);

  // get all existing friendship
  	db.friendship.drop();
  	db.flat_users.find().forEach(function(u){
  		db.friendship.insert([{"user_id": u.user_id, "friends": u.friends}, {"user_id": u.friends, "friends": u.user_id}]);
  	});

  	// join friendship with users and get user_id, friend_id and friend's year of birth
  	db.birth.drop();
	db.friendship.aggregate([{
		$lookup: {
			from: "users",
			localField: "friends",
			foreignField: "user_id",
			as: "Y"
		}
	}, {$project: {_id: 0, "user_id": 1, "friends": 1, "YOB": "$Y.YOB"}}, {$out: "birth"}]);

	// get every user's min YOB
	var cur = db.birth.aggregate([{
		$group: {_id: "$user_id", "minBirth": {$min: "$YOB"}}
	}]); 

	cur.forEach(function(user){
		var cc = db.birth.find({user_id: user._id, YOB: user.minBirth}, {user_id: 1, friends: 1}).sort({friends: 1}).limit(1);
		cc.forEach(function(old){
			results[user._id] = old.friends;
		})
	})
	return results;

	// cur.forEach(function(user){
	// 	// var cc = db.birth.find({user_id: user._id, YOB: user.minBirth}, {user_id: 1, friends: 1}).sort({friends: 1}).limit(1);
	// 	var cc = db.birth.find({user_id: user._id, YOB: user.minBirth}, {user_id: 1, friends: 1});
	// 	cc.forEach(function(old){
	// 		// print(user._id, old.friends);
	// 		var minId = [];
	// 		minId.push(old.friends);
	// 		minUserId = minId[0];
	// 		for (var i = 0; i < minId.length; i++)
	// 		{
	// 			// print(minId[i]);
	// 			if (minUserId > minId[i])
	// 			{
	// 				minUserId = minId[i];
	// 			}
	// 		}
	// 		results[user._id] = minUserId;
	// 		// print(user._id, results[user._id]);
	// 	})
	// });

  	return results
}
