// query6 : Find the average user friend count
//
// Return a decimal as the average user friend count of all users
// in the users document.

function find_average_friendcount(dbname){
  	db = db.getSiblingDB(dbname)
  // TODO: return a decimal number of average friend count
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
  	return db.flat_users.find().count() / db.users.find().count();
}
