// query 8: Find the average user friend count for all users with the same hometown city
// using MapReduce
// Using the same terminology in query7, we are asking you to write the mapper,
// reducer and finalizer to find the average friend count for each hometown city.

var city_average_friendcount_mapper = function() {
  // implement the Map function of average friend count
  var key = this.hometown.city;
  var value = {
  	user_num: 1,
  	friends_num: this.friends.length
  };
  emit(key, value);
};

var city_average_friendcount_reducer = function(key, values) {
  // implement the reduce function of average friend count
  var reducedVal = {user_num: 0, friends_num: 0};
  for (var index = 0; index < values.length; index++)
  {
  	reducedVal.user_num += values[index].user_num;
  	reducedVal.friends_num += values[index].friends_num;
  }
  return reducedVal;
};

var city_average_friendcount_finalizer = function(key, reduceVal) {
  // We've implemented a simple forwarding finalize function. This implementation 
  // is naive: it just forwards the reduceVal to the output collection.
  // You may need to change it to pass the test case for this query
  var ret = reduceVal;
  return ret.friends_num / ret.user_num;
}
