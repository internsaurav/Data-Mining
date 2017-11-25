package recSystems
/*
minCoUsers- minimum number of corated users to be considered as for pearson similarity
neigbrSize- number of nearest neighbours to choose when calculating the ratings.
 */

@SerialVersionUID(100L)
class Hyperparameters(var minCoUsers:Int = 0, var neigbrSize:Int = 0, var jaccardSim:Double =0, var avgUserRating:Double = 0.0, var minNeighbours:Int =0) extends Serializable {
  var minimumCoratingusers = minCoUsers
  var neighbourhoodSize = neigbrSize
  var minimumJaccardSimilarity= jaccardSim
  var averageUserRating = avgUserRating
  var minNumNeighbours = minNeighbours
}
