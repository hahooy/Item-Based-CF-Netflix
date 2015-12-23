# Netflix Movie Recommendation System

A movie recommendation system for predicting movie ratings of given users. The recommendation system is based on a item-item collabrative filtering algorithm implemented in Hadoop MapReduce. The training data set contains 3.25 million movie ratings provided by Netflix. The testing data set contains 100000 ratings from Netflix.

Four MapReduce jobs are implemented for the recommendation system. The first job preprocesses training data and precomputes numRatings and sumRatings for every movie. The second job finds out all pairs of items that have been rated together by users. The third job computes the similarity for every pair of items based on Pearson correlation. The forth job predicts all ratings for all item-user entries in TestingRatings.txt. We also implemented a Java program called RMSE.java to compute the mean absolute error and root mean squared error.

## Ratings prediction errors

* Mean Absolute Error: 0.7276
* Root Mean Squared Error: 0.9210