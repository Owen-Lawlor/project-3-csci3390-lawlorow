
1.
    Graph File               MIS File           Is an MIS? :
line_100_edges.csv line_100_MIS_test_1.csv         Yes     ;
line_100_edges.csv 	line_100_MIS_test_2.csv         No      ;
twitter_10000_edges.csv 	twitter_10000_MIS_test_1.csv	Yes     ;
twitter_10000_edges.csv	twitter_10000_MIS_test_2.csv    Yes     ;

2. 
    small_edges.csv: Number of Iterations = 1; Luby's Algorithm completed in 6s;
    line_100_edges.csv: Number of Iterations = 12; Luby's Algorithm completed in 21s; 
    twitter_100_edges.csv: Number of Iterations = 6; Luby's Algorithm completed in 14s;
    twitter_1000_edges.csv: Number of Iterations = 31; Luby's Algorithm completed in 49s;
    twitter_10000_edges.csv: Number of Iterations = 42; Luby's Algorithm completed in 77s;

3. a. Using 3 x 4 cores, the algorithm took about 2 hours to complete. The number of vertices per iteration slowed as the algorithm progressed. For example, it brought down the number of vertices from an initial number of over 11 million vertices to over 6 million in iteration 1, but from iteration 5 to iteration 6 it went from about 3.7 million active vertices to 3.1 million active vertices. 
b. Unfortunately, I was not able to complete the runs for 4 x 2 cores and 2 x 2 cores before submission, but I know that the order from fastest to slowest of these trials would be 3 x 4, 2 x 4, and then 2 x 2. This is because having more cores distributed over a larger number of machines leads to an increase in parallelism and in efficiency and  therefore a decreased runtime as the workload is able to be further distributed.
