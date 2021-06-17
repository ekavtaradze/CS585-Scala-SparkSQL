# Project 3 - CS 585

GROUP: Elene Kavtaradze and Michael Ludwig

This submission is by Elene Kavtaradze and includes problems 1 and 4.

The whole project is built through SBT

Question One:
    
    Run QuestionOne class, as scala


Question Four:

    Run QuestionFour class, as java first, then scala
    
    Matrix Creation is done through Java and stored in /data
    Scala part reads files from /data
    
    This Joins the two datasets, groups by I and K, 
    and sums the values for each group. Since SparkSQL does
    not have reduce functoin, simply Spark SQL functions
    were used to achieve the same result as Map-Reduce
    
