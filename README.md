# Spark_Shortest-path
A spark program to find out the shortest path in a graph from a particular input node to every single node rest in the graph.

### Input format:
> N0,N1,4
> 
> N0,N2,3
>
> N1,N2,2
>
> N1,N3,2
>
> N2,N3,7
>
> N3,N4,2
>
> N4,N0,4
>
> N4,N1,4
>
> N4,N5,6

### Output format:
> N2,3,N0-N2
>
> N1,4,N0-N1
>
> N3,6,N0-N1-N3
>
> ...
## Instruction:
Download the ***AssigTwoz5223541.java*** and ***Hadoop-Core.jar***.

cmd run `javac -cp ".:Hadoop-Core.jar" AssigTwoz5223541.java`

cmd run `java -cp ".:Hadoop-Core.jar" AssigTwoz5223541 starting_node input.txt output` whose starting_node is the node where you want your path starts with.

The output file will be contained in the directory ***output***.

Or you can directly download all the *.class* files representing all the classes contained in the file and then run `java -cp ".:Hadoop-Core.jar" AssigTwoz5223541 input.txt output`
