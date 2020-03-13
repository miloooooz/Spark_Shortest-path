// Code explanation:
// In this assignment, I first create a class called paths to store my the information finally needed to output.
// The first RDD is all_vertices which is used to store all vertices appearing.
// The second RDD is graph which gives all the vertices with their next_node and distance between each other.
// Then I transformed the RDD into adjacency_list which returns tuple in the form of (this_vertex, (next_node, distance, this_vertex))
// RDD adjacency_with_dis reduces adjacency_list to (this_vertex, (current_shortest_path, (current_distance, [(next_one, distance, this_vertex)])))
// Then it goes into a while loop containing: maptopairs which transforms adjacency_with_dis into all possible routes for current state (this_vertex, (parent_node + this_vertex, distance))
// reducepair then transforms the intersection of maptopairs and adjacency_with_dis into a new adjacency_with_dis: (vertex, (cur_path, (current_distance, [(next_one, distance, cur_path)])))
// If the contents of reducepair and previous adjacency_with_dis equal, break the while loop.
// THen set the required information into a paths structure RDD and save as file.
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;

public class AssigTwoz5223541 {

    public static class paths implements Serializable {
        String end;
        Integer dist;
        String path;

        public paths(String end, Integer dist, String path) {
            this.end = end;
            this.dist = dist;
            this.path = path;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            return sb.append(end).append(",").append(dist).append(",").append(path).toString();
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Assignment 2").setMaster("local");

        JavaSparkContext context = new JavaSparkContext(conf);

        String starting_node = args[0];
        JavaRDD<String> input = context.textFile(args[1]);

        JavaRDD<String> all_vertices = input.flatMap((FlatMapFunction<String, String>)
                inputs -> {
                    // input text file
                    // output all existing vertices distinctly
                    ArrayList<String> vertex_set = new ArrayList<>();
                    String[] parts = inputs.split(",");
                    vertex_set.add(parts[0]);
                    vertex_set.add(parts[1]);
                    return vertex_set.iterator();
        }).distinct();

        JavaPairRDD<String,Tuple2< String,Integer>> graph =
                input.union(all_vertices)       // the union here makes such format: ["N0, N1, 1", "N2, N3, 4", "N0", "N1", "N2","N3"]
                .mapToPair((PairFunction<String, String, Tuple2< String,Integer>>)
                        s -> {
                            String[] parts = s.split(",");
                            String start;
                            String end;
                            int dis;
                            if (parts.length == 3){
                                start = parts[0];
                                end = parts[1];
                                dis = Integer.parseInt(parts[2]);
                            }
                            else {
                                start = parts[0];
                                end = "None";
                                dis = -1;
                            }
                            return new Tuple2<>(start, new Tuple2<>(end, dis));
        });

        JavaPairRDD<String, Tuple3<String, Integer, String>> adjacency_list
                = graph.groupByKey()
                .flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Tuple3<String, Integer, String>>)
                        links -> {
                            // return (vertex, (next_node, distance, vertex))
                            String start = links._1;
                            Iterable<Tuple2<String, Integer>> dis = links._2;
                            ArrayList<Tuple2<String, Tuple3<String, Integer, String>>> ret = new ArrayList<>();
                            int length = 0;
                            for (Tuple2<String, Integer> d: dis){
                                length += 1;
                                if (!(length > 1 & d._2.equals(-1))){
                                    ret.add(new Tuple2<>(start, new Tuple3<>(d._1, d._2, start)));
                                }
                            }
                            return ret.iterator();
        });

        JavaPairRDD<String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>> adjacency_with_dis
                = adjacency_list
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<String, Iterable<Tuple3<String, Integer, String>>>, String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>)
                        adjacencylist -> {
                            // return (vertex, ("", (current_distance, [(next_one, distance, vertex)])))
                            String start = adjacencylist._1;
                            Iterable<Tuple3<String, Integer, String>> lists = adjacencylist._2;
                            int dis = -1;
                            if(start.equals(starting_node)) {
                                dis = 0;
                            }
                            return new Tuple2<>(start, new Tuple2<>(starting_node, new Tuple2<>(dis, lists)));
        });
//        adjacency_with_dis.collect().forEach(System.out::println);

        while (true) {
            JavaPairRDD<String, Tuple2<String, Integer>> maptopairs =
                    adjacency_with_dis
                    .flatMapToPair((PairFlatMapFunction<Tuple2<String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>, String, Tuple2<String, Integer>>)
                            lists -> {
                                // input (vertex, (current_path, (current_distance, [(next_one, distance, vertex)])))
                                // return (vertex, (parent_node + self, distance))
                                Iterable<Tuple3<String, Integer, String>> adjacency_cur = lists._2._2._2;
                                Integer cur_dist = lists._2._2._1;
                                ArrayList<Tuple2<String, Tuple2<String, Integer>>> ret = new ArrayList<>();
                                ArrayList<String> occurrence = new ArrayList<>();
                                for (Tuple3<String, Integer, String> t : adjacency_cur) {
                                    occurrence.add(t._1());
                                    if (cur_dist.equals(-1)) {
                                        ret.add(new Tuple2<>(t._1(), new Tuple2<>("", -1)));
                                    } else {
                                        if (t._2() != -1) {
                                            StringBuilder path = new StringBuilder();
                                            ret.add(new Tuple2<>(t._1(), new Tuple2<>(path.append(t._3()).append("-").append(t._1()).toString(), cur_dist + t._2())));
                                        }else
                                            ret.add(new Tuple2<>(t._1(), new Tuple2<>(lists._2._1, cur_dist + t._2())));
                                    }
                                }
                                int occurred = 0;
                                for (String o:occurrence){
                                    if (o.equals(lists._1))
                                        occurred += 1;
                                }
                                if (occurred == 0)
                                    ret.add(new Tuple2<>(lists._1, new Tuple2<>("", -1)));
                                return ret.iterator();
            });

//            maptopairs.collect().forEach(System.out::println);

            JavaPairRDD<String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>> reducepair =
                    maptopairs
                    .groupByKey()
                    .join(adjacency_with_dis)
                    .mapToPair((PairFunction<Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>>,
                            String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>)
                            last -> {
                                // input: (vertex, ([(parent_node + self, distance)], (current_path, (current_distance, [(next_one, distance, vertex)])))
                                // return (vertex, (cur_path, (current_distance, [(next_one, distance, cur_path)])))
                                String this_vertex = last._1;
                                Iterable<Tuple2<String, Integer>> candidates = last._2._1;
                                Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>> adjacency = last._2._2;
                                Integer min = adjacency._2._1;
                                String prev = "";
                                for (Tuple2<String, Integer> i : candidates) {
                                    if (i._2 > -1) {
                                        if (min == -1){
                                            min = i._2;
                                            prev = i._1;
                                        }
                                        else if (i._2 < min) {
                                            min = i._2;
                                            prev = i._1;
                                        }
                                    }
                                }
                                Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>> new_adjacency;

                                if (this_vertex.equals(starting_node)) {
                                    new_adjacency = new Tuple2<>(adjacency._1, new Tuple2<>(0, adjacency._2._2));
                                } else {
                                    if (min < adjacency._2._1 || adjacency._2._1.equals(-1)) {
                                        ArrayList<Tuple3<String, Integer, String>> new_adjacencylist = new ArrayList<>();
                                        for (Tuple3<String, Integer, String> adj: adjacency._2._2) {
                                            new_adjacencylist.add(new Tuple3<>(adj._1(), adj._2(), prev));
                                        }
                                        new_adjacency = new Tuple2<>(prev, new Tuple2<>(min, new_adjacencylist));
                                    } else {
                                        ArrayList<Tuple3<String, Integer, String>> new_adjacencylist = new ArrayList<>();
                                        for (Tuple3<String, Integer, String> adj: adjacency._2._2) {
                                            new_adjacencylist.add(new Tuple3<>(adj._1(), adj._2(), adjacency._1));
                                        }
                                        new_adjacency = new Tuple2<>(adjacency._1, new Tuple2<>(adjacency._2._1, new_adjacencylist));
                                    }
        //                            }
                                }
                                return new Tuple2<>(this_vertex, new_adjacency);
            });

//            reducepair.collect().forEach(System.out::println);

            Integer equality = adjacency_with_dis
                    .join(reducepair)
                    .map((Function<Tuple2<String, Tuple2<Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>>, Integer>)
                            joint -> {
                                Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>> joint1 = joint._2._1._2;
                                Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>> joint2 = joint._2._2._2;
                                int ret = 0;
                                if (joint1.equals(joint2))
                                    return 1;
                                return ret;
                            }).reduce(Integer::sum);

            if (equality == reducepair.count())
                break;

            adjacency_with_dis = reducepair;

        }

        JavaRDD<paths> answer = adjacency_with_dis.flatMap((FlatMapFunction<Tuple2<String, Tuple2<String, Tuple2<Integer, Iterable<Tuple3<String, Integer, String>>>>>, paths>)
                p -> {
                    ArrayList<paths> ret = new ArrayList<>();
                    String vertex = p._1;
                    Integer distant = p._2._2._1;
                    if (!vertex.equals(starting_node)){
                        if (distant == -1){
                            ret.add(new paths(vertex, distant, ""));
                        }else{
                            ret.add(new paths(vertex, distant, p._2._1));
                        }
                    }
                    return ret.iterator();
        })
                .sortBy((Function<paths, Object>)
                x -> x.dist, true, 1)
                .sortBy((Function<paths, Object>)
                x -> x.dist != -1, false, 1);
//        answer.collect().forEach(System.::println);
        answer.saveAsTextFile(args[2]);

    }
}
