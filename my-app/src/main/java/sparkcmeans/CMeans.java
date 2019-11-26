package sparkcmeans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


import scala.Tuple2;

public class CMeans  implements Serializable  {
	
	private static final double FUZZINESS =10;
	private static final int CLUSTERNUM =5;
	private static final int MAX_ITERATION =5;
	private static final double EPSILON = 0.7;
	/**
	   * Initializes random centers
	   * @param data input data
	   * @return ArrayList of random centers
	   */
	public ArrayList<Point> initRandomCenters(JavaRDD<Point> data){
		List<Point> sample = data.takeSample(true, CLUSTERNUM, System.nanoTime());
		ArrayList<Point> clusters = new ArrayList<Point>(sample);
		return clusters;
	}
	/**
	   * Implementation of the Fuzzy C - Means algorithm
	   * @param data input data to the algorithm
	   * @param sc Java Spark Context variable passed from main function
	   */
	public void runAlgorithm(JavaRDD<Point> data, JavaSparkContext sc){
		// random initializations of the centers
		ArrayList<Point> centers = initRandomCenters(data);

		int iteration = 0;
		boolean converged = false;
		//This Algorithm should stop after converged = true or iteration crosses MAX_ITERATION
		while(iteration < MAX_ITERATION && (!converged)){
			// broadcast the centers to all the machines
			final Broadcast<ArrayList<Point>> broadcastedCenters =sc.broadcast(centers);
			
			JavaRDD<Point> centerCandidates = newCenterCalculation(data,broadcastedCenters);
			

			/*U can comment the following lines as they are only important to save MemberShip Matrix but 
			 * results are not used
			 * */
			// returns Membership Matrix by using data and broadcasted centers
			JavaRDD<ArrayList<Double>> Mem = memberShipMatrixGenerator(data,broadcastedCenters);
			//saves Membership matrix in a file
			Mem.saveAsTextFile("/home/prashant/Downloads/mem_matrix_iteration_"+iteration);
			
			
			
			// updatedResults is a Tuple2 : (updated_centers, is_changed)
			Tuple2<ArrayList<Point>,Boolean> updatedResults = UpdateCenters(centers,centerCandidates);
			
			
			if(!updatedResults._2){
				//This means no change was made
				converged=true;
			}
			else{
				centers = updatedResults._1;
				System.out.println("===================Iteration: "+iteration+"===================");
				//Save Output in files
				saveOutput(data,broadcastedCenters,sc,iteration);
				iteration++;
			}
			
		}
		if(iteration == MAX_ITERATION){
			System.out.println("Stopped Algorithm as Max Iteration: "+iteration+": Reached");
		}
		else{
			System.out.println("converged after "+"***********Iteration: "+iteration+":***********");
		}
	}
	/**
	 * This method saves output in files
	 * @param data input data
	 * @param broadcastedCenters broadcasted centers
	 * @param sc Java Spark Context
	 * @param iteration iteration at which Algorithm is running
	 */
	private void saveOutput(JavaRDD<Point> data, Broadcast<ArrayList<Point>> broadcastedCenters, JavaSparkContext sc, int iteration) {
		// TODO Auto-generated method stub
		JavaRDD<Tuple2<Integer, Point>> saveResults = mapPoints(data,broadcastedCenters);
		JavaRDD<Point> saveCenters2 = sc.parallelize(broadcastedCenters.value());
		JavaRDD<String> saveCenters = saveCenters2.map(new Function<Point, String>() {

			public String call(Point v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.toString();
			}
		});
		//saves Centers List for each iteration in respective file
		saveCenters.saveAsTextFile("/home/prashant/Downloads/center_list_iteration_"+iteration);
		//Saves points in respective Cluster files
		for(int j=0;j<CLUSTERNUM;j++){
			final int it = j;
			JavaRDD<String> results = saveResults.filter(new Function<Tuple2<Integer, Point>, Boolean>() {
				public Boolean call(Tuple2<Integer, Point> v1) throws Exception {
					// TODO Auto-generated method stub
					return (it==v1._1);
				}
			})
			.map(new Function<Tuple2<Integer, Point>, String>() {

				public String call(Tuple2<Integer,  Point> v1) throws Exception {
					// TODO Auto-generated method stub
					return v1._2.toString();
				}
			});
			results.saveAsTextFile("/home/prashant/Downloads/cluster_no_"+j+"_iteration_"+iteration);
		
		}
		
	}
	/**
	 * @param data input data
	 * @param broadcastedCenters Broadcasted Centers
	 * @return JavaRDD of Tuple2: (Integer, Point): i.e Mapped Point like (num,A) Point A belongs to cluster num
	 */
	private JavaRDD<Tuple2<Integer, Point>> mapPoints(JavaRDD<Point> data,
			final Broadcast<ArrayList<Point>> broadcastedCenters) {
		// TODO Auto-generated method stub
		JavaRDD<Tuple2<Integer,  Point>> mappedPoints = data.map(new Function<Point, Tuple2<Integer,  Point>>() {
			
			public Tuple2<Integer, Point> call(Point v1) throws Exception {
				Double totalDistance =0.0;
				final Double[] pointsToCluster = new Double[CLUSTERNUM];
				Arrays.fill(pointsToCluster,0.0);
				for(int j=0;j<CLUSTERNUM; j++){
					
					double tempDistance = Point.DistanceComputer(v1,broadcastedCenters.value().get(j))+0.001;
					pointsToCluster[j] =Math.pow(tempDistance, 2/(FUZZINESS-1));
					totalDistance+=(1/pointsToCluster[j]);
				}
				int index =0;
				double max_U = Double.MIN_VALUE;
				for(int j=0;j<CLUSTERNUM;j++){
					double u_i_j = Math.pow(pointsToCluster[j]*totalDistance, -1);
					if(max_U < u_i_j){
						index = j;
						max_U =u_i_j;
					}
				}
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Point>(index,v1);
			}
		});
		return mappedPoints;
	}
	/**
	 * @param data input Data
	 * @param broadcastedCenters broadcasted Centers
	 * @return JavaRDD of ArrayList<Double> : it returns Member Ship Matrix
	 */
	private JavaRDD<ArrayList<Double>> memberShipMatrixGenerator(JavaRDD<Point> data,
			final Broadcast<ArrayList<Point>> broadcastedCenters) {
		JavaRDD<ArrayList<Double>> mem = data.map(new Function<Point, ArrayList<Double>>() {
			
			public ArrayList<Double> call(Point v1) throws Exception {
				Double totalDistance =0.0;
				final Double[] pointsToCluster = new Double[CLUSTERNUM];
				ArrayList<Double> MemVector = new ArrayList<Double>();
				for(int j=0;j<CLUSTERNUM; j++){
					MemVector.add(0.0);
				}
				Arrays.fill(pointsToCluster,0.0);
				for(int j=0;j<CLUSTERNUM; j++){
					
					double tempDistance = Point.DistanceComputer(v1,broadcastedCenters.value().get(j))+0.001;
					pointsToCluster[j] =Math.pow(tempDistance, 2/(FUZZINESS-1));
					totalDistance+=(1/pointsToCluster[j]);
				}
				for(int j=0;j<CLUSTERNUM;j++){
					double u_i_j = Math.pow(pointsToCluster[j]*totalDistance, -1);
					MemVector.set(j, u_i_j);
				}
				// TODO Auto-generated method stub
				return MemVector;
			}
		});
		// TODO Auto-generated method stub
		return mem;
	}
	private Tuple2<ArrayList<Point>, Boolean> UpdateCenters(ArrayList<Point> centers,
			JavaRDD<Point> centerCandidates) {
		ArrayList<Point> newCenters = centers;
		ArrayList<Point> candidates = new ArrayList<Point>(centerCandidates.collect());
		Boolean isChanged = false;
		
		// go over all candidates and replace if needed:
		for(int j=0;j<CLUSTERNUM; j++){
			if(Point.DistanceComputer(centers.get(j), candidates.get(j))>EPSILON*EPSILON){
				// in case the distance is greater than EPSILON^2 we should replace the current center
				isChanged = true;
				newCenters.set(j,candidates.get(j));
			}
		}
		// TODO Auto-generated method stub
		return new Tuple2<ArrayList<Point>, Boolean>(newCenters,isChanged);
	}
	
	/**
	   * This method calculates the new centers, that may replace the current ones
	   * @param data Input data
	   * @param broadcastedCenters Broadcasted centers
	   * @return JavaRDD<Point> new Centers
	   */
	private JavaRDD<Point> newCenterCalculation(JavaRDD<Point> data, final Broadcast<ArrayList<Point>> broadcastedCenters) {
		// TODO Auto-generated method stub
		//gets number of features in data set
		final int Dim_Data = data.first().getX().size();
		FlatMapFunction<Iterator<Point>, Tuple2<Integer,Tuple2<Point,Double>>> setup=	new FlatMapFunction<Iterator<Point>,Tuple2<Integer,Tuple2<Point,Double>>>() {
			public Iterable<Tuple2<Integer, Tuple2<Point, Double>>> call(Iterator<Point> t) throws Exception {
				/**
			       * Represents for each point the distance from all the clusters:
			       * For data_point x_i:
			       * pointsToCluster[j] = pow((x_i - c_j), (2/(m-1)))
			       */
				final Double[] pointsToCluster = new Double[CLUSTERNUM];
				Arrays.fill(pointsToCluster,0.0);
				/**
			       * Represents the denominator part of the new center
			       * formula:
			       * NormPointsToCluster[j] = SUM_i: u_i_j_m
			       *
			       */
				final Double[] NormPointsToCluster = new Double[CLUSTERNUM];
				Arrays.fill(NormPointsToCluster,0.0);
				/**
			       * Single point from the membership matrix * data_point:
			       * ActualDistanceToCluster[j] = x_i * u_i_j_m
			       */
				final Point[] ActualDistanceToCluster = new Point[CLUSTERNUM];
				for(int arr_i=0;arr_i<CLUSTERNUM;arr_i++){
					ActualDistanceToCluster[arr_i]=new Point(0.0,Dim_Data);
				}
				ArrayList<Tuple2<Integer,Tuple2<Point,Double>>> temp420= new ArrayList<Tuple2<Integer,Tuple2<Point,Double>>>();
				while(t.hasNext()){
					Double totalDistance =0.0;
					//copying data point as it would be changed otherwise
					Point tempPoint = new Point(t.next());
					// computation of the distance of data point from each cluster:
					for(int j=0;j<CLUSTERNUM; j++){
						// the distance of data point from cluster j:
						double tempDistance = Point.DistanceComputer(tempPoint,broadcastedCenters.value().get(j))+0.001;
						pointsToCluster[j] =Math.pow(tempDistance, 2/(FUZZINESS-1));
						
						// update the total_distance:
						totalDistance+=(1/pointsToCluster[j]);
					}
					/**
			         * Calculation of the new values of the membership matrix:
			         * Each value in the matrix defined as:
			         *               u_i_j = 1 / ( SUM_k( pow((||x_i - c_j|| / ||x_i - c_K||), (2/(m - 1))) ))
			         */
					for(int j=0;j<CLUSTERNUM;j++){
						// calculation of (u_i_j)^m:
						double u_i_j_m = Math.pow(pointsToCluster[j]*totalDistance, -FUZZINESS);
						Point newTempPoint=Point.Multiply(u_i_j_m, tempPoint);
						ActualDistanceToCluster[j]=Point.Add(ActualDistanceToCluster[j], newTempPoint);
						NormPointsToCluster[j]+=u_i_j_m;
					}
					
				}
				temp420= new ArrayList<Tuple2<Integer,Tuple2<Point,Double>>>();
				// TODO Auto-generated method stub
				for(int j=0;j < CLUSTERNUM; j++){
					Tuple2<Point,Double> temp1 = new Tuple2<Point,Double>(ActualDistanceToCluster[j],NormPointsToCluster[j]);
					Tuple2<Integer,Tuple2<Point,Double>> temp2 = new Tuple2<Integer,Tuple2<Point,Double>>(j,temp1);
					temp420.add(temp2);
				}
//				System.out.println("length:="+temp420.size());
				return temp420;
			}
		};
		//combine function takes  a rdd and converts it into pair RDD
		PairFunction<Tuple2<Integer, Tuple2<Point, Double>>, Integer, Tuple2<Point, Double>> combine =new PairFunction<Tuple2<Integer,Tuple2<Point,Double>>, Integer, Tuple2<Point,Double>>() {

			public Tuple2<Integer, Tuple2<Point, Double>> call(Tuple2<Integer, Tuple2<Point, Double>> t)
					throws Exception {
				// TODO Auto-generated method stub
				return t;
			}
		};
		//PointAdd function is passed to reduce to aggregate results from all partitions
		Function2<Tuple2<Point, Double>, Tuple2<Point, Double>, Tuple2<Point, Double>> PointAdd=new Function2<Tuple2<Point,Double>, Tuple2<Point,Double>, Tuple2<Point,Double>>() {
			
			public Tuple2<Point, Double> call(Tuple2<Point, Double> v1, Tuple2<Point, Double> v2) throws Exception {
				return new Tuple2<Point, Double>(Point.Add(v1._1, v2._1),v1._2+v2._2);
				// TODO Auto-generated method stub
			}
		};
		//FinalMap is function that is passed to a map to return JavaRDD of points from pairRDD
		Function<Tuple2<Integer, Tuple2<Point, Double>>, Point> finalMap =new Function<Tuple2<Integer,Tuple2<Point,Double>>, Point>() {

			public Point call(Tuple2<Integer, Tuple2<Point, Double>> v1) throws Exception {
				// TODO Auto-generated method stub
				return Point.Multiply(1/(v1._2._2), v1._2._1);
			}
		};
		//Mapping data points to a center and converting into JavaPairRDD<Integer, Tuple2<Point, Double>> 
		JavaPairRDD<Integer, Tuple2<Point, Double>> newCenterTemp = data.mapPartitions(setup).mapToPair(combine);
		System.out.println("check420:"+newCenterTemp.count());
		//Adding Results saved from different partitions and mapping JavaPairRDD as JavaRDD of Points
		JavaRDD<Point> newCenterCandidates  = newCenterTemp.reduceByKey(PointAdd).map(finalMap);
		System.out.println("New Center Candidates Count:"+newCenterCandidates.count());
		return newCenterCandidates;
	}
	public static void main(String[] args){
		/*
		 * The following two lines Logger.* is to reduce the number of info messages coming 
		 * from Apache Spark as it clutters the output with unnecessary info*/
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		/*
		 * The conf is SparkConf created . 
		 * Argument options for setMaster method of SparkConf() object are:
		 * 1. "local"==> it runs the code in 1 core
		 * 2. "local[num]==> num is integer that should be ideally be same as number of cores present in the system*/
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines2 =sc.textFile("/home/prashant/Downloads/sirData.csv");
		/*
		 * The following statement splits each line of file into features 
		 * set the value of documentSeparator as 
		 * 1.CSV 
		 * 		final String documentSeparator=",";
		 * 2.space separated -" "
		 * 		final String documentSeparator=" ";
		 * 3. if any other pattern
		 * 		string pattern ="$$$"
		 * 		final String documentSeparator=pattern;
		 * */
		final String documentSeparator=",";
		/*
		 * Each data point is getting mapped as JavaRDD of Object of Point Class
		 * Point Class has various methods to simulate Calculations of Fuzzy C Means algorithm
		 * For details on Point Class . Refer to Point Class file*/
		JavaRDD<Point> data =lines2.map(new Function<String, Point>() {

			public Point call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new Point(Arrays.asList(arg0.split(documentSeparator)));
			}
		});
		/*
		 * Create an instance of CMeans class to run the algorithm*/
		CMeans t1 = new CMeans();
		/*Pass the data and Spark Context as parameters to runAlgorithm function
		 * */
		t1.runAlgorithm(data, sc);
		sc.close();
	}
}