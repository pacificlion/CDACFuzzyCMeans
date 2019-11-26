package sparkcmeans;

import java.io.Serializable;
import java.util.*;
public class Point implements Serializable {
	ArrayList<Double> x=new ArrayList<Double>();
	public ArrayList<Double> getX() {
		return x;
	}
	/** Constructor Point Class
	 * @param s List of String
	 */
	Point(List<String> s){
		ArrayList<Double> d = new ArrayList<Double>();
		for(String t:s){
			if(t==""||t==null||t.isEmpty()||t.matches("[a-zA-Z]+\\.?"))d.add(Double.parseDouble("0.0"));
			else
			d.add(Double.parseDouble(t));
		}
		this.x = new ArrayList<Double>(d);
	}
	public void setX(ArrayList<Double> x) {
                this.x = new ArrayList<Double>(x);
	}
	Point(Point p){
                this.x = new ArrayList<Double>(p.x);
	}
	Point(ArrayList<Double> x){
		this.x =x;
	}
	Point(double val,int lim){
		ArrayList<Double> d = new ArrayList<Double>();
		for(int i=0;i<lim;i++){
			d.add(val);
		}
		this.x = new ArrayList<Double>(d);
	}
	/**
	 * @param dim Number of features
	 * @param min Minimum Value of random Value
	 * @param max Maximum Value of random Value
	 * @return Point object
	 */
	protected static Point RandomPointGenerator(int dim,int min,int max){
		ArrayList<Double> example =new ArrayList<Double>();
		Random r = new Random();
		for(int arr_i=0;arr_i<dim;arr_i++){
			double temp = min + (max - min) * r.nextDouble();
			example.add(temp);
		}
//		System.out.println(example);
		Point test=new Point(example);
		return test;
	}
	/**
	 * @param q point
	 * @param p another point
	 * @return point of Element wise addition of two points
	 */
	protected static Point Add(Point q,Point p){
		ArrayList<Double> xtemp=new ArrayList<Double>();
		try{
			if(p.x.size()==0||q.x.size()==0){
				throw new Exception("Point Not Initialised, Cannot Multiply");
			}
			for(int arr_i=0;arr_i<p.x.size();arr_i++){
                            double temp2 = p.x.get(arr_i)+q.x.get(arr_i);
				xtemp.add(temp2);
			}
		}
		catch(Exception e){
			System.out.println(e);
			System.exit(0);
		}
                Point ex = new Point(xtemp);
	return ex;
	}
	/**
	 * @param constant
	 * @param p point
	 * @return point of Element wise multiplication of a point and a constant
	 */
	protected static Point Multiply(double constant,Point p){
                ArrayList<Double> xtemp=new ArrayList<Double>();
		try{
			if(p.x.size()==0){
				throw new Exception("Point Not Initialised, Cannot Multiply");
			}
			for(int arr_i=0;arr_i<p.x.size();arr_i++){
				double temp2 = p.getX().get(arr_i)*constant;
                                xtemp.add(temp2);
			}
		}
		catch(Exception e){
			System.out.println(e);
			System.exit(0);
		}
                Point ex = new Point(xtemp);
		return ex;
	}
	/**
	 * @param number number of random points to produce
	 * @param dim Number of features
	 * @param min Minimum Value of random Value
	 * @param max Maximum Value of random Value
	 * @return list of points 
	 */
	protected static ArrayList<Point> CreateRandomPoints(int number,int dim, int min, int max){
		ArrayList<Point> P = new ArrayList<Point>();
		for(int arr_x=0;arr_x<number;arr_x++){
			P.add(RandomPointGenerator(dim, min, max));
		}
		return P;
	}
	/**
	 * @param A point A
	 * @param B point B
	 * @return Euclidean distance between two points
	 */
	protected static double DistanceComputer(Point A, Point B){
		double distance = 0;
		try{
			if(A.getX().size()!=B.getX().size()){
				throw new Exception("Dimensions of two points should be same for distance computing");
			}
			for(int arr_i=0;arr_i<A.getX().size();arr_i++){
				distance += Math.pow((A.getX().get(arr_i) - B.getX().get(arr_i)), 2);
			}
		}
		catch(Exception e){
			System.out.println(e);
			System.exit(0);
		}
		
		return Math.sqrt(distance);
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 * returns space separated features of point
	 */
	@Override
	public String toString() {
		String str="";
		int arr_i;
    	for(arr_i=0;arr_i<this.x.size()-1; arr_i++){
    		str=str.concat(this.getX().get(arr_i)+",");
    	}
    	str=str.concat(this.getX().get(arr_i)+"");
    	return str;
    }
}