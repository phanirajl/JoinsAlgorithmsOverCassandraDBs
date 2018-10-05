import com.datastax.driver.core.Cluster;
import CassandraJoins_time_measure.*;
//`import CassandraJoins.*;
public class TestCassandraJoins_time_measure
{

  public static void main(String[] args) throws  ClassNotFoundException, CassandraJoinsException
{

        String Host   = "127.0.1.1";
        String KeySpace = "moviedb";
        String tableName  =  "movie";
        String keyspace = "benchmark";

System.out.println("Testing Cassandra Joins. Please wait...");

	Cluster ClusterConn=CassandraJoins.connect(Host);

//Fountouris
String[] sc1={"movieid", "language", "color"};
String[] wr1={"movieid=\'0129884\'"};

String[] wr2={"event_type in (\'concert\', \'opera\')"};
String[] wr3={"event_type in (\'theater\', \'movie\')"};

String[] wr4={"id=5"};

//Query 2
String[] sc={ "label","comment", "producer","nr","propertyTex1","propertyTex2","propertyTex3",
		"propertyNum1", "propertyNum2", "propertyTex4", "propertyTex5", "propertyNum4"};
String[] wr={"nr=893"};
String[] sc8={"nr"};
String[] sc9={"productfeature"};
String[] wr8={"producttype=1"};
String[] sc10={ "product_label","product_comment", "producer","product_propertyTex1","product_propertyTex2","product_propertyTex3",
		"product_propertyNum1", "product_propertyNum2", "product_propertyTex4", "product_propertyTex5", "product_propertyNum4"};

//Query 8
String[] scQuery8={"title","text","reviewdate","rating1","rating2","rating3","rating4","language"}; 
//String[] wcQuery8={"language='en'"};
String[] scQuery81={"nr","name"};
String[] wcQuery8={"product=348","language=\'en\'"};

//Query 9
String[] scempty={};
String[] scQuery9={"nr","name","mbox_sha1sum","country"};
String[] wrQuery9={"nr=7034"};
String[] scQuery91={"nr","product","title"};

//Query 10
String[] scQuery10={"nr", "price"};
String[] wrQuery10={"product=481","deliverydays in (1,2,3)", "validto > '2008-07-14 21:00:00+0000'"};  //IN ('2008-07-14 21:00:00+0000', '2008-09-16 00:00:00')"};
String[] wrQuery101={"country='US'"};


//Query 12
String[] scQuery12={"vendor","offerwebpage", "price","deliverydays", "validto"};
String[] scQuery12_1={"nr", "label"};
String[] wrQuery12={"nr = 3"};
String[] scQuery12_2={"homepage", "label"};


try
{

//Query 2 !!!!	
System.out.println("Query 2");
CassandraJoins.join(ClusterConn , keyspace, "product", "producer", sc,wr , "producer","nr", sc8, null, 300000, "query_2_time","=");
CassandraJoins.join(ClusterConn , keyspace, "query_2", "product_nr", sc10,null, "productfeatureproduct","product", sc9, null, 300000, "query_2_finished_time","=");

//Query 8 !!!
System.out.println("Query 8");
CassandraJoins.join(ClusterConn, keyspace, "review", "person",scQuery8 , wcQuery8, "person", "nr", scQuery81, null, 20, "query_8_time", "=");
	
//Query 9 !!!!
System.out.println("Query 9");
CassandraJoins.join(ClusterConn , keyspace, "person", "nr", scQuery9, null, "review1","person", scempty, wrQuery9, 300000, "query_9_time","=");
CassandraJoins.join(ClusterConn , keyspace, "query_9", "nr", null, null, "review1","person", scQuery91, null, 300000, "query_9_finished_time","=");

//Query 10 !!!!
System.out.println("Query 10");
CassandraJoins.join(ClusterConn, keyspace, "offer", "vendor",scQuery10 , wrQuery10, "vendor", "nr", scempty, wrQuery101, 10, "query_10_time", "=");


//Query 12 !!!
System.out.println("Query 12");
CassandraJoins.join(ClusterConn , keyspace, "offer1", "product", scQuery12, wrQuery12, "product","nr", scQuery12_1, null, 300000, "query_12_time","=");
CassandraJoins.join(ClusterConn , keyspace, "query_12", "offer1_vendor", null, null, "vendor","nr", scQuery12_2, null, 300000, "query_12_finished_time","=");



//CassandraJoins.join(ClusterConn , keyspace, "query_2", "product_nr", sc10, null, "productfeatureproduct","product", sc9, null, 30000000, "query_2_finished","=");

//Fountouris
//CassandraJoins.join(ClusterConn , KeySpace, "movie", "movieid", sc1,null , "producedby","movieid", null, null, 300000, "movie_producedby","=");
//CassandraJoins.join(ClusterConn , KeySpace, "movie", "movieid", sc1,wr1 , "producedby","movieid", null, null, 300000, "movie_producedby2","=");
//CassandraJoins.join(ClusterConn , KeySpace, "events1", "price", null, wr2, "events2","price", null, wr3, 300000, "events1_events2","=");
//CassandraJoins.join(ClusterConn , KeySpace, "elements", "id", null, null, "collections","s", null, null, 300000, "elements_collections","CONTAINS");
//CassandraJoins.join(ClusterConn , KeySpace, "elements", "id", null, wr4, "collections","s", null, null, 300000, "elements_collections2","CONTAINS");




}
catch (CassandraJoinsException e)
{
	System.out.println(e);
}
	System.out.println("!!!!Finished!!!!");
	ClusterConn.close();

}
}
