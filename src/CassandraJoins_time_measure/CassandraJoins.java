package CassandraJoins_time_measure;


import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.ListIterator;
import java.util.Date;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.Bytes;


public class CassandraJoins 
{


	private CassandraJoins()
	{;}

public static Cluster connect(String node)
 {

    Cluster  cluster = Cluster.builder()
            .addContactPoint(node)
            .build();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("ClusterConnected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() )
       {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }

    return cluster;
   }

private static boolean hasColumn(Cluster ClusterConn , String KeySpace, String tableName, String columnName) 
{
if (!hasTable(ClusterConn ,  KeySpace, tableName) )
	return false;

if (ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getColumn(columnName)!=null)
	return true;

return false;
} 


private static boolean IsIndexedColumn(Cluster ClusterConn , String KeySpace, String tableName, String columnName)  throws CassandraJoinsException
{
if (!hasTable(ClusterConn ,  KeySpace, tableName) )
  throw new CassandraJoinsException("Table not found. Keyspace : "+ KeySpace + " Table: "+ tableName);

if (ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getColumn(columnName).getName()!=null)
	return true;

return false;
} 


private static boolean IsSinglePartitionKey(Cluster ClusterConn , String KeySpace, String tableName, String columnName) throws CassandraJoinsException
{
boolean found=false;
int keysnumber=0;

if (!hasTable(ClusterConn ,  KeySpace, tableName) )
  throw new CassandraJoinsException("Table not found. Keyspace : "+ KeySpace + " Table: "+ tableName);

List<ColumnMetadata> l= ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getPartitionKey();

if (l==null)
	return false;

if (l.isEmpty())
	return false;

ListIterator<ColumnMetadata> i = l.listIterator();


do
{
if (i.next().getName().equals(columnName))
  found= true;
keysnumber++;
}
while (i.hasNext());

if (keysnumber !=1 )
	return false;

if (found) return true;

return false;
}


private static boolean hasAllPartitionKeys(Cluster ClusterConn , String KeySpace, String tableName, String[] whereRelations, String columnName) throws CassandraJoinsException
{

if (!hasTable(ClusterConn ,  KeySpace, tableName) )
  throw new CassandraJoinsException("Table not found. Keyspace : "+ KeySpace + " Table: "+ tableName);


if (whereRelations==null)
	return IsSinglePartitionKey(ClusterConn , KeySpace, tableName, columnName);

List<ColumnMetadata> l= ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getPartitionKey();

if (l==null)
	return false;

if (l.isEmpty())
	return false;

ListIterator<ColumnMetadata> i = l.listIterator();


do
{
String keyname= i.next().getName();
boolean found=false;
for (int j=0; j < whereRelations.length;j++)
	{
          
	if (whereRelations[j].trim().startsWith(keyname))
		found=true;

	}

if (columnName!=null)
{
if (keyname.equals(columnName))
   found=true;
}

if (found==false)
	return false;

}
while (i.hasNext());


return true;
}




private static boolean  IsClusteringKey(Cluster ClusterConn , String KeySpace, String tableName, String columnName)  throws CassandraJoinsException
{

if (!hasTable(ClusterConn ,  KeySpace, tableName) )
  throw new CassandraJoinsException("Table not found. Keyspace : "+ KeySpace + " Table: "+ tableName);


List<ColumnMetadata> l= ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getClusteringColumns();


if (l==null)
	return false;


if (l.isEmpty())
	return false;

ListIterator<ColumnMetadata> i = l.listIterator();


do
{
ColumnMetadata c = i.next();

if (c.getName().equals(columnName))
  return true;
}
while (i.hasNext());

return false;

}


private static boolean hasAllPrevClusteringKeys(Cluster ClusterConn , String KeySpace, String tableName,  String[] whereRelations, String columnName)  throws CassandraJoinsException
{

if (!hasTable(ClusterConn ,  KeySpace, tableName) )
  throw new CassandraJoinsException("Table not found. Keyspace : "+ KeySpace + " Table: "+ tableName);


List<ColumnMetadata> l= ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getClusteringColumns();


if (l==null)
	return false;


if (l.isEmpty())
	return false;

ListIterator<ColumnMetadata> i = l.listIterator();


do
{

ColumnMetadata c = i.next();

String keyname=c.getName();


if (whereRelations==null)
{

if (!keyname.equals(columnName))
   return false;

return true;
}

if (keyname.equals(columnName))
    break;

boolean found=false;
for (int j=0; j < whereRelations.length;j++)
	{
          
	if (whereRelations[j].trim().startsWith(keyname))
		found=true;

	}

if (found==false)
	return false;

}
while (i.hasNext());


return true;	
}




private static String createSelectClause(String[] selectColumns, String columnName)
{
if (selectColumns==null) 
	return new String("SELECT *");

int size=selectColumns.length;

String result=null;

if  (size==0)
	return new String("SELECT *");

if (columnName!=null)
{
 result=new String("SELECT "+ columnName + " ");

for (int i=0;i<size;i++)
  if (!(selectColumns[i].equals(columnName)))
	result = new String(result + ", " + selectColumns[i] + " ");
}
else
{
 result=new String("SELECT "+selectColumns[0]);

for (int i=1;i<size;i++)
	result = new String(result + ", "+selectColumns[i]);
}

return result;
}

private static String createWhereClause(String[] whereRelations, String columnName, String val, String operator)
{



String result="";

String op="=";

if (operator!=null)
if (operator.equals("CONTAINS"))
	op=" CONTAINS ";

if ((columnName!=null) && (val!=null) && (whereRelations==null))
	return new String("WHERE (" + columnName + op + val + ")");

if ((columnName!=null) && (val!=null) && (whereRelations!=null))
	result=new String("WHERE (" + columnName + op + val + ") AND (" + whereRelations[0] + ")");

if  (((columnName==null) || (val==null)) && (whereRelations!=null) )
  {
        if (whereRelations.length!=0)
	 	result=new String("WHERE ("+ whereRelations[0] + ") ");
     
 }

if (whereRelations !=null)
{
int size = whereRelations.length;

	for (int i=1;i<size;i++)
		result = new String(result + " AND ("+whereRelations[i] + ")");
}

return result;
}


private static String getColumnType(Cluster ClusterConn , String KeySpace, String tableName, String columnName) 
{
ColumnMetadata column=ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getColumn(columnName);
if (column==null)
   return null;
return  column.getType().toString();

}


private static boolean hasTable(Cluster ClusterConn , String KeySpace, String tableName) 
{
if (ClusterConn.getMetadata().getKeyspace(KeySpace)==null) 
   return false;

TableMetadata table=ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName);

if (table==null)
   return false;

return true;

}

private static boolean isCollectionType(String ctype)
{
if ( ctype.startsWith("set") || ctype.startsWith("map") || ctype.startsWith("list") )
	return true;

return false;
}

private static String create_join_table(Cluster ClusterConn , String KeySpace, String tableName1, String columnName1, String[] selectColumns1,  String tableName2, String columnName2,  String[] selectColumns2,  String resultTableName) throws CassandraJoinsException
{
String query = new String("CREATE TABLE " + KeySpace + "." + resultTableName + "\n(\n");
String resultcolumns=new String(columnName1);
String pkeycolumns="";

String joincoltype=getColumnType(ClusterConn,KeySpace,tableName1, columnName1);
if (!isCollectionType(joincoltype))
	 pkeycolumns=new String(columnName1);

query+=columnName1 + " " + getColumnType(ClusterConn,KeySpace,tableName1, columnName1);


	for (int i=0;i < selectColumns1.length; i++)
  	{
    		if (!selectColumns1[i].equals(columnName1))
                 {
                        String ctype=getColumnType(ClusterConn,KeySpace,tableName1, selectColumns1[i]);
			if (ctype.equals("counter"))
                        	throw new CassandraJoinsException("Cannot Join tables with column type: counter. Keyspace:"+ KeySpace + " Table:"+tableName1+" Column:"+ selectColumns1[i]);
                        if (ctype==null)
                           throw new CassandraJoinsException("Column:"+selectColumns1[i] +" does not exists on table:"+tableName1);
          		query+=",\n"+tableName1+"_"+selectColumns1[i] + " " + ctype;
                        resultcolumns+=", "  +tableName1+"_"+selectColumns1[i] ; 
			if (!isCollectionType(ctype))
			{
			  if (pkeycolumns.equals(""))
                      	  	pkeycolumns+=tableName1+"_"+selectColumns1[i] ; 
			  else
                      	  	pkeycolumns+=", "  +tableName1+"_"+selectColumns1[i] ; 
			}
                 }
   	}

String tabName2=tableName2;

if (tableName2.equals(tableName1))
  tabName2= tableName2 + "_";


	for (int i=0;i < selectColumns2.length; i++)
  	{
    		if (!selectColumns2[i].equals(columnName2))
		{
			
                        String ctype=getColumnType(ClusterConn,KeySpace,tableName2, selectColumns2[i]);
			if (ctype.equals("counter"))
                        	throw new CassandraJoinsException("Cannot Join tables with column type: counter. Keyspace:"+ KeySpace + " Table:"+tableName2+" Column:"+ selectColumns2[i]);
                        if (ctype==null)
                           throw new CassandraJoinsException("Column:"+selectColumns2[i] +" does not exists on table:"+tableName2);
          		query+=",\n"+tabName2+"_"+selectColumns2[i] + " " + ctype;
                        resultcolumns+=", "  +tabName2+"_"+selectColumns2[i] ; 
			if (!isCollectionType(ctype))
                      	  pkeycolumns+=", "  +tabName2+"_"+selectColumns2[i] ; 

		}
   	}

query+=",\n PRIMARY KEY (" + pkeycolumns + ")";

query+="\n);\n";


Session session=ClusterConn.connect();

session.execute("DROP TABLE if exists " + KeySpace +"." + resultTableName);
//session.execute(query);
return resultcolumns;
}

private static String  join2rows(Cluster ClusterConn, String KeySpace, String tableName1, String[] selectColumns1, String columnName1, String tableName2, String[] selectColumns2, String columnName2, String resultTableName, String resultColumns, Row row1, Row row2) throws CassandraJoinsException
{
    String keyval1=getVal(ClusterConn,KeySpace,tableName1, columnName1, row1);

                    String inserts="("+keyval1;

                      
			for (int i=0;i < selectColumns1.length; i++)
  			{
    				if (!selectColumns1[i].equals(columnName1))
                 		{

               				inserts= inserts+ ", " + getVal(ClusterConn,KeySpace,tableName1, selectColumns1[i], row1);
              
                 		}
   			}

		for (int i=0;i < selectColumns2.length; i++)
  			{
    				if (!selectColumns2[i].equals(columnName2))
				{
			

               				inserts= inserts+ ", " + getVal(ClusterConn,KeySpace,tableName2, selectColumns2[i], row2);
              
                 		}

		    
		}
                     inserts+=")";

                     inserts= "insert into " + KeySpace + "." + resultTableName + "(" + resultColumns + ") values" + inserts;

return new String(inserts);
}

private static void sortmergejoin(Cluster ClusterConn , String KeySpace, String tableName1, String columnName1, String[] selectColumns1, String[] whereRelations1, String tableName2, String columnName2, String[] selectColumns2, String[] whereRelations2,  long rowsLimit, String resultTableName, String resultColumns) throws CassandraJoinsException
{
System.out.println("Running sortmerge join. Keyspace:" + KeySpace + " Table1:" + tableName1 + " Column1:"+ columnName1 + " Table2: " + tableName2 + " Column2:"+ columnName2);

	long startTime = System.currentTimeMillis();

sortmergejoin( ClusterConn , KeySpace,  tableName1,  columnName1, selectColumns1,  whereRelations1, tableName2,  columnName2,  selectColumns2,  whereRelations2,   rowsLimit,  resultTableName,  resultColumns,  true);

sortmergejoin( ClusterConn , KeySpace,  tableName1,  columnName1, selectColumns1,  whereRelations1, tableName2,  columnName2,  selectColumns2,  whereRelations2,   rowsLimit,  resultTableName,  resultColumns,  false);

long millis = System.currentTimeMillis() - startTime;
long second = (millis / 1000) % 60;
long minute = (millis / (1000 * 60)) % 60;
long hour = (millis / (1000 * 60 * 60)) % 24;
millis = millis % 1000;
String time = String.format("%02d hours %02d min %02d sec %d msec", hour, minute, second, millis);
	System.out.println("Elapsed time:"+time);
}

private static void sortmergejoin(Cluster ClusterConn , String KeySpace, String tableName1, String columnName1, String[] selectColumns1, String[] whereRelations1, String tableName2, String columnName2, String[] selectColumns2, String[] whereRelations2,  long rowsLimit, String resultTableName, String resultColumns, boolean left) throws CassandraJoinsException
{

String where1= createWhereClause(whereRelations1,null,null,null);

String query1= createSelectClause(selectColumns1, columnName1) + "\n" + 
"FROM " + KeySpace + "." + tableName1 + "\n" +
where1+
"ORDER BY " + columnName1 + " ASC";


if (rowsLimit!=0)
  query1+= " LIMIT " + rowsLimit;


Session session=ClusterConn.connect();

ResultSet results1 = null;

if (where1.contains(" in ") ||  where1.contains(" IN "))
	results1 = session.execute(new SimpleStatement(query1).setFetchSize(Integer.MAX_VALUE));
else
	 results1 = session.execute(query1);




String where2= createWhereClause(whereRelations2,null,null,null);

String query2= createSelectClause(selectColumns2, columnName2) + "\n" + 
"FROM " + KeySpace + "." + tableName2 + "\n" +
where2 +
"ORDER BY " + columnName2 + " ASC";


if (rowsLimit!=0)
  query1+= " LIMIT " + rowsLimit;




ResultSet results2 = null;

if (where2.contains(" in ") ||  where2.contains(" IN "))
	 results2 = session.execute(new SimpleStatement(query2).setFetchSize(Integer.MAX_VALUE));
else
	 results2 = session.execute(query2);



Iterator<Row> i1 = results1.iterator();
Iterator<Row> i2 = results2.iterator();

if ( (!i1.hasNext()) || (!i2.hasNext()))
   return;

Row row1=i1.next(); 
Row row2=i2.next(); 
String keyval1=getVal(ClusterConn,KeySpace,tableName1, columnName1, row1);
String keyval2=getVal(ClusterConn,KeySpace,tableName2, columnName2, row2);

do
{

    if (keyval1.equals(keyval2)) 
     {
       String inserts=join2rows(ClusterConn,KeySpace,tableName1,selectColumns1,columnName1,tableName2,selectColumns2,columnName2,  resultTableName, resultColumns, row1, row2);
      // session.execute(inserts);
       if (left)
       {
       	row2=i2.next();
        if (row2!=null)
            keyval2=getVal(ClusterConn,KeySpace,tableName2, columnName2, row2);
       }
      else
        {
	 row1=i1.next();
         if (row1!=null)
              keyval1=getVal(ClusterConn,KeySpace,tableName1, columnName1, row1);
         }
     }
     else if (keyval1.compareTo(keyval2) < 0 )
     {
      row1=i1.next();
      if (row1!=null)
           keyval1=getVal(ClusterConn,KeySpace,tableName1, columnName1, row1);
    }
   else  
   {
       row2=i2.next();
      if (row2!=null)
          keyval2=getVal(ClusterConn,KeySpace,tableName2, columnName2, row2);
   }
}
while ((row1!=null) && (row2!=null));

}

private static String getVal(Cluster ClusterConn, String KeySpace, String tableName, String columnName, Row row) throws CassandraJoinsException
{

     String ctype=getColumnType(ClusterConn,KeySpace,tableName, columnName);
     int valint=0;
     double valdouble=0;
     long vallong=0;
     BigInteger valbigint=null;
     boolean valbool=false;
     BigDecimal valdecimal=null;
     float valfloat=0;
     Date valdate=null;
     InetAddress valinet=null;
     ByteBuffer valbytes=null;

     switch(ctype)
     {
     case "int":
           valint=row.getInt(columnName);
	   return new String(Integer.toString(valint));
     case "bigint":
	   vallong=row.getLong(columnName);
	   return new String(Long.toString(vallong));
     case "varint":
           valbigint=row.getVarint(columnName);
           return new String(valbigint.toString());
     case "boolean":
	   valbool=row.getBool(columnName);
	   return new String(Boolean.toString(valbool));
     case "decimal":
	   valdecimal=row.getDecimal(columnName);
	   return new String(valdecimal.toString());
     case "float":
           valfloat=row.getFloat(columnName);
	   return new String(Float.toString(valfloat));
     case "double":
	   valdouble=row.getDouble(columnName);
           return new String( Double.toString(valdouble));
     case "blob":
           valbytes=row.getBytes(columnName);
	   System.out.println(Bytes.toHexString(valbytes));
           return new String(Bytes.toHexString(valbytes));
     case  "ascii":
     case "text":
     case "varchar":
	 String val=row.getString(columnName);
	 return new String("\'"+ val + "\'");
     case "inet":
	   valinet=row.getInet(columnName);
           return new String("\'" + valinet.toString().substring(1) + "\'");
     case "timestamp":
           valdate=row.getTimestamp(columnName);
           return new String(Long.toString(valdate.getTime()));
     case "counter":
	throw new CassandraJoinsException("Cannot Join tables with column type: counter. Keyspace:"+ KeySpace + " Table:"+tableName+" Column:"+ columnName);
     default:
        if  (!isCollectionType(ctype))
		throw new CassandraJoinsException("Unsupported type for Cassandra Joins. Keyspace:"+ KeySpace + " Table:"+tableName+" Column:"+ columnName + " Type:"+ctype);
	
	if (ctype.startsWith("set"))
	{
		String contenttype=ctype.substring(4,ctype.length()-1);
                boolean strliteral=false;
		if (contenttype.equals("text") || contenttype.equals("ascii") ||  contenttype.equals("varchar") ||   contenttype.equals("inet"))
		strliteral=true;

		Set<Object> valset=row.getSet(columnName, Object.class);
		Iterator<Object>  si = valset.iterator();
		String result="{";
		if (strliteral)
               		  result=result + "\'" + si.next().toString() + "\'";
	        else
               		  result=result +  si.next().toString();
                while (si.hasNext())
		{
			Object o= si.next();
			if (strliteral)
                        	result=result + ", " + "\'" + o.toString() + "\'";
			else
                        	result=result + ", " + o.toString();
		}
	        return new String(result+"}");	
	}
       else if (ctype.startsWith("list"))
	{
	String contenttype=ctype.substring(5,ctype.length()-1);
                boolean strliteral=false;
		if (contenttype.equals("text") || contenttype.equals("ascii") ||  contenttype.equals("varchar") ||   contenttype.equals("inet"))
		strliteral=true;

		List<Object> vallist=row.getList(columnName, Object.class);
		Iterator<Object> li = vallist.iterator();
		String result="[";
		if (strliteral)
               		  result=result + "\'" + li.next().toString() + "\'";
	        else
               		  result=result +  li.next().toString();
                while (li.hasNext())
		{
			Object o= li.next();
			if (strliteral)
                        	result=result + ", " + "\'" + o.toString() + "\'";
			else
                        	result=result + ", " + o.toString();
		}
	        return new String(result+"]");	

	}
       else if (ctype.startsWith("map"))
	{
	        String contenttype1="";
	        String contenttype2="";

		int i=4;
		while ( (i<ctype.length()) && (ctype.charAt(i)!=','))
		{
			contenttype1+=ctype.charAt(i);
			i++;
 		}

		i++;
		
		while ( (i<ctype.length()) && (ctype.charAt(i)!='>'))
		{
			contenttype2+=ctype.charAt(i);
			i++;
 		}


		boolean strliteral1=false;
		boolean strliteral2=false;

		if (contenttype1.equals("text") || contenttype1.equals("ascii") ||  contenttype1.equals("varchar") ||   contenttype1.equals("inet"))
			strliteral1=true;
		if (contenttype2.equals("text") || contenttype2.equals("ascii") ||  contenttype2.equals("varchar") ||   contenttype2.equals("inet"))
			strliteral2=true;

		Map<Object,Object> valmap=row.getMap(columnName, Object.class, Object.class);

		Set<Map.Entry<Object,Object>> me = valmap.entrySet();

		String result="{";

		Iterator<Map.Entry<Object,Object>> ei = me.iterator();

		Map.Entry<Object,Object> e=ei.next();

		if (strliteral1)
               		  result=result + "\'" + e.getKey().toString() + "\'";
	        else
               		  result=result +  e.getKey().toString();

		result+=":";

		if (strliteral2)
               		  result=result + "\'" + e.getValue().toString() + "\'";
	        else
               		  result=result +  e.getValue().toString();

		while (ei.hasNext())
		{
			e= ei.next();

			if (strliteral1)
                        	result=result + ", " + "\'" + e.getKey().toString() + "\'";
			else
                        	result=result + ", " + e.getKey().toString();
		
			result+=":";

			
			if (strliteral2)
                        	result=result +  "\'" + e.getValue().toString() + "\'";
			else
                        	result=result +  e.getValue().toString();



		}
	        return new String(result+"}");	




       }

	
    } 
return null;

}

private static void indexjoin(Cluster ClusterConn , String KeySpace, String tableName1, String columnName1, String[] selectColumns1, String[] whereRelations1, String tableName2, String columnName2,  String[] selectColumns2, String[] whereRelations2, long rowsLimit, String resultTableName, String resultColumns, String operator) throws CassandraJoinsException
{

System.out.println("Running index join. Keyspace:" + KeySpace + " Table1:" + tableName1 + " Column1:"+ columnName1 + " Table2: " + tableName2 + " Column2:"+ columnName2);


	long startTime = System.currentTimeMillis();

String query1= createSelectClause(selectColumns1, columnName1) + "\n" + 
"FROM " + KeySpace + "." + tableName1 + "\n" +
createWhereClause(whereRelations1,null,null,null) ;

if (rowsLimit!=0)
  query1+= " LIMIT " + rowsLimit;

Session session=ClusterConn.connect();

	ResultSet results1 = session.execute(query1);

        



for (Row row : results1) 
{
                 
                String keyval=getVal(ClusterConn,KeySpace,tableName1, columnName1, row);
              

		String query2= createSelectClause(selectColumns2, columnName2) + "\n" 		+ 
		"FROM " + KeySpace + "." + tableName2 + "\n" +
		createWhereClause(whereRelations2,columnName2,keyval,operator) ;

		if (rowsLimit!=0)
	         	  query2+= " LIMIT " + rowsLimit ;


		ResultSet results2 = session.execute(query2);


		for (Row row2 : results2) 
		{
  		     String inserts=join2rows(ClusterConn,KeySpace,tableName1,selectColumns1,columnName1,tableName2,selectColumns2,columnName2,  resultTableName, resultColumns, row, row2);
                     //session.execute(inserts);
		}

}

long millis = System.currentTimeMillis() - startTime;
long second = (millis / 1000) % 60;
long minute = (millis / (1000 * 60)) % 60;
long hour = (millis / (1000 * 60 * 60)) % 24;
millis = millis % 1000;
String time = String.format("%02d hours %02d min %02d sec %d msec", hour, minute, second, millis);
	System.out.println("Elapsed time:"+time);
}




private static String[]  getColumnNames(Cluster ClusterConn, String KeySpace, String tableName) throws CassandraJoinsException
{

	String[] selectColumns=null;

if (KeySpace==null)
	throw new CassandraJoinsException("Null KeySpace parameter");

if (tableName==null) 
	throw new CassandraJoinsException("Null table name parameter");

if (!hasTable(ClusterConn , KeySpace, tableName) )
   throw new CassandraJoinsException("Table not found. Keyspace: " + KeySpace + "Table: " + tableName);

	List<ColumnMetadata> l= ClusterConn.getMetadata().getKeyspace(KeySpace).getTable(tableName).getColumns();

  	List<String> sc=new ArrayList<String>();

	ListIterator<ColumnMetadata> i = l.listIterator();


	do
	{
	ColumnMetadata c = i.next();
		sc.add(c.getName());
	}
	while (i.hasNext());

  selectColumns=new String[sc.size()];
  sc.toArray(selectColumns);

return selectColumns;
}


private static String checkWhereRelations(Cluster ClusterConn ,String KeySpace, String tableName, String[] whereRelations)
{
for (int i=0;i< whereRelations.length;i++)
   {
	if (
                 (!whereRelations[i].contains("=")) 
             && (!whereRelations[i].contains(" in ")) && (!whereRelations[i].contains(" IN "))
             &&  (!whereRelations[i].contains("<")) 
             &&  (!whereRelations[i].contains(">")) 
           )

           return new  String(whereRelations[i] + " doesn't contain  operator: IN, =, < , > , <= , >=");

        String colname=null;

         int eq_pos=whereRelations[i].indexOf("=");
         int lt_pos=whereRelations[i].indexOf("<");
         int gt_pos=whereRelations[i].indexOf(">");
         int lteq_pos=whereRelations[i].indexOf("<=");
         int gteq_pos=whereRelations[i].indexOf(">=");
         int IN_pos=whereRelations[i].indexOf(" IN ");
         int in_pos=whereRelations[i].indexOf(" in ");

         PriorityQueue<Integer> pq= new PriorityQueue<Integer>();

         if (eq_pos!=-1)
           pq.add(eq_pos);
         
          if (lt_pos!=-1)
           pq.add(lt_pos);

          if (gt_pos!=-1)
           pq.add(gt_pos);

          if (lteq_pos!=-1)
           pq.add(lteq_pos);

          if (gteq_pos!=-1)
           pq.add(gteq_pos);

          if (IN_pos!=-1)
           pq.add(IN_pos);

          if (in_pos!=-1)
           pq.add(in_pos);

          int minval=pq.peek();

        if (minval==eq_pos)
         	colname=whereRelations[i].split("=")[0].replaceAll("\\s","");
	else if (minval==in_pos) 
       		colname=whereRelations[i].split(" in ")[0].replaceAll("\\s","");
	else if (minval==IN_pos) 
       		colname=whereRelations[i].split(" IN ")[0].replaceAll("\\s","");
        else if (minval==lt_pos)
         	colname=whereRelations[i].split("<")[0].replaceAll("\\s","");
        else if (minval==gt_pos)
         	colname=whereRelations[i].split(">")[0].replaceAll("\\s","");
        else if (minval==lteq_pos)
         	colname=whereRelations[i].split("<=")[0].replaceAll("\\s","");
        else if (minval==gteq_pos)
         	colname=whereRelations[i].split(">=")[0].replaceAll("\\s","");


        if (!hasColumn(ClusterConn , KeySpace, tableName,colname ))
           return new String("Column not found in WHERE clause. Keyspace: " + KeySpace + "Table: " + tableName + " Column: " + colname);
   }
return null;
}

public static void join(Cluster ClusterConn , String KeySpace, String tableName1, String columnName1, String[] selectColumns1, String[] whereRelations1, String tableName2, String columnName2, String[] selectColumns2, String[] whereRelations2, long rowsLimit, String resultTableName, String operator) throws CassandraJoinsException
{
boolean key1p, key2p, key1i, key2i,key1ap,key2ap;

boolean ckey1, ckey2;

if (rowsLimit <=0)
   throw new CassandraJoinsException("Unacceptable rows limit number:"+rowsLimit) ;

if ((tableName1==null) || (tableName2==null))
	throw new CassandraJoinsException("Null table name parameter");

if ((columnName1==null) || (columnName2==null))
	throw new CassandraJoinsException("Null column name parameter");

if (KeySpace==null)
	throw new CassandraJoinsException("Null KeySpace parameter");

if (resultTableName==null)
	throw new CassandraJoinsException("Null result tablename parameter");

if (!hasTable(ClusterConn , KeySpace, tableName1) )
   throw new CassandraJoinsException("Table not found. Keyspace: " + KeySpace + "Table: " + tableName1);
    
if (!hasTable(ClusterConn , KeySpace, tableName2) )
   throw new CassandraJoinsException("Table not found. Keyspace: " + KeySpace + "Table: " + tableName2);

if (selectColumns1!=null)
{
for (int i=0;i< selectColumns1.length;i++)
	if (!hasColumn(ClusterConn , KeySpace, tableName1,selectColumns1[i]) )
   throw new CassandraJoinsException("Column not found. Keyspace: " + KeySpace + "Table: " + tableName1 + " Column:" + selectColumns1[i]);
}

if (selectColumns2!=null)
{
for (int i=0;i< selectColumns2.length;i++)
	if (!hasColumn(ClusterConn , KeySpace, tableName2,selectColumns2[i]) )
   throw new CassandraJoinsException("Column not found. Keyspace: " + KeySpace + "Table: " + tableName2 + " Column:" + selectColumns2[i]);
}


if (!hasColumn(ClusterConn , KeySpace, tableName1,columnName1) )
   throw new CassandraJoinsException("Column not found. Keyspace: " + KeySpace + "Table: " + tableName1 + " Column:" + columnName1);

if (!hasColumn(ClusterConn , KeySpace, tableName2,columnName2) )
   throw new CassandraJoinsException("Column not found. Keyspace: " + KeySpace + "Table: " + tableName1 + " Column:" + columnName2);

if (selectColumns1==null)
	selectColumns1=getColumnNames(ClusterConn,  KeySpace, tableName1);


if (selectColumns2==null)
	selectColumns2=getColumnNames(ClusterConn, KeySpace, tableName2);

if (whereRelations1!=null)
{
String error=checkWhereRelations(ClusterConn, KeySpace, tableName1, whereRelations1);

	if (error!=null)
           throw new  CassandraJoinsException(error);
}

if (whereRelations2!=null)
{
String error=checkWhereRelations(ClusterConn, KeySpace, tableName2, whereRelations2);

	if (error!=null)
           throw new  CassandraJoinsException(error);
}

if (operator==null)
           throw new  CassandraJoinsException("Join operator  must be = or CONTAINS");

if (!operator.equals("=") && !operator.equals("CONTAINS"))
           throw new  CassandraJoinsException("Join operator  must be = or CONTAINS");

String ctype1=getColumnType(ClusterConn,KeySpace,tableName1, columnName1);
String ctype2=getColumnType(ClusterConn,KeySpace,tableName2, columnName2);

if (operator.equals("="))
{
if (isCollectionType(ctype1) || isCollectionType(ctype2))
           throw new  CassandraJoinsException("Joins using operator = can't be performed to  columns with type:set, map, list");
}

if (operator.equals("CONTAINS"))
{
if (isCollectionType(ctype1) )
           throw new  CassandraJoinsException("Joins using operator CONTAINS require the joining column of the first table to not be of type:set, map, list");

if (!isCollectionType(ctype2))
           throw new  CassandraJoinsException("Joins using operator CONTAINS require the joining column of the second table to not be on of types:set, map, list");
}


if (operator.equals("="))
{

ckey1=IsClusteringKey(ClusterConn , KeySpace, tableName1, columnName1);
ckey2=IsClusteringKey(ClusterConn , KeySpace, tableName2, columnName2);
if ((ckey1==true) && (ckey2==true))
{
       if ( hasAllPartitionKeys(ClusterConn, KeySpace, tableName1,whereRelations1,null) && hasAllPrevClusteringKeys(ClusterConn, KeySpace, tableName1,whereRelations1,columnName1)
             && hasAllPartitionKeys(ClusterConn, KeySpace, tableName2,whereRelations2,null) && hasAllPrevClusteringKeys(ClusterConn, KeySpace, tableName2,whereRelations2,columnName2))
        {
	   String resultColumns=create_join_table(ClusterConn , KeySpace, tableName1, columnName1, selectColumns1,   tableName2,  columnName2,  selectColumns2,   resultTableName) ;
	  sortmergejoin(ClusterConn ,  KeySpace,  tableName1,  columnName1, selectColumns1, whereRelations1, tableName2,  columnName2, selectColumns2, whereRelations2, rowsLimit, resultTableName, resultColumns);
          return;
        }
}
}
	
key1p=IsSinglePartitionKey(ClusterConn , KeySpace, tableName1, columnName1);
key2p=IsSinglePartitionKey(ClusterConn , KeySpace, tableName2, columnName2);
	
key1i=IsIndexedColumn(ClusterConn , KeySpace, tableName1, columnName1);
key2i=IsIndexedColumn(ClusterConn , KeySpace, tableName2, columnName2);

key1ap=hasAllPartitionKeys(ClusterConn, KeySpace, tableName1,whereRelations1,columnName1);
key2ap=hasAllPartitionKeys(ClusterConn, KeySpace, tableName2,whereRelations2,columnName2);


if (operator.equals("="))
{

if ( key2p || key2i || key2ap)
{
	 String resultColumns=create_join_table(ClusterConn , KeySpace, tableName1, columnName1, selectColumns1,   tableName2,  columnName2,  selectColumns2,   resultTableName) ;
	 indexjoin(ClusterConn ,  KeySpace,  tableName1,  columnName1, selectColumns1, whereRelations1, tableName2,  columnName2, selectColumns2, whereRelations2, rowsLimit, resultTableName, resultColumns,"=");
	 return;
}

if (key1p || key1i || key1ap)
{ 
	  String resultColumns=create_join_table(ClusterConn , KeySpace, tableName1, columnName1, selectColumns1,   tableName2,  columnName2,  selectColumns2,   resultTableName) ;
	  indexjoin(ClusterConn ,  KeySpace,  tableName2,  columnName2, selectColumns2, whereRelations2, tableName1,  columnName1, selectColumns1, whereRelations1, rowsLimit, resultTableName, resultColumns,"=");
	  return;
}
}

if (operator.equals("CONTAINS"))
{

if (  key2i || key2ap)
{
	 String resultColumns=create_join_table(ClusterConn , KeySpace, tableName1, columnName1, selectColumns1,   tableName2,  columnName2,  selectColumns2,   resultTableName) ;
	 indexjoin(ClusterConn ,  KeySpace,  tableName1,  columnName1, selectColumns1, whereRelations1, tableName2,  columnName2, selectColumns2, whereRelations2, rowsLimit, resultTableName, resultColumns, "CONTAINS");
	 return;
}

}

if (operator.equals("="))
   throw new CassandraJoinsException("An index  on joining column is required to at least one of the tables to perform joins . Keyspace: " + KeySpace + " Table 1: " + tableName1 + " Join Column 1:"+columnName1 + " Table 2:" + tableName2 + " Join Column 2:" + columnName2);

if (operator.equals("CONTAINS"))
   throw new CassandraJoinsException("An index  on joining column is required to table 2  to perform joins on collections . Keyspace: " + KeySpace + " Table 1: " + tableName1 + " Join Column 1:"+columnName1 + " Table 2:" + tableName2 + " Join Column 2:" + columnName2);

}


}
