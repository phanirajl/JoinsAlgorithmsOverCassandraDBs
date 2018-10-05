package CassandraJoins;

/**
 * Exception class for CassandraJoins JAVA API 
 * 
 * @author Fountouris Antonios
 */
public class CassandraJoinsException extends Exception
{
private static final long serialVersionUID = 7526472295622776147L;

      public CassandraJoinsException() {}

      public CassandraJoinsException(String message)
      {
         super(message);
      }
 }


