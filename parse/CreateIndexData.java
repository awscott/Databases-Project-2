package simpledb.parse;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
<<<<<<< HEAD
   private String idxname, tblname, fldname, idtype;
=======
   private String idxname, tblname, fldname;
>>>>>>> 5712dc26842fedf189c49134eb0801334dfdd73a
   
   /**
    * Saves the table and field names of the specified index.
    */
<<<<<<< HEAD
   public CreateIndexData(String idtype, String tblname, String fldname, String idxname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.idtype = idtype;
=======
   public CreateIndexData(String idxname, String tblname, String fldname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
>>>>>>> 5712dc26842fedf189c49134eb0801334dfdd73a
   }
   
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
<<<<<<< HEAD

   public String indexType() {
		return idtype;
	}
=======
>>>>>>> 5712dc26842fedf189c49134eb0801334dfdd73a
}

