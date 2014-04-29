package simpledb.parse;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {

   private String idxname, tblname, fldname, idtype;

   /**
    * Saves the table and field names of the specified index.
    */

   public CreateIndexData(String idtype, String tblname, String fldname, String idxname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.idtype = idtype;

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

   public String indexType() {
		return idtype;
	}

}

