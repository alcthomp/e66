/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import com.sleepycat.db.*;
import com.sleepycat.bind.*;
import com.sleepycat.bind.tuple.*;
import java.lang.*;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-data pair in the underlying
 * BDB database.
 */
public class InsertRow {
    private Table table;         // the table in which the row will be inserted
    private Object[] values;     // the individual values to be inserted
    private DatabaseEntry key;   // the key portion of the marshalled row
    private DatabaseEntry data;  // the data portion of the marshalled row
	private Object NULL;
   
    /**
     * Constructs an InsertRow object for a row containing the specified
     * values that is to be inserted in the specified table.
     *
     * @param  t  the table
     * @param  values  the values in the row to be inserted
     */
    public InsertRow(Table table, Object[] values) {
        this.table = table;
        this.values = values;
        
        // These objects will be created by the marshall() method.
        this.key = null;
        this.data = null;
    }
    
    /**
     * Takes the collection of values for this InsertRow
     * and marshalls them into a key/data pair.
     */
    public void marshall() {
		//Make Offset array and test for primary key
    	
		int pKey = -3;
        int[] offsetArray = new int[table.numColumns()];
		for (int i=0; i < offsetArray.length ; i++){
			if(table.getColumn(i).isPrimaryKey()){ 
				pKey = i; 
				offsetArray[i] = -1;
			} else if (values[i] == null){
				offsetArray[i] = -2;
			} else if (table.getColumn(i).getType() == 3){	
				String str = (String)values[i];
				offsetArray[i] = str.length();
				
			} else {
				offsetArray[i] = table.getColumn(i).getLength();	
			}
		}

		//Write Primary Key buffer and save to databaseEntry object
		
		if (pKey != -3){
			try{
				TupleOutput keyBuffer = new TupleOutput();
				switch (table.getColumn(pKey).getType()){
					case 0: keyBuffer.writeInt(((Integer)(values[pKey]))); break;
					case 1: keyBuffer.writeDouble((Double)values[pKey]); break;
					case 2: keyBuffer.writeBytes((String)values[pKey]); break;
					case 3: keyBuffer.writeBytes((String)values[pKey]); break;
				}
				this.key = new DatabaseEntry(keyBuffer.getBufferBytes(), 0,keyBuffer.getBufferLength());
		
				keyBuffer.close();
	        } catch (Exception e) {
	            System.err.println("An error occured writing the primary key");
	            System.out.println(e);
	        }
		}

		
		//Write Offset header to valuebuffer
		
		int offsetTotal = offsetArray.length*4+4;
		TupleOutput valueBuffer = new TupleOutput();
		valueBuffer.writeInt(offsetArray.length*4+4);
		
		for (int i=0; i < offsetArray.length ; i++){
			if (offsetArray[i] == -1){
				valueBuffer.writeInt(-1);
				continue;
			} else if (offsetArray[i] == -2){
				valueBuffer.writeInt(-2);
				continue;
			} else if (i != 0){
				offsetTotal = offsetTotal + offsetArray[i];
				valueBuffer.writeInt(offsetTotal);
				}
		}
		//Write actual values to Buffer and save valuebuffer as a databaseEntry 
		
		for (int i=0; i < offsetArray.length ; i++){
			if (!(table.getColumn(i).isPrimaryKey())){
				switch (table.getColumn(i).getType()){
					case 0: valueBuffer.writeInt((Integer)values[i]); break;
					case 1: valueBuffer.writeDouble((Double)values[i]); break;
					case 2: valueBuffer.writeBytes((String)values[i]); break;
					case 3: valueBuffer.writeBytes((String)values[i]); break;
				}
			}
		}
        this.data = new DatabaseEntry(valueBuffer.getBufferBytes(), 0, valueBuffer.getBufferLength());
        try{
        	valueBuffer.close();
	    } catch (Exception e) {
	        System.err.println("buffer could not close, if problem presist contact administrator");
	        System.out.println(e);
	    }
    }
    
    /**
     * Returns the DatabaseEntry for the key in the key/data pair for this row.
     *
     * @return  the key DatabaseEntry
     */
    public DatabaseEntry getKey() {
        return this.key;
    }
    
    /**
     * Returns the DatabaseEntry for the data item in the key/data pair 
     * for this row.
     *
     * @return  the data DatabaseEntry
     */
    public DatabaseEntry getData() {
        return this.data;
    }
}
