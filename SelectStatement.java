/*
 * SelectStatement.java
 *
 * DBMS Implementation
 */

import java.util.*;
import com.sleepycat.db.*;

/**
 * A class that represents a SELECT statement.
 */
public class SelectStatement extends SQLStatement {
    /* Used in the selectList for SELECT * statements. */
    public static final String STAR = "*";
    
    private ArrayList<Object> selectList;
    private Limit limit;
    private boolean distinctSpecified;
    
    /** 
     * Constructs a SelectStatement object involving the specified
     * columns and other objects from the SELECT clause, the specified
     * tables from the FROM clause, the specified conditional
     * expression from the WHERE clause (if any), the specified Limit
     * object summarizing the LIMIT clause (if any), and the specified
     * value indicating whether or not we should eliminate duplicates.
     *
     * @param  selectList  the columns and other objects from the SELECT clause
     * @param  fromList  the list of tables from the FROM clause
     * @param  where  the conditional expression from the WHERE clause (if any)
     * @param  limit  summarizes the info in the LIMIT clause (if any)
     * @param  distinctSpecified  should duplicates be eliminated?
     */
    public SelectStatement(ArrayList<Object> selectList, 
                           ArrayList<Table> fromList, ConditionalExpression where,
                           Limit limit, Boolean distinctSpecified)
    {
        super(fromList, new ArrayList<Column>(), where);
        this.selectList = selectList;
        this.limit = limit;
        this.distinctSpecified = distinctSpecified.booleanValue();
        
        /* add the columns in the select list to the list of columns */
        for (int i = 0; i < selectList.size(); i++) {
            Object selectItem = selectList.get(i);
            if (selectItem instanceof Column)
                this.addColumn((Column)selectItem);
        }
    }
    
    /**
     * Returns a boolean value indicating whether duplicates should be
     * eliminated in the result of this statement -- i.e., whether the
     * user specified SELECT DISTINCT.
     */
    public boolean distinctSpecified() {
        return this.distinctSpecified;
    }
    
    public void execute() throws DatabaseException, DeadlockException {
        try {
            // Remove the table's information from the catalog.
        	if (selectList.size() == 1 && super.numTables() == 1){
        		super.getTable(0).open();
        		TableIterator ti = new TableIterator(this, super.getTable(0), super.getWhere() != null); 
        		ti.printAll(System.out);
        		
        		System.out.println("the number of Tuples selected: " + ti.numTuples());
        		ti.close();
        		
        	}else { 
        		throw new Exception("Select and From must only include one variable");
        	}
            
        } catch (Exception e) {
            System.err.println("Could not access table ");
            System.out.println(e);
        }
    	
        /* not yet implemented */
    }
}
