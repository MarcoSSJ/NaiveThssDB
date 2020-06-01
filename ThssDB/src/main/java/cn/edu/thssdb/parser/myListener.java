package cn.edu.thssdb.parser;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class myListener extends SQLBaseListener{
	private Manager manager;
	//private Database database;
	long sessionId;
	private ExecuteStatementResp resp = new ExecuteStatementResp();
	private Status status = new Status();
	private boolean success = true;

	public void setSessionId(long sessionId){
		this.sessionId = sessionId;
	}

	@Override
	public void enterParse(SQLParser.ParseContext ctx){
		//进入数据库操作，对应g4文件3-4行parse
		manager = Manager.getInstance();
	}

	@Override
	public void exitCommit_stmt(SQLParser.Commit_stmtContext ctx){

	}

	@Override
	public void exitTransaction_stmt(SQLParser.Transaction_stmtContext ctx){

	}

	@Override
	public void exitShow_db_stmt(SQLParser.Show_db_stmtContext ctx){
		//展示数据库，需要接口

	}

	@Override
	public void exitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx){
		try {
			String dbName = ctx.database_name().getText();
			//看一看是否存在该数据库，不存在创建，存在报错，需要接口
			manager.createDatabaseIfNotExists(dbName);
		}
		catch (Exception e)
		{}
	}

	@Override
	public void exitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx){
		try{
			String dbName = ctx.database_name().getText();
			//看一看是否存在该数据库，不存在报错，存在删除，需要接口
			manager.deleteDatabase(dbName);
		}
		catch (Exception e)
		{}
	}

	@Override
	public void exitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {

		String dbName = ctx.database_name().getText();
		System.out.println(sessionId+" use "+dbName);
		//改变数据库
		manager.use(sessionId, dbName);
	}

	@Override
	public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
		String tableName = ctx.table_name().getText();
		//删除整个表
		Database database = manager.getDatabase(sessionId);
		try {
			database.drop(tableName);
			database.write();
		}
		catch (IOException e) {
			//TODO:删除失败
		}

	}

	@Override
	public void exitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx){
		try {
			String tableName = ctx.table_name().getText();
			List<SQLParser.Column_defContext> columnDefCtxs = ctx.column_def();
			int numOfColumns = columnDefCtxs.size();
			Column[] columns = new Column[numOfColumns];
			String primary = "";
			//先处理主键问题
			List<SQLParser.Column_nameContext> table_constraintContexts = ctx.table_constraint().column_name();
			if(!table_constraintContexts.isEmpty())
			{
				List<SQLParser.Column_nameContext> column_nameContexts = ctx.table_constraint().column_name();
				primary = column_nameContexts.get(0).getText();
			}
			for(int i=0;i<numOfColumns;i++)
			{
				SQLParser.Column_defContext column_defContext = columnDefCtxs.get(i);
				String columnName = column_defContext.column_name().getText();
				String typeRaw = column_defContext.type_name().getText();
				//预处理type
				typeRaw = typeRaw.toUpperCase();
				typeRaw.replaceAll(" ","");
				StringBuilder typeLength = new StringBuilder("32");
				if(typeRaw.charAt(0)=='S')
				{
					typeLength = new StringBuilder();
					//识别预设字符串长度
					for(int j=7;j<typeRaw.length()-1;j++)
					{
						typeLength.append(typeRaw.charAt(j));
					}
					typeRaw = "STRING";
				}
				int maxLength = Integer.parseInt(typeLength.toString());
				ColumnType columnType = ColumnType.valueOf(typeRaw);
				boolean notNull = false;
				List<SQLParser.Column_constraintContext> column_constraintContexts = column_defContext.column_constraint();
				if(!column_constraintContexts.isEmpty())
				{
					String columnConstraint = column_constraintContexts.get(0).getText();
					if (columnConstraint.toUpperCase().equals("NOTNULL"))
					{
						notNull = true;
					}
				}
				if(columnName.equals(primary))
					columns[i] = new Column(columnName,columnType,1, notNull, maxLength);
				else
					columns[i] = new Column(columnName,columnType,0, notNull, maxLength);
			}
			Database db = manager.getDatabase(sessionId);
			db.create(tableName, columns);
			db.write();
			//manager.database.create(tableName, columns);
		}
		catch (Exception e)
		{

		}
	}


	/*
	insert_stmt :
    K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
        K_VALUES value_entry ( ',' value_entry )* ;
    */
	@Override
	public void exitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
		//TODO:检测主键为空和重复主键的问题

		String tableName = ctx.table_name().getText();
		Database database = manager.getDatabase(sessionId);


		/*
		value_entry :
    	'(' literal_value ( ',' literal_value )* ')' ;
    	*/
		//简单版本只取第一个value_entry
		String origin_value = ctx.value_entry(0).getText().trim();//去空格
		StringBuilder literal_value = new StringBuilder();
		for(int i=1;i<origin_value.length()-1;i++)//去括号
			literal_value.append(origin_value.charAt(i));

		String[] entry_value = literal_value.toString().split(",");//这里拿到每个属性值
		int entry_num = entry_value.length;

		Table currentTable = database.getTable(tableName);

		Entry[] entry = new Entry[entry_num];
		for(int i=0;i<entry_num;i++)
			entry[i] = new Entry(entry_value[i]);

		Row insertRow;

		List<SQLParser.Column_nameContext> column_names = ctx.column_name();
		//看是否是默认插入，INSERT INTO person VALUES (‘Bob’, 15)或INSERT INTO person(name) VALUES (‘Bob’)
		int column_num = column_names.size();
		if(column_num == 0)
		{
			// 默认输入，类似INSERT INTO person VALUES (‘Bob’, 15)，entries不调整
			insertRow = new Row(entry);
		}
		else
		{
			//类似INSERT INTO person(name) VALUES (‘Bob’)
			String[] column_name = new String[column_num];
			for(int i=0;i<column_num;i++)//拿到列名
				column_name[i] = column_names.get(i).getText();

			int table_column_num = currentTable.columns.size();
			Entry[] table_entry = new Entry[table_column_num];
			for(int i=0;i<table_column_num;i++)
				table_entry[i] = new Entry(null);

			for(int i=0;i<column_num;i++)
			{
				//check every column
				int index;
				//这里在找插入的是哪一个属性
				for(index = 0; index < table_column_num; index++)
					if(currentTable.columns.get(index).name.equals(column_name[i]))
						break;
				table_entry[index] = new Entry(entry[i]);
			}
			insertRow = new Row(table_entry);
		}
		try {
			currentTable.insert(insertRow);
			currentTable.write();
		}
		catch (IOException e){
			//TODO:exception
		}
	}

	@Override
	public void exitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
		String tableName = ctx.table_name().getText();
		Database database = manager.getDatabase(sessionId);
		Table currentTable = database.getTable(tableName);
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		//System.out.println(comparator);
		String attrName = ctx.multiple_condition().condition().expression(0).getText();
		String attrValue = ctx.multiple_condition().condition().expression(1).getText();
		//System.out.println(attrName);
		//System.out.println(attrValue);
		try {
			currentTable.delete(comparator, attrName, attrValue);
			currentTable.write();
		}
		catch(IOException e)
		{
			//TODO: exception
		}
	}

	@Override
	public void exitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
		// 更新哪个表
		String tableName = ctx.table_name().getText();
		Database database = manager.getDatabase(sessionId);
		System.out.println(tableName);
		// 更新哪一列
		String attrToBeUpdated = ctx.column_name().getText();
		System.out.println(attrToBeUpdated);
		// 更新为何值
		String valueTobeUpdated = ctx.expression().getText();
		System.out.println(valueTobeUpdated);
		Table currentTable = database.getTable(tableName);
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		String attrName = ctx.multiple_condition().condition().expression(0).getText();
		String attrValue = ctx.multiple_condition().condition().expression(1).getText();
		try {
			currentTable.update(attrToBeUpdated, valueTobeUpdated, comparator, attrName, attrValue);
			currentTable.write();
		}
		catch(IOException e)
		{

		}
	}

	@Override
	public void exitSelect_stmt(SQLParser.Select_stmtContext ctx){
        /*
        select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
        K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
        */
		Database database = manager.getDatabase(sessionId);
		resp.rowList = new ArrayList<>();
		resp.columnsList = new ArrayList<>();
		ArrayList<String> resultTables = new ArrayList<>();//列来自什么表 tablename
		ArrayList<String> resultColumns = new ArrayList<>();//查询哪些列 columnname
		ArrayList<Integer> resultIndex = new ArrayList<>();
		List<SQLParser.Result_columnContext> result_columnContexts = ctx.result_column();
		ArrayList<String> resultRows = new ArrayList<>(); //查询结果

		//select部分
		//TODO:select *,设置selectAll = true
		//TODO:select table_name.*...，table名放在resultTables里面,resultColumns里面放一个*
		//TODO:select column_name,resultTables里面放*，column名放在resultColumns里面
		//TODO:select table_name.column_name...，table名放在resultTables里面，column名放在resultColumns里面
        /*
        result_column
        : '*'
        | table_name '.' '*'
        | column_full_name;
        */
		boolean selectAll = false;
		if(result_columnContexts.get(0).getText().equals("*"))
		{
			//select *
			selectAll = true;
		}
		else
		{
			for(SQLParser.Result_columnContext result_columnContext : result_columnContexts)
			{
				if(result_columnContext.table_name() != null)
				{
					//table_name.*
					resultTables.add(result_columnContext.table_name().getText());
					resultColumns.add("*");
				}
				else
				{
					//column_full_name
                    /*
                    column_full_name:
                    ( table_name '.' )? column_name ;
                    */
					if(result_columnContext.column_full_name().table_name() == null)
					{
						//column_name
						resultTables.add("*");
						resultColumns.add(result_columnContext.column_full_name().column_name().getText());
						//System.out.println("result tables: *");
					}
					else
					{
						//table_name.column_name
						resultTables.add(result_columnContext.column_full_name().table_name().getText());
						resultColumns.add(result_columnContext.column_full_name().column_name().getText());
					}
					//System.out.println(result_columnContexts.get(i).column_full_name().column_name().getText());
				}
			}
		}

		//from部分
		//TODO:如果没有join，则是单表查询，isSingleTable为true，表名放在sigleTableName内，其他为空
		//TODO：有join（两个表），on条件分别是leftTableName.leftArrtName=rightTableName.rightTableAttrName
        /*
        table_query :
        table_name
        | table_name ( K_JOIN table_name )+ K_ON multiple_condition ;
        */
		boolean isSingleTable = false; //单表查询
		String sigleTableName = "";
		String leftTableName = "";
		String rightTableName = "";
		String leftTableAttrName = "";
		String rightTableAttrName = "";
		String temp = "";
		if(ctx.table_query(0).table_name().size()==1)
		{
			//table_name
			isSingleTable = true;
			sigleTableName = ctx.table_query(0).table_name(0).getText();
		}
		else
		{
			//拿到哪两个表做join
			leftTableName = ctx.table_query(0).table_name(0).getText();
			rightTableName = ctx.table_query(0).table_name(1).getText();

			//解析on tableName1.attrName1 = tableName2.attrName2
			leftTableAttrName = ctx.table_query(0)
					.multiple_condition()
					.condition()
					.expression(0)
					.comparer()
					.column_full_name()
					.column_name()
					.getText();
			rightTableAttrName = ctx.table_query(0)
					.multiple_condition()
					.condition()
					.expression(1)
					.comparer()
					.column_full_name()
					.column_name()
					.getText();
			temp = ctx.table_query(0)
					.multiple_condition()
					.condition()
					.expression(0)
					.comparer()
					.column_full_name()
					.table_name()
					.getText();
			//如果顺序是反的，反过来，保证left对应left，right对应right
			if(!leftTableName.equals(temp))
			{
				temp = leftTableName;
				leftTableName = rightTableName;
				rightTableName = temp;
			}
		}

		//TODO:只做只有一个where条件的情况，whereAttrName whereComparator whereAttrValue
		//where attrName = attrValue
        /*
        multiple_condition :
        condition
        | multiple_condition AND multiple_condition
        | multiple_condition OR multiple_condition ;
        */
		String whereAttrName = null;
		String whereComparator = null;
		String whereAttrValue = null;
		boolean hasWhere = true;
		if(ctx.multiple_condition()!=null)
		{
			whereAttrName = ctx.multiple_condition()
					.condition()
					.expression(0)
					.comparer()
					.column_full_name()
					.column_name()
					.getText();
			whereComparator = ctx.multiple_condition()
					.condition()
					.comparator()
					.getText();
			whereAttrValue = ctx.multiple_condition()
					.condition()
					.expression(1)
					.comparer()
					.literal_value()
					.getText();
		}
		else
		{
			hasWhere = false;
		}

		if(isSingleTable)
		{
			Table table = database.getTable(sigleTableName);
			//TODO:单表查询
			if(selectAll)
			{
				for(int i = 0; i < table.columns.size(); i++)
				{
					resp.columnsList.add(table.columns.get(i).getName());
				}
				if(hasWhere == false)
				{
					resultRows = table.select();
				}
				else
				{
					resultRows = table.select(whereComparator, whereAttrName, whereAttrValue);
				}
			}
			else
			{
				for(int i = 0; i < resultColumns.size(); i++)
				{
					resultIndex.add(table.getIndex(resultColumns.get(i)));
				}
				resp.columnsList.addAll(resultColumns);
				if(hasWhere == false)
				{
					resultRows = table.select();
				}
				else
				{
					resultRows = table.select(whereComparator, whereAttrName, whereAttrValue);
				}
			}

		}
		else
		{
			try {
				Table leftTable = database.getTable(leftTableName);
				Table rightTable = database.getTable(rightTableName);
				QueryResult queryResult = new QueryResult(leftTable, rightTable, leftTableAttrName, rightTableAttrName);
				//TODO：多表查询
				if (selectAll) {
					resultRows = queryResult.newTable.select();
				} else {
					if (hasWhere == false) {
						resultRows = queryResult.newTable.select();
					} else {
						resultRows = queryResult.newTable.select(whereComparator, whereAttrName, whereAttrValue);
					}
				}
			}
			catch (Exception e)
			{
				//TODO: exception

			}
		}
		//System.out.println(resultRows.toString());
		if(selectAll) {
			for (int i = 0; i < resultRows.size(); i++) {
				resp.rowList.add(Arrays.asList(resultRows.get(i).split(",")));
			}
		}
		else{
			for (int i = 0; i < resultRows.size(); i++) {
				ArrayList<String> row = new ArrayList<>();
				List<String> oldRow = Arrays.asList(resultRows.get(i).split(","));
				for(int j = 0; j < resultIndex.size(); j++){
					row.add(oldRow.get(resultIndex.get(j)));
				}
				resp.rowList.add(row);
			}
		}
		//resp.rowList.add(resultRows);
		//System.out.println(resp.rowList.toString());
	}

	public ExecuteStatementResp getResult(){
		if(success){
			status.setCode(Global.SUCCESS_CODE);
			resp.hasResult = true;
		}
		else{
			status.setCode(Global.FAILURE_CODE);
		}
		resp.setStatus(status);
		return resp;
	}

	@Override
	public void exitParse(SQLParser.ParseContext ctx){
		//退出数据库操作，进行持久化等操作，需要接口
		Database database = manager.getDatabase(sessionId);
		try {
			database.quit();
		}
		catch (Exception e)
		{}
	}

}
