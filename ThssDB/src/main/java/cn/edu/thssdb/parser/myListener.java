package cn.edu.thssdb.parser;
import cn.edu.thssdb.exception.RowExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
//import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class myListener extends SQLBaseListener{
	private Manager manager;
	//private Database database;
	long sessionId;
	private final ExecuteStatementResp resp = new ExecuteStatementResp();
	private final Status status = new Status();
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
		Database database = manager.getDatabase(sessionId);
		try {
			database.write();
		}
		catch (IOException e){
			//TODO:
		}
	}

	@Override
	public void exitTransaction_stmt(SQLParser.Transaction_stmtContext ctx){
		manager.beginTransaction(sessionId);
	}

	@Override
	public void exitRollback_stmt(SQLParser.Rollback_stmtContext ctx){
		// TODO:rollback
		Database database = manager.getDatabase(sessionId);
		try {
			database.rollback();
		}
		catch (IOException e){
			//TODO:
		}
		catch (ClassNotFoundException e){

		}
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
		//改变数据库
		manager.use(sessionId, dbName);
	}

	@Override
	public void exitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx){
		String tableName = ctx.table_name().getText();
		Database database = manager.getDatabase(sessionId);
		Table table = database.getTable(tableName);

		String[] columns = {"name", "type", "primary", "notNull", "maxLength"};
		resp.columnsList = new ArrayList<>();
		resp.columnsList.addAll(Arrays.asList(columns));
		resp.rowList = new ArrayList<>();
		for(int i = 0; i < table.columns.size(); i++){
			Column column = table.columns.get(i);
			resp.rowList.add(Arrays.asList(column.toString().split(",")));
		}
		resp.hasResult = true;
	}

	@Override
	public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
		String tableName = ctx.table_name().getText();
		//删除整个表
		Database database = manager.getDatabase(sessionId);
		try {
			database.drop(tableName);
			if(!manager.isTransaction(sessionId)) {
				database.write();
			}
		}
		catch (IOException e) {
			//TODO:删除失败
		}

	}

	@Override
	public void exitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx){
		try {
			String tableName = ctx.table_name().getText();
			List<SQLParser.Column_defContext> column_defContexts = ctx.column_def();
			int columnNum = column_defContexts.size();
			Column[] columns = new Column[columnNum];
			//先处理主键问题，仅支持后置单主键
			List<SQLParser.Column_nameContext> column_nameContexts = ctx.table_constraint().column_name();
			String primary_column = "";
			if(!column_nameContexts.isEmpty())
				primary_column = column_nameContexts.get(0).getText();

			for(int i=0;i<columnNum;i++)
			{
				SQLParser.Column_defContext column_defContext = column_defContexts.get(i);
				String column = column_defContext.column_name().getText();
				String type = column_defContext.type_name().getText();
				//预处理type
				type = type.toUpperCase();
				type = type.replaceAll(" ","");
				String size_str;
				if(type.length()>6 && type.contains("STRING"))//STRING类型
				{
					size_str = type.substring(7,type.length()-1);
					type = "STRING";
				}
				else
					size_str = "32";
				int size = Integer.parseInt(size_str);
				ColumnType columnType = ColumnType.valueOf(type);

				//仅支持NOTNULL一种限制
				boolean notNull = false;
				List<SQLParser.Column_constraintContext> column_constraintContexts = column_defContext.column_constraint();
				if(!column_constraintContexts.isEmpty())
					notNull = true;

				if(column.equals(primary_column))
					columns[i] = new Column(column,columnType,1, notNull, size);
				else
					columns[i] = new Column(column,columnType,0, notNull, size);
			}
			Database database = manager.getDatabase(sessionId);
			database.create(tableName, columns);
			if(!manager.isTransaction(sessionId)) {
				database.write();
			}
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

		Database database = manager.getDatabase(sessionId);
		Table table = database.getTable(ctx.table_name().getText());

		/*
		value_entry :
    	'(' literal_value ( ',' literal_value )* ')' ;
    	*/
		//简单版本只取第一个value_entry
		String origin_value = ctx.value_entry(0).getText().trim();//去空格
		//不知道这里为什么拿literal value拿到的是一串数字？？？
//		List<SQLParser.Literal_valueContext> literal_valueContexts = ctx.value_entry(0).literal_value();
//		System.out.println(literal_valueContexts.toString());
//		for(int i=0;i<literal_valueContexts.size();i++)//去括号
//			System.out.println(literal_valueContexts.get(i).toString());
		String literal_value;
		literal_value = origin_value.substring(1,origin_value.length()-1);
		String[] entry_value = literal_value.split(",");//这里拿到每个属性值

		int entry_num = entry_value.length;
		Entry[] entry = new Entry[entry_num];
		for(int i=0;i<entry_num;i++)
			entry[i] = new Entry(entry_value[i]);

		Row insertRow;

		List<SQLParser.Column_nameContext> column_names = ctx.column_name();
		//看是否是默认插入，INSERT INTO person VALUES (‘Bob’, 15)或INSERT INTO person(name) VALUES (‘Bob’)
		int column_num = column_names.size();
		if(column_num == 0)
			// 默认输入，类似INSERT INTO person VALUES (‘Bob’, 15)，entries不调整
			insertRow = new Row(entry);
		else
		{
			//类似INSERT INTO person(name) VALUES (‘Bob’)

			//拿到原有表中列的信息
			int table_column_num = table.columns.size();
			Entry[] table_entry = new Entry[table_column_num];
			for(int i=0;i<table_column_num;i++)
				table_entry[i] = new Entry(null);

			//拿到要插入的列的信息
			String[] column_name = new String[column_num];
			for(int i=0;i<column_num;i++)
				column_name[i] = column_names.get(i).getText();

			//找插入的是哪一个属性
			for(int i = 0;i<column_num;i++)
			{
				int j;
				for(j = 0; j < table_column_num; j++)
					if(table.columns.get(j).name.equals(column_name[i]))
						break;
				table_entry[j] = new Entry(entry[i].value);
			}
			insertRow = new Row(table_entry);
			for(int i = 0; i < table_column_num; i++){
				if(insertRow.getEntries().get(i).value==null&&table.columns.get(i).isNotNull()){
					this.success = false;
					status.setCode(Global.FAILURE_CODE);
					String msg = table.columns.get(i).getName() + " can not be null";
					status.setMsg(msg);
					return;
				}
			}
		}
		try {
			table.insert(insertRow);
			if(!manager.isTransaction(sessionId)) {
				//table.write();
			}
		}
		catch (RowExistException e){
			this.success = false;
			status.setCode(Global.FAILURE_CODE);
			String msg = "row has already exist";
			status.setMsg(msg);
		}
	}

	@Override
	public void exitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
		Database database = manager.getDatabase(sessionId);
		Table table = database.getTable(ctx.table_name().getText());
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		String attrName = ctx.multiple_condition().condition().expression(0).getText();
		String attrValue = ctx.multiple_condition().condition().expression(1).getText();
		try {
			table.delete(comparator, attrName, attrValue);
			if(!manager.isTransaction(sessionId)) {
				table.write();
			}
		}
		catch(IOException e)
		{
			//TODO: exception
		}
	}

	@Override
	public void exitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
		Database database = manager.getDatabase(sessionId);
		String attribute1 = ctx.column_name().getText();
		String value1 = ctx.expression().getText();
		Table table = database.getTable(ctx.table_name().getText());
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		String attribute2 = ctx.multiple_condition().condition().expression(0).getText();
		String value2 = ctx.multiple_condition().condition().expression(1).getText();
		try {
			table.update(attribute1, value1, comparator, attribute2, value2);
			if(!manager.isTransaction(sessionId)) {
				table.write();
			}
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
		List<SQLParser.Result_columnContext> result_columnContexts = ctx.result_column();
		ArrayList<String> resultRows = new ArrayList<>(); //查询结果

		//select部分
		//select *,设置selectAll = true
		//select table_name.*...，table名放在resultTables里面,resultColumns里面放一个*
		//select column_name,resultTables里面放*，column名放在resultColumns里面
		//select table_name.column_name...，table名放在resultTables里面，column名放在resultColumns里面
        /*
        result_column
        : '*'
        | table_name '.' '*'
        | column_full_name;
        */
		boolean selectAll = false;
		if(result_columnContexts.get(0).getText().equals("*"))
			//select *
			selectAll = true;
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
				}
			}
		}

		//from部分
		//如果没有join，则是单表查询，isSingleTable为true，single_table，其他为空
		//有join（两个表），left_table.left_attribute=right_table.right_attribute
        /*
        table_query :
        table_name
        | table_name ( K_JOIN table_name )+ K_ON multiple_condition ;
        */
		boolean isSingleTable = false; //单表查询
		String single_table = "";
		String left_table = "";
		String right_table = "";
		String left_attribute = "";
		String right_attribute = "";
		String temp;
		if(ctx.table_query(0).table_name().size()==1)
		{
			//table_name
			isSingleTable = true;
			single_table = ctx.table_query(0).table_name(0).getText();
		}
		else
		{
			//拿到哪两个表做join
			left_table = ctx.table_query(0).table_name(0).getText();
			right_table = ctx.table_query(0).table_name(1).getText();

			//解析on tableName1.attrName1 = tableName2.attrName2
			left_attribute = ctx.table_query(0).multiple_condition().condition().expression(0).comparer()
					.column_full_name().column_name().getText();
			right_attribute = ctx.table_query(0).multiple_condition().condition().expression(1).comparer()
					.column_full_name().column_name().getText();
			temp = ctx.table_query(0).multiple_condition().condition().expression(0).comparer().column_full_name()
					.table_name().getText();
			//如果顺序是反的，反过来，保证left对应left，right对应right
			if(!left_table.equals(temp))
			{
				temp = left_table;
				left_table = right_table;
				right_table = temp;
			}
		}

		//只做只有一个where条件的情况，where_attribute comparator where_value
		//where attrName = attrValue
        /*
        multiple_condition :
        condition
        | multiple_condition AND multiple_condition
        | multiple_condition OR multiple_condition ;
        */
		String where_attribute = null;
		String comparator = null;
		String where_value = null;
		boolean hasWhere = true;
		if(ctx.multiple_condition()!=null)
		{
			where_attribute = ctx.multiple_condition().condition().expression(0).comparer().column_full_name()
					.column_name().getText();
			comparator = ctx.multiple_condition().condition().comparator().getText();
			where_value = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();
		}
		else
			hasWhere = false;

		if(isSingleTable)
		{	try{
				Table table = database.getTable(single_table);
				QueryTable queryTable = new QueryTable(table);
				//单表查询

				if (selectAll)//选择全部
				{
					for (int i = 0; i < queryTable.columns.size(); i++)
						resp.columnsList.add(queryTable.columns.get(i).getName());
					if (!hasWhere) {
						resultRows = queryTable.result();
					} else {
						queryTable.query(comparator, where_attribute, where_value);
						resultRows = queryTable.result();
						//resultRows = table.select(comparator, where_attribute, where_valve);
					}
				} else//select 有东西
				{
					//for (String resultColumn : resultColumns)
					//	resultIndex.add(table.getIndex(resultColumn));
					resp.columnsList.addAll(resultColumns);
					if (!hasWhere) {
						QueryResult queryResult = new QueryResult(queryTable, resultColumns);
						resultRows = queryResult.result();
					} else {
						queryTable.query(comparator, where_attribute, where_value);
						QueryResult queryResult = new QueryResult(queryTable, resultColumns);
						resultRows = queryResult.result();
						//resultRows = table.select(comparator, where_attribute, where_valve);
					}
				}
			}
			catch (TableNotExistException e)
			{
				this.success = false;
				status.setCode(Global.FAILURE_CODE);
				String msg = "table does not exist";
				status.setMsg(msg);
				return;
			}
		}
		else
		{
			try {
				Table leftTable = database.getTable(left_table);
				Table rightTable = database.getTable(right_table);
				//QueryResult queryResult = new QueryResult(leftTable, rightTable, left_attribute, right_attribute);
				//多表查询
				QueryTable queryTable = new QueryTable(leftTable, rightTable, left_attribute, right_attribute);
				//System.out.println(queryTable.columns.toString());
				//System.out.println(queryTable.rows.toString());
				if (selectAll) {
					for(int i = 0; i < queryTable.columns.size(); i++)
						resp.columnsList.add(queryTable.columns.get(i).getName());
					if(!hasWhere) {
						//resultRows = queryResult.newTable.select();
						resultRows = queryTable.result();
					}
					else{
						//resultRows = queryResult.newTable.select(comparator, where_attribute, where_valve);
						queryTable.query(comparator, where_attribute, where_value);
						resultRows = queryTable.result();
					}
				}
				else {
					resp.columnsList.addAll(resultColumns);
					if (!hasWhere) {
						QueryResult queryResult = new QueryResult(queryTable, resultColumns);
						resultRows = queryResult.result();
					}
					else {
						queryTable.query(comparator, where_attribute, where_value);
						QueryResult queryResult = new QueryResult(queryTable, resultColumns);
						resultRows = queryResult.result();
						//resultRows = queryResult.newTable.select(comparator, where_attribute, where_valve);
					}
				}
			}
			catch (TableNotExistException e)
			{
				this.success = false;
				status.setCode(Global.FAILURE_CODE);
				String msg = "table does not exist";
				status.setMsg(msg);
				return;
			}
			catch (Exception e){
				//
			}
		}
		for (String resultRow : resultRows)
			resp.rowList.add(Arrays.asList(resultRow.split(",")));

	}

	public ExecuteStatementResp getResult(){
		if(success){
			status.setCode(Global.SUCCESS_CODE);
			resp.hasResult = true;
		}
		else {
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
