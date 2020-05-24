package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class myListener extends SQLBaseListener{
	private Manager manager;

	@Override
	public void enterParse(SQLParser.ParseContext ctx){
		//进入数据库操作，对应g4文件3-4行parse
		manager = new Manager();
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
		//改变数据库
		manager.use(dbName);
		//switchDatabase里面是空的……
	}

	@Override
	public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
		String tableName = ctx.table_name().getText();
		//删除整个表
		manager.database.drop(tableName);
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
				//            List<SQLParser.Column_nameContext> column_nameContexts = ctx.table_constraint().column_name();
				//            ArrayList<String> primaryNames = new ArrayList<>();
				//            //int numOfPrimary = column_nameContexts.size();
				//            for (SQLParser.Column_nameContext column_nameContext : column_nameContexts)
				//            {
				//                primaryNames.add(column_nameContext.getText());
				//            }
				//            System.out.println(Arrays.toString(primaryNames.toArray()));
				//            for (int i = 0; i < numOfColumns; i++)
				//            {
				//                //System.out.println(columns[i].name());
				//                if (primaryNames.contains(columns[i].name()))
				//                {
				//                    columns[i].setPrimary(1);
				//                    break;
				//                }
				//            }
				//主键只有一列
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
					//not null only
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

			//        try {
			//            Database db = manager.getCurrentDB();
			//            if(db.containsTable(tableName)){
			//                success = false;
			//                status.msg += "Duplicated tableName.\n";
			//            }
			//            else {
			//                db.create(tableName, columns);
			//                status.msg += "Create table successfully.\n";
			//            }
			//        }catch (Exception e){
			//            success = false;
			//            status.msg+="Failed to create table.";
			//        }
			//TODO:
			//建立表的接口

			manager.database.create(tableName, columns);
		}
		catch (Exception e)
		{}
	}

	@Override
	public void exitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
		//TODO:检测主键为空和重复主键的问题

		String tableName = ctx.table_name().getText();
		List<SQLParser.Column_nameContext> column_nameContexts = ctx.column_name();
		//看是否是默认插入，INSERT INTO person VALUES (‘Bob’, 15)或INSERT INTO person(name) VALUES (‘Bob’)
		int numOfColumn = column_nameContexts.size();
		String[] columnNames = new String[numOfColumn];
		for(int i=0;i<numOfColumn;i++)
		{
			columnNames[i] = column_nameContexts.get(i).getText();
		}
		//System.out.println("columnNum: "+numOfColumn);
		//List<SQLParser.Value_entryContext> value_entryContexts = ctx.value_entry();

		//TODO：这里处理value为什么要这么处理我也不知道，到时候测试时试验一下
		String rawEntryValue = ctx.value_entry(0).getText();
		// 去空格
		rawEntryValue = rawEntryValue.trim();
		// 去括号
		StringBuilder rawWithoutBrace = new StringBuilder();
		for(int i=1;i<rawEntryValue.length()-1;i++)
		{
			rawWithoutBrace.append(rawEntryValue.charAt(i));
		}
		//System.out.println(rawWithoutBrace);
		String[] entryValues = rawWithoutBrace.toString().split(",");
		int numOfEntries = entryValues.length;
		//System.out.println("entryNum: "+numOfEntries);

		//TODO：这里需要找到表，这里有问题
		Table currentTable = manager.database.getTable(tableName);

		Entry[] entries = new Entry[numOfEntries];
		//System.out.println(Arrays.toString(entries));
		for(int i=0;i<numOfEntries;i++)
		{
			entries[i] = new Entry(entryValues[i]);
		}
		//System.out.println(Arrays.toString(entries));
		Row insertRow;

		if(numOfColumn == 0)
		{
			// 默认输入，类似INSERT INTO person VALUES (‘Bob’, 15)，entries不调整
			insertRow = new Row(entries);
		}
		else
		{
			//类似INSERT INTO person(name) VALUES (‘Bob’)
			int numOfRealColumns = currentTable.columns.size();
			Entry[] realEntries = new Entry[numOfRealColumns];
			for(int i=0;i<numOfRealColumns;i++)
			{
				realEntries[i] = new Entry(null);
			}
			for(int i=0;i<numOfColumn;i++)
			{
				//check every column
				int index;
				//这里在找插入的是哪一个属性
				for(index = 0; index < numOfRealColumns; index++)
				{
					if(currentTable.columns.get(index).name.equals(columnNames[i]))
					{
						break;
					}
				}
				realEntries[index] = new Entry(entries[i]);
			}
			insertRow = new Row(realEntries);
		}
		currentTable.insert(insertRow);
	}

	@Override
	public void exitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
		String tableName = ctx.table_name().getText();
		Table currentTable = manager.database.getTable(tableName);
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		System.out.println(comparator);
		String attrName = ctx.multiple_condition().condition().expression(0).getText();
		String attrValue = ctx.multiple_condition().condition().expression(1).getText();
		System.out.println(attrName);
		System.out.println(attrValue);
		currentTable.delete(comparator, attrName, attrValue);
	}

	@Override
	public void exitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
		// 更新哪个表
		String tableName = ctx.table_name().getText();
		System.out.println(tableName);
		// 更新哪一列
		String attrToBeUpdated = ctx.column_name().getText();
		System.out.println(attrToBeUpdated);
		// 更新为何值
		String valueTobeUpdated = ctx.expression().getText();
		System.out.println(valueTobeUpdated);
		Table currentTable = manager.database.getTable(tableName);
		String comparator = ctx.multiple_condition().condition().comparator().getText();
		String attrName = ctx.multiple_condition().condition().expression(0).getText();
		String attrValue = ctx.multiple_condition().condition().expression(1).getText();
		currentTable.update(attrToBeUpdated, valueTobeUpdated, comparator, attrName, attrValue);
	}

	@Override
	public void exitSelect_stmt(SQLParser.Select_stmtContext ctx){
        /*
        select_stmt :
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
        K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
        */
		ArrayList<String> resultTables = new ArrayList<>();//列来自什么表 tablename
		ArrayList<String> resultColumns = new ArrayList<>();//查询哪些列 columnname
		List<SQLParser.Result_columnContext> result_columnContexts = ctx.result_column();
		ArrayList<Row> resultRows = new ArrayList<>(); //查询结果

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
			Table table = manager.database.getTable(sigleTableName);
			//TODO:单表查询
			if(selectAll)
			{
				resultRows = table.select();
			}
			else
			{
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
				Table leftTable = manager.database.getTable(leftTableName);
				Table rightTable = manager.database.getTable(rightTableName);
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
	}

	@Override
	public void exitParse(SQLParser.ParseContext ctx){
		//退出数据库操作，进行持久化等操作，需要接口
		try {
			manager.database.quit();
		}
		catch (Exception e)
		{}
	}
}
