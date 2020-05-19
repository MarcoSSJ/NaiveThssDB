package cn.edu.thssdb.parser;

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

//        try
//        {
//            currentTable.insert(insertRow);
//        }catch(NDException e)
//        {
//            success = false;
//            status.msg+="Some of your insert values cannot be null.\n";
//        }
        //TODO：在这里进行插入
    }

    @Override
    public void exitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        //TODO：这里需要找到是哪张表
        Table currentTable = manager.database.getTable(tableName);
        String comparator = ctx.multiple_condition().condition().comparator().getText();
        System.out.println(comparator);
        String attrName = ctx.multiple_condition().condition().expression(0).getText();
        String attrValue = ctx.multiple_condition().condition().expression(1).getText();
        System.out.println(attrName);
        System.out.println(attrValue);
        //由于表的delete方法的参数是主键，所以首先找到所有被删除的行的主键
        ArrayList<Entry> deleteEntries = new ArrayList<>(); //被删除的行的主键
        //找到传入的语句中的attrName是第几列
        int attrNameIndex = 0;
        ArrayList<Column> currentColumns = currentTable.columns;
        for(int i=0;i<currentTable.columns.size();i++)
        {
            System.out.println(currentColumns.get(i).name);
            if(currentColumns.get(i).name.equals(attrName))
            {
                attrNameIndex = i;
                break;
            }
        }
        int primaryIndex = currentTable.primaryIndex;
        Entry attrValueEntry = new Entry(attrValue);
        Iterator<Row> iterator = currentTable.iterator();
        switch (comparator)
        {
            case "=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    //System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)==0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<>":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)!=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            default:
                break;
        }
        for (Entry deleteEntry : deleteEntries) {
            currentTable.delete(deleteEntry);
        }
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
        //TODO:找到哪个表
        Table currentTable = manager.getCurrentDB().getTable(tableName);
        // 更新哪一行，找到主键
        String comparator = ctx.multiple_condition().condition().comparator().getText();
        String attrName = ctx.multiple_condition().condition().expression(0).getText();
        String attrValue = ctx.multiple_condition().condition().expression(1).getText();
        // 条件中的attrName是第几列
        int attrNameIndex = 0;
        ArrayList<Column> currentColumns = currentTable.columns;
        for(int i=0;i<currentTable.columns.size();i++){
            if(currentColumns.get(i).name.equals(attrName)){
                attrNameIndex = i;
                break;
            }
        }
        // 更新中的attrToBeUpdated是第几列
        int attrToBeUpdatedIndex = 0;
        for(int i=0;i<currentTable.columns.size();i++){
            if(currentColumns.get(i).name.equals(attrToBeUpdated)){
                attrToBeUpdatedIndex = i;
                break;
            }
        }
        int primaryIndex = currentTable.primaryIndex;
        Entry attrValueEntry = new Entry(attrValue);
        ArrayList<Entry> updateEntries = new ArrayList<>(); //被更新的行的主键
        Iterator<Row> iterator = currentTable.iterator();
        switch (comparator){
            case "=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)==0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<>":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)!=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            default:
                break;
        }
        for (Entry updateEntry : updateEntries) {
            System.out.println(updateEntry);
            //TODO：从数据库表里面把要改的行拿出来
            Row updateRow = currentTable.getRow(updateEntry);
            System.out.println(updateRow);
            ArrayList<Entry> updateRowEntries = updateRow.getEntries();
            Entry[] newRowEntries = new Entry[updateRowEntries.size()];
            //ArrayList<Entry> newRowEntries = new ArrayList<>();
            for (int j = 0; j < updateRowEntries.size(); j++) {
                if (j == attrToBeUpdatedIndex) {
                    newRowEntries[j] = new Entry(valueTobeUpdated);
                } else {
                    newRowEntries[j] = updateRowEntries.get(j);
                }
            }
            //updateRowEntries.get(attrToBeUpdatedIndex) = new Entry(valueTobeUpdated);
            Row newRow = new Row(newRowEntries);
            //TODO：更新表
//            currentTable.update(updateEntry, newRow);
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
