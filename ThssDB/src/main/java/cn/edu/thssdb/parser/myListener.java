package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.type.ColumnType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    public void exitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) throws IOException, ClassNotFoundException {
        String dbName = ctx.database_name().getText();
        //看一看是否存在该数据库，不存在创建，存在报错，需要接口
        manager.createDatabaseIfNotExists(dbName);
    }

    @Override
    public void exitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) throws IOException {
        String dbName = ctx.database_name().getText();
        //看一看是否存在该数据库，不存在报错，存在删除，需要接口
        manager.deleteDatabase(dbName);
    }

    @Override
    public void exitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        //改变数据库
        manager.switchDatabase(dbName);

    }

    @Override
    public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        //删除表
        manager.database.drop(tableName);
    }

    @Override
    public void exitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
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
    }

    @Override
    public void exitParse(SQLParser.ParseContext ctx) throws IOException {
        //退出数据库操作，进行持久化等操作，需要接口
        manager.database.quit();
    }
}
