package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Manager;

import java.io.IOException;

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
    public void exitParse(SQLParser.ParseContext ctx) throws IOException {
        //退出数据库操作，进行持久化等操作，需要接口
        manager.database.quit();
    }
}
