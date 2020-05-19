package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.schema.Table;
import javafx.scene.control.Cell;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  public Table newTable;

  public QueryResult(QueryTable[] queryTables) throws IOException, ClassNotFoundException {
    // TODO
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.metaInfoInfos = new ArrayList<MetaInfo>();
    ArrayList<Column> newcolumns = new ArrayList<Column>();
    for(int i = 0; i<queryTables.length;i++)
    {
      MetaInfo new_info = new MetaInfo(queryTables[i].tableName,queryTables[i].columns);
      metaInfoInfos.add(new_info);
      newcolumns.addAll(queryTables[i].columns);
    }
    Column[] mColumns = newcolumns.toArray(new Column[newcolumns.size()]);
    newTable = new Table(queryTables[0].databaseName,"table",mColumns);

    if(queryTables.length == 1)
    {
      while(queryTables[0].hasNext())
      {
        newTable.insert(queryTables[0].next());
      }
    }

    if(queryTables.length == 2)
    {
      while(queryTables[0].hasNext())
      {
        Row row1 = queryTables[0].next();
        while (queryTables[1].hasNext())
        {
          LinkedList<Row> mRows = new LinkedList<Row>();
          Row row2 = queryTables[1].next();
          mRows.add(row1);
          mRows.add(row2);
          newTable.insert(combineRow(mRows));
        }
        queryTables[1].reset();
      }
    }
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    Row newRow = new Row();
    for(int i = 0;i<rows.size();i++)
    {
      newRow.appendEntries(rows.get(i).getEntries());
    }
    return newRow;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}