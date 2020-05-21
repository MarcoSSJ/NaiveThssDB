package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {

  int cursor;       // index of next element to return
  int lastRet = -1; // index of last element returned; -1 if no such
  int number;
  public String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  protected ArrayList<Row> rows;

  QueryTable(Table table, Row[] m_rows, String m_databaseName, String m_tableName, ArrayList<Column> m_columns) {
    // TODO
    rows = new ArrayList<Row>(Arrays.asList(m_rows));
    number = rows.size();
    cursor = 0;
    lastRet = -1;
    columns = new ArrayList<Column>(m_columns);
    tableName = m_tableName;
    databaseName = m_databaseName;
  }

  @Override
  public boolean hasNext() {
    // TODO
    if(cursor < number)
      return true;
    else
      return false;
  }

  @Override
  public Row next() {
    // TODO
    if(hasNext()){
      Row result = rows.get(cursor);
      cursor += 1;
      lastRet += 1;
      return result;
    }
    return null;
  }

  public void reset(){
    cursor = 0;
    lastRet = -1;
  }

}