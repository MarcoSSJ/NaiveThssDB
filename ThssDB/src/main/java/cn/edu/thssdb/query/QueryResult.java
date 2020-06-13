package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.schema.Table;
import javafx.scene.control.Cell;

public class QueryResult {

	private List<MetaInfo> metaInfoInfos;
	private List<Integer> index;
	private List<Cell> attrs;
	public Table newTable;
	private QueryTable queryTable;
	private ArrayList<String> result;

	public QueryResult(QueryTable queryTable, ArrayList<String> columns)
	{
		this.queryTable = queryTable;
		this.result = new ArrayList<>();
		index = new ArrayList<>();
		for(int i = 0; i < columns.size(); i++){
			for(int j = 0; j < this.queryTable.columns.size(); j++){
				if(this.queryTable.columns.get(j).compareTo(columns.get(i))==0){
					index.add(j);
					break;
				}
			}
		}
		Iterator iterator = queryTable.rows.iterator();
		while (iterator.hasNext()){
			Row row = (Row)iterator.next();
			ArrayList<String> resRow = new ArrayList<>();
			for(int i = 0; i < index.size(); i++)
			{
				resRow.add(row.getEntries().get(index.get(i)).toString());
			}
			result.add(resRow.toString());
		}
	}

	public QueryResult(Table leftQueryTable, Table rightQueryTable, String leftAttrName, String rightAttrName) throws IOException, ClassNotFoundException {
		// TODO
		this.index = new ArrayList<>();
		this.attrs = new ArrayList<>();
		this.metaInfoInfos = new ArrayList<MetaInfo>();
		ArrayList<Column> newcolumns = new ArrayList<Column>();

		MetaInfo leftInfo = new MetaInfo(leftQueryTable.tableName,leftQueryTable.columns);
		metaInfoInfos.add(leftInfo);
		newcolumns.addAll(leftQueryTable.columns);

		MetaInfo rightInfo = new MetaInfo(rightQueryTable.tableName,rightQueryTable.columns);
		metaInfoInfos.add(rightInfo);
		newcolumns.addAll(rightQueryTable.columns);
		Column[] mColumns = newcolumns.toArray(new Column[newcolumns.size()]);

		newTable = new Table(leftQueryTable.getDatabaseName(),"queryTable", mColumns);

		Iterator<Row> leftIterator = leftQueryTable.iterator();
		Iterator<Row> rightIterator;

		int leftIndex = leftQueryTable.getIndex(leftAttrName);
		int rightIndex = rightQueryTable.getIndex(rightAttrName);

		while(leftIterator.hasNext())
		{
			Row row1 = leftIterator.next();
			rightIterator = rightQueryTable.iterator();
			while (rightIterator.hasNext())
			{
				LinkedList<Row> mRows = new LinkedList<>();
				Row row2 = rightIterator.next();
				if(row1.getEntries().get(leftIndex).compareTo(row2.getEntries().get(rightIndex))==0) {
					mRows.add(row1);
					mRows.add(row2);
					newTable.insert(combineRow(mRows));
				}
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

	public ArrayList<String> result(){
		ArrayList<String> res = new ArrayList<>();
		Iterator iterator = this.result.iterator();
		while (iterator.hasNext()){
			String row = iterator.next().toString();
			row = row.substring(1, row.length()-1);
			res.add(row);
		}
		return res;
	}
}