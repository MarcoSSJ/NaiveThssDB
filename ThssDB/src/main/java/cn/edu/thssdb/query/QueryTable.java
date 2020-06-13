package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import com.sun.org.apache.xpath.internal.objects.XNull;
import javafx.scene.control.Cell;

import java.io.IOException;
import java.util.*;

public class QueryTable implements Iterator<Row> {

	int cursor;       // index of next element to return
	int lastRet = -1; // index of last element returned; -1 if no such
	int number;
	public String databaseName;
	public String tableName;
	public ArrayList<Column> columns;
	public ArrayList<Row> rows;
	private List<MetaInfo> metaInfoInfos;
	private List<Integer> index;
	private List<Cell> attrs;

	public QueryTable(Table leftQueryTable, Table rightQueryTable, String leftAttrName, String rightAttrName) throws IOException, ClassNotFoundException {
		// TODO
		this.index = new ArrayList<>();
		this.attrs = new ArrayList<>();
		this.metaInfoInfos = new ArrayList<>();
		this.columns = new ArrayList<>();
		this.rows = new ArrayList<>();

		MetaInfo leftInfo = new MetaInfo(leftQueryTable.tableName,leftQueryTable.columns);
		metaInfoInfos.add(leftInfo);
		columns.addAll(leftQueryTable.columns);

		MetaInfo rightInfo = new MetaInfo(rightQueryTable.tableName,rightQueryTable.columns);
		metaInfoInfos.add(rightInfo);
		columns.addAll(rightQueryTable.columns);

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
				Row row2 = rightIterator.next();
				if(row1.getEntries().get(leftIndex).compareTo(row2.getEntries().get(rightIndex))==0) {
					Row newRow = new Row();
					newRow.appendEntries(row1.getEntries());
					newRow.appendEntries(row2.getEntries());
					this.rows.add(newRow);
				}
			}
		}
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