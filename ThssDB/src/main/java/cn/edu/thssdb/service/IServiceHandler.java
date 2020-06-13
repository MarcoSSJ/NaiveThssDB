package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.myListener;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

	@Override
	public GetTimeResp getTime(GetTimeReq req) throws TException {
		GetTimeResp resp = new GetTimeResp();
		resp.setTime(new Date().toString());
		resp.setStatus(new Status(Global.SUCCESS_CODE));
		return resp;
	}

	@Override
	public ConnectResp connect(ConnectReq req) throws TException {
		ConnectResp resp = new ConnectResp();
		String usr = req.username;
		String pwd = req.password;
		if (usr.equals("username") && pwd.equals("password")) {//先写死，都是管理员与空密码
			ThssDB server = ThssDB.getInstance();
			resp.setSessionId(server.addSession());
			resp.setStatus(new Status(Global.SUCCESS_CODE));
		}
		else {
			resp.setStatus(new Status(Global.FAILURE_CODE));
		}
		return resp;
	}

	@Override
	public DisconnectResp disconnect(DisconnectReq req) throws TException {
		DisconnectResp resp = new DisconnectResp();

		ThssDB server = ThssDB.getInstance();
		boolean res = server.deleteSession(req.getSessionId());
		if(res)
			resp.setStatus(new Status(Global.SUCCESS_CODE));
		else
			resp.setStatus(new Status(Global.FAILURE_CODE));
		return resp;
	}

	@Override
	public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {

		ThssDB thssDB = ThssDB.getInstance();
		ExecuteStatementResp resp = new ExecuteStatementResp();
		if (thssDB.checkSession(req.getSessionId())) {
			String statement = req.statement;
			long sessionId = req.getSessionId();
			CodePointCharStream charStream = CharStreams.fromString(statement);
			SQLLexer lexer = new SQLLexer(charStream);
			CommonTokenStream tokens = new CommonTokenStream(lexer);
			SQLParser parser = new SQLParser(tokens);
			ParseTree tree = parser.parse();
			ParseTreeWalker walker = new ParseTreeWalker();
			myListener listener = new myListener();
			listener.setSessionId(sessionId);
			walker.walk(listener, tree);
			resp = listener.getResult();
		}
		else {
			// TODO
		}
		resp.setStatus(new Status(Global.SUCCESS_CODE));
		return resp;
	}
}
