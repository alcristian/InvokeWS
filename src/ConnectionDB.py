from java.sql import DriverManager

class ConnectionDB():
    
    def __init__(self):
        pass
    
    def getJDBCConnection(self, str):
        if str == "SRC":
            str = "INTEG_ODI_TMP"
        
        conns = { 
                 "INTEG_FWK_HG":{"jdbc" : "jdbc:oracle:thin:@db-desa-sp.transacional.getnet.local:1521/integracaosrv",
                                 "usr"  : "INTEG_FRAMEWORK_WR",
                                 "pwd"  : "getnet123wr"},
                 "INTEG_LOGS_HG":{"jdbc" : "jdbc:oracle:thin:@dsv12c-bd-hg-cb.getnet.local:1539/INTCREDHGSRV",
                                 "usr"  : "INTEG_LOGS",
                                 "pwd"  : "rvnmney3jhjh#2094"},
                 "INTEG_ODI_TMP":{"jdbc" : "jdbc:oracle:thin:@db-desa-sp.transacional.getnet.local:1521/integracaosrv",
                                 "usr"  : "INTEG_ODI_TMP",
                                 "pwd"  : "INTEG_ODI_TMP"},
                 }
        
        jdbc = conns.get(str).get("jdbc")
        usr = conns.get(str).get("usr")
        pwd = conns.get(str).get("pwd")
        
        return DriverManager.getConnection(jdbc, usr, pwd)

    def closeCon(self, conn):
        if conn is not None:
            conn.close()
            
def test():
    snpRef = ConnectionDB()
    conn = snpRef.getJDBCConnection("SRC")
    
    stmt = conn.createStatement()
    
    rs = stmt.executeQuery("select sysdate from dual")
    
    while rs.next():
        print rs.getString(1)

    rs.close()
    stmt.close()
    conn.close()
    
if __name__ == "__main__":
    test()