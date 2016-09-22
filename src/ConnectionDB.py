from java.sql import DriverManager

class ConnectionDB():
    
    def __init__(self):
        pass
    
    def getJDBCConnection(self, str):
        if str == "SRC":
            str = "INTEG_ODI_TMP"
        
        conns = { 
                 "INTEG_FWK_HG":{"jdbc" : "xxx",
                                 "usr"  : "xxx",
                                 "pwd"  : "xxx"},
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
