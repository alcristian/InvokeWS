import java.sql.SQLException
from ConnectionDB import ConnectionDB
from InvokeWS import InvokeWS
from telnetlib import theNULL

class SelectMsgSoap:
    
    def __init__(self):
        None
        
    def execute(self, wsdl, soapAction, nrSession,nrMod,nrThread,nmTable):
        snpRef = ConnectionDB()
        conn = snpRef.getJDBCConnection("SRC")
        
        try:
            stmt = conn.createStatement()
           
            sql = """
            SELECT A.CD_ESTAB,
                   A.DOCUMENTO,
                   A.TXT_MENSAGEM
              FROM (
                 SELECT SUBSTR(CHAVE_NEGOCIO,0,INSTR(CHAVE_NEGOCIO,'|')-1)CD_ESTAB,
                        DOCUMENTO,
                        TXT_MENSAGEM, 
                        MOD(MOD_ID_MONITOR,'%s')MOD_ID_MONITOR,
                        RANK() OVER (PARTITION BY DOCUMENTO, SUBSTR(CHAVE_NEGOCIO,0,INSTR(CHAVE_NEGOCIO,'|')-1) ORDER BY MOD_ID_MONITOR)RANK
                 FROM INTEG_ODI_TMP.%s
                 ) A
                WHERE A.RANK = 1 AND A.MOD_ID_MONITOR ='%s'""" % (nrThread,nmTable,nrMod)


            rs = stmt.executeQuery(sql)
            
            while rs.next():
                xmlRequest = rs.getString("TXT_MENSAGEM")

                print wsdl
                ws = InvokeWS(wsdl)
                (replayStatus, replayReason, replayMessage, replayHeaders,data) = ws.execute(xmlRequest, soapAction)
               
                xmlCodeFUpdate = rs.getString("CD_ESTAB")
                xmlCodeFUpdate1 = rs.getString("DOCUMENTO")
                
                xmlCodeErro = "S"
                 
                if data.find("<codigodRetorno>0<") in [-1]:
                    xmlCodeErro = "E"
                 
                sql2 = """
                UPDATE INTEG_ODI_TMP.%s SET TXT_RETORNO =SUBSTR('%s',instr('%s','<env:Body>'),400),FL_EVENTO_RETORNO = '%s' WHERE SUBSTR(CHAVE_NEGOCIO,0,INSTR(CHAVE_NEGOCIO,'|')-1) ='%s' AND DOCUMENTO ='%s'""" % (nmTable,data,data,xmlCodeErro, xmlCodeFUpdate,xmlCodeFUpdate1)

                stmtUp = conn.createStatement()
                stmtUp.execute(sql2)
                stmtUp.close()
                    
            rs.close()
            stmt.close()
        
        except java.sql.SQLException, e:
            print e
    
if __name__ == '__main__':
    select = SelectMsgSoap()
    select.execute("""http://apc-des-sp.transacional.getnet.local:7777/integracao/proxy/framework/V1/Integ_FWK_Receber_HabilitacaoProdutoServico?wsdl""", """eventoReceberHabilitacaoProdutoServico""", """#pg_NUM_SESSAO""","""1""","""3""","""FWK_MONITOR_22097001""")
