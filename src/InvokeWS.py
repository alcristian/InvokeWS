#!/bin/bash
from httplib import HTTPConnection
from urlparse import urlparse

                
class InvokeWS:
    
    """Classe a ser utilizada para realizar request em WS"""
    
    def __init__(self, url):
        """params:
                - url: endereco da wsdl/endpoint do servico que deve ser requisitado"""
        parsed = urlparse(url)
        self.protocol = parsed.scheme
        self.host     = parsed.hostname
        self.port     = parsed.port
        self.path     = parsed.path
        
    def execute(self, message, soapAction=None):
        """ Metodo para executar o WS
            params
                - message: Mensagem a ser enviado para o WS
                - soapAction: Nao e obrigatorio informar, mas se existir mais de um servico com o
                              mesmo namespace, deve utilizar para selecionar a operacao desejada
        """
        try:
            
            if(message.find("&")):
                message = message.replace("&", "&amp;")
                    
            message = message.encode('ascii', 'xmlcharrefreplace')
            
            conn = HTTPConnection(self.host, self.port)
            
            conn.putrequest("POST", self.path)
            conn.putheader("Content-Type", "text/xml; charset=iso-8859-1")
            conn.putheader("Content-Length", str(len(message)))
            
            if(soapAction):
                conn.putheader("SOAPAction", soapAction)
            
            conn.endheaders()
            conn.send(message)
            response = conn.getresponse()
            data = response.read()
            replayStatus = response.status
            replayReason = response.reason
            replayMessage = response.msg
            replayHeaders = response.getheaders()
            
            print replayStatus
            #200 - OK (Synchronous)
            #201 - CREATED
            #202 - ACCEPTED(Asynchronous)
           # if replayStatus not in [200,201, 202] :
           #     raise Exception("%s - %s " % (replayStatus, replayMessage))
            
            return (replayStatus, replayReason, replayMessage, replayHeaders,data)
            
        except Exception:
            raise;
        finally:
            if (conn != None):
                conn.close()