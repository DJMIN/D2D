import xmlrpc.client
server = xmlrpc.client.Server('http://localhost:9001/RPC2')
print(server.system.methodHelp('supervisor.readProcessStdoutLog'))
print(server.supervisor.readProcessStdoutLog("lazyone", 100, 100))
print(server.supervisor.getState())
for p in server.supervisor.getAllProcessInfo():
    print(list(p.keys()))
    print(list(p.values()))
print(server.system.listMethods())
# server.system.methodHelp('supervisor.shutdown')