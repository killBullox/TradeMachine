import socket
s = socket.socket()
r = s.connect_ex(('127.0.0.1', 8000))
s.close()
print('PORT_FREE' if r != 0 else 'PORT_BUSY')
