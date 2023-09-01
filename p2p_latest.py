import socket
import threading
import logging
global lines 
global reply
global lim
lines = {}
lim=50
reply='0'
def handle_client(client_socket, client_address):
    global lines 
    global reply
    global lim
    print("Connected by", client_address)
    prev=-1
    while reply!='1':
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if not data: continue
            try:
                line_no, line, *_ = map(str,data.split('\n')) 
                line_no = int(line_no)
                prev=line_no
                if(line_no==-1): continue
                if line_no not in lines.keys(): lines[line_no] = line
                l=len(lines)
                logging.warning(l)
                if(l==lim): reply='1'
            except: lines[prev]=lines[prev]+data
            client_socket.sendall(reply.encode())
        except Exception as e:
            print("error: ", e)
            print(data)
    print("Thread Closed")
    client_socket.close()

def main():
    global reply
    server_ip = '10.194.28.9'  # Listen on all available interfaces
    base_port = 12345
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_ip, base_port))
    server_socket.listen(5)
    print("Server is listening for incoming connections...")
    while reply!='1':
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()
    print("Done receiving")
    server_socket.close()
if __name__ == "__main__":
    main()
