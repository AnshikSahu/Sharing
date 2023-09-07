import socket
import threading
import logging
import time
from queue import Queue

clients_send={}
clients_recv={}
done={}
queues={} # queues[id] is the queue for client id
lines={} 
lim=1000
num_clients=1
active_clients_send=0
active_clients_recv=0

my_ip='10.194.1.207'
my_port_begin=8000
vayu_ip='10.17.7.218'
vayu_port=9801
vayu_=(vayu_ip,vayu_port)

def vayu_connect():
    global vayu_socket
    global vayu_
    vayu_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    vayu_socket.connect(vayu_)
    vayu_socket.sendall(b"SESSION RESET\n")
    reply=vayu_socket.recv(4096)
    while(not reply):
        try:
            vayu_socket.close()
        except:
            pass
        try:
            vayu_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            vayu_socket.connect(vayu_)
            vayu_socket.sendall(b"SESSION RESET\n")
            reply=vayu_socket.recv(4096)
        except:
            time.sleep(0.01)
            continue

def client_connect_send(id):
    global clients_send
    global queues
    global done
    global active_clients_send
    # opens a connection with the id_ client for sending
    _socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.bind((my_ip,my_port_begin+id))
    _socket.listen(5)
    done[id]=False
    queues[id]=Queue()
    id_=bytes(str(id),'utf-8')+b'#1'
    socket.setdefaulttimeout(5.0)
    while(not done[id]):
        try:
            conn, addr=_socket.accept()
            reply=conn.recv(4096)
            if(reply!=id_):
                conn.close()
                continue
            for _ in range(10):
                try:
                    conn.sendall(b"OK")
                    if(id in clients_send):
                        try:
                            clients_send[id].close()
                            active_clients_send-=1
                        except:
                            pass
                    clients_send[id]=conn
                    active_clients_send+=1
                    logging.warning("connected to client for send "+str(id))
                    break
                except:
                    continue 
        except:
            try:
                conn.close()
            except:
                pass
            continue
    clients_send[id].close()
    _socket.close()

def client_connect_recv(id):
    global clients_recv
    global queues
    global done
    global lines
    global lim
    global active_clients_recv
    # opens a connection with the id_ client for sending
    _socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.bind((my_ip,my_port_begin+id+1000))
    _socket.listen(5)
    id_=bytes(str(id),'utf-8')+b'#2'
    while(len(lines)<lim):
        try:
            conn, addr=_socket.accept()
            reply=conn.recv(4096)
            if(reply!=id_):
                conn.close()
                continue
            for _ in range(10):
                try:
                    conn.sendall(b"OK")
                    if(id in clients_recv):
                        try:
                            clients_recv[id].close()
                            active_clients_recv-=1
                        except:
                            pass
                    clients_recv[id]=conn
                    active_clients_recv+=1
                    logging.warning("connected to client for recv "+str(id))
                    break
                except:
                    continue 
        except:
            try:
                conn.close()
            except:
                pass
            continue
    while not done[id]:
        time.sleep(0.001)
    clients_recv[id].close()
    _socket.close()             

def send_to_client(id):
    #sends the messages in the queue of client id to the client
    global queues
    global clients_recv
    global done
    global lines
    global lim
    global active_clients

    curr_queue = queues[id]
    curr_socket = clients_send[id]
    curr_socket.settimeout(0.5)
    while not done[id]:
        while curr_queue.qsize() > 0:
            msg = curr_queue.get()
            try:
                curr_socket.sendall(msg)
                reply = curr_socket.recv(4096)
                if (msg == b"DONE"):
                    if (reply == b"DONE"):
                        done[id] = True
                        active_clients -= 1
                        return
                    else:
                        if len(reply>=3):
                            l=(reply[3:]).split(b'\n')[:-1]
                            for i in l:
                                curr_queue.put(lines[i])
                        curr_queue.put(b"DONE")
                elif (reply != b"OK"):
                    curr_queue.put(msg)
            except:
                curr_queue.put(msg)
        time.sleep(0.001)

def recv_from_client(id):
    #receives the messages from the client and handles them (puts in queue)
    global queues
    global clients_recv
    global lines
    global lim
    global num_clients

    while len(lines) < lim:
        try:
            socket = clients_recv[id]
            response = socket.recv(4096)
            index=0
            while (response[index]!=10):
                index+=1
            line_no=response[:index]
        except:
            continue
        if line_no not in lines:
            lines[line_no] = response
            logging.warning(len(lines))
            for i in range(1,num_clients+1):
                if i != id:
                    queues[i].put(response)
            if len(lines) >= lim:
                for i in range(1,num_clients+1):
                    queues[i].put(b"DONE")

def get():
    global lines
    global lim
    global vayu_socket
    start = time.time()
    while (len(lines) != lim):
        curr = time.time()
        if (curr - start >= 0.01):
            count = 10
            old = start
            start = time.time()
            while(count >= 1):
                try:
                    vayu_socket.sendall(b"SENDLINE\n") 
                    break
                except:
                    count -= 1
                    continue
            if (count == 0):
                vayu_connect()
            else:
                response=b''
                while True:
                    response_new= vayu_socket.recv(4096)
                    response+=response_new
                    if response == b'-1\n\n':
                        start=old
                        break
                    if(response_new==b'' or response_new[-1]==10):
                        parse_thread=threading.Thread(target=parse,args=(response,))
                        parse_thread.start()
                        break

def parse(data_string):
    # lines: bit_string -> bit_string (line_no -> line)
    global lines
    global num_clients
    global queues

    try:
        line_no=b""
        index=0
        while (data_string[index]!=10):
            index+=1
        line_no=data_string[:index]
        if line_no not in lines.keys():
            lines[line_no] = data_string
            # send_to_client(data_string)
            for i in range(1,num_clients+1):
                queues[i].put(data_string)
            logging.warning(len(lines))
    except Exception as e:
        print("error: ", e)

def submit():
    global lines
    global vayu_socket
    status=b'FAILED'
    for _ in range(10):
        if status == b"SUCCESS":
            break
        vayu_socket.sendall(b"SUBMIT\nKASHISH@COL334-672\n"+bytes(str(lim),'utf-8')+b"\n")
        for i in lines.values():
            vayu_socket.sendall(i)
        status = vayu_socket.recv(4096).split(b'\n')[1]
    if(status==b"SUCCESS"):
        print("SUCCESS")
    else:
        print("FAILED")
    vayu_socket.close()

def main():
    global num_clients
    global lim
    global active_clients_send
    global active_clients_recv
    global lines
    global done
    global queues
    global clients_send
    global clients_recv

    
    logging.warning("starting")
    logging.warning("num_clients: "+str(num_clients))
    recv_connect_threads={}
    send_connect_threads={}
    for i in range(1,num_clients+1):
        send_connect_threads[i]=threading.Thread(target=client_connect_send, args=(i,))
        recv_connect_threads[i]=threading.Thread(target=client_connect_recv, args=(i,))
        send_connect_threads[i].start()
        recv_connect_threads[i].start()
    while(active_clients_send<num_clients or active_clients_recv<num_clients):
        time.sleep(0.001)
    logging.warning("connected to all clients")
    vayu_connect_thread=threading.Thread(target=vayu_connect)
    vayu_connect_thread.start()
    logging.warning("connected to vayu")

    send_threads={}
    recv_threads={}

    get_thread=threading.Thread(target=get)
    get_thread.start()

    for i in range(1,num_clients+1):
        send_threads[i]=threading.Thread(target=send_to_client, args=(i,))
        recv_threads[i]=threading.Thread(target=recv_from_client, args=(i,))
        send_threads[i].start()
        recv_threads[i].start()
        
    while(len(lines)<lim):
        time.sleep(0.001)

    submit()

    for i in range(1,num_clients+1):
        send_threads[i].join()
        recv_threads[i].join()

    logging.warning("done")

if __name__ == "__main__":
    main()
