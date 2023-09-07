master_ip='10.194.1.207'
master_port=8000
vayu_ip='10.17.6.5'
vayu_port=9801
my_id=1

import socket
import threading
import logging
import time

global send_socket
global recv_socket
global vayu_socket

vayu_=(vayu_ip,vayu_port)

send_socket=None
recv_socket=None
vayu_socket=None

global lines
global recv_status
recv_status=False
lines={}
lim=1000

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

def client_connect(id,s):
    id_=bytes(str(id),'utf-8')+s
    port=master_port+id+ 1000*(s==b'#2')
    _socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.connect((master_ip,port))
    _socket.sendall(id_)
    reply=_socket.recv(4096)
    while(reply!=b"OK"):
        try:
            _socket.close()
        except:
            pass
        try:
            _socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _socket.connect((master_ip,port))
            _socket.sendall(id_)
            reply=_socket.recv(4096)
        except:
            time.sleep(0.01)
            continue
    return _socket

def main():
    global send_socket
    global recv_socket
    global vayu_socket
    global id
    
    send_socket=client_connect(id,b'#1')
    recv_socket=client_connect(id,b'#2')

    vayu_connect()
    logging.warning("Connected to all sockets")
    
    # start threads
    get_thread=threading.Thread(target=get)
    get_thread.start()
    logging.warning("get thread started")
    recv_thread=threading.Thread(target=recv)
    recv_thread.start()
    logging.warning("recv thread started")

    # wait for threads to finish
    while(len(lines)<lim):
        logging.warning(len(lines))
        continue
    
    submit_thread=threading.Thread(target=submit)
    submit_thread.start()
    logging.warning("submit thread started")
    submit_thread.join()

    # close sockets
    vayu_socket.close()
    send_socket.close()
    recv_socket.close()

def get():
    # get lines from vayu 
    start = time.time()
    while (len(lines) != lim):
        curr = time.time()
        #function which caters to the rate limit on vayu
        if (curr - start >= 0.01):
            count = 10
            #error handling for vayu socket connection
            old = start
            start = time.time()
            while(count >= 1):
                try:
                    vayu_socket.sendall(b"SENDLINE\n") 
                    break
                except:
                    count -= 1
                    continue
            #if error then create re-establish a new connection to vayu
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
    try:
        line_no=b""
        index=0
        while (data_string[index]!=10):
            index+=1
        line_no=data_string[:index]
        if line_no not in lines.keys():
            lines[line_no] = data_string
            send(data_string)
            logging.warning(len(lines))
    except Exception as e:
        print("error: ", e)

def send(response):
    # this function is called in the parse function where you send a line you received from vayu to the master
    global send_socket
    global id
    for _ in range(10):
        try:
            send_socket.sendall(response)
            reply = send_socket.recv(1024)
            if reply == b'OK':
                return
        except:
            continue
    client_connect(id+b'#1')
    send(response)

def recv():
    # client stores the line in the local dictionary
    global lines
    global recv_status
    global recv_socket
    recv_socket.settimeout(2)
    while len(lines) != lim:
        try:
            response = recv_socket.recv(1024)
            try:
                if (response == b"DONE"):
                    if (len(lines) == lim):
                        recv_socket.sendall(b"DONE")
                    else:
                        send_message = b"NO\n"
                        for i in range(1,lim):
                            encoded_key = str(i).encode()
                            if encoded_key not in lines.keys():
                                send_message += encoded_key + b"\n"
                        recv_socket.sendall(send_message)
                else:
                    line_no=b""
                    index=0
                    while (response[index]!=10):
                        index+=1
                    line_no=response[:index]
                    if line_no not in lines.keys():
                        lines[line_no] = response
                        logging.warning(len(lines))
                    recv_socket.sendall(b"OK")
            except Exception as e:
                print("error: ", e)
        except:
            client_connect(id+b'#2')
        
def submit():
    global lines
    global vayu_socket
    status=b'FAILED'
    for _ in range(10):
        if status == b"SUCCESS":
            break
        vayu_socket.sendall(b"SUBMIT\nKASHISH@COL334-672\n"+str(lim)+"\n")
        for i in lines.values():
            vayu_socket.sendall(i)
        status = vayu_socket.recv(4096).split(b'\n')[1]
    if(status==b"SUCCESS"):
        print("SUCCESS")
    else:
        print("FAILED")
    vayu_socket.close()
