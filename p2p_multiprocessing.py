import socket
import logging
from multiprocess import Manager, Process, Value

# Define global variables
manager = Manager()
lines = manager.dict()
lim = 950
reply = Value('i', 0)

def handle_client(client_socket, client_address):
    global lines
    global reply
    global lim
    print("Connected by", client_address)
    prev = -1
    while reply.value != 1:  # Use reply.value to access the value of the multiprocessing.Value
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                continue
            try:
                line_no, line, *_ = map(str, data.split('\n'))
                line_no = int(line_no)
                prev = line_no
                if line_no == -1:
                    continue
                if line_no not in lines.keys():
                    lines[line_no] = line
                l = len(lines)
                logging.warning(l)
                if l == lim:
                    reply.value = 1  # Use reply.value to update the value
            except:
                lines[prev] = lines[prev] + data
            client_socket.sendall(str(reply.value).encode())
        except Exception as e:
            print("error: ", e)
            print(data)
    print("Thread Closed")
    client_socket.close()

def my_client():
    svayu = socket.socket()
    svayu.connect(("vayu.iitd.ac.in", 9801))
    while True:
        try:
            svayu.sendall(b"SENDLINE\n")
            response = svayu.recv(4096).decode('utf-8')
            if response == "-1\n-1\n":
                continue
            reply_val = sendline(response)
            if reply_val == 1:
                break
        except Exception as e:
            print("error: ", e)
            svayu.close()
            break
    print("Thread Closed")
    svayu.close()

def sendline(data):
    global lines
    global reply
    global lim
    if not data:
        return 0
    try:
        line_no, line, *_ = map(str, data.split('\n'))
        line_no = int(line_no)
        prev = line_no
        if line_no == -1:
            return 0
        if line_no not in lines.keys():
            lines[line_no] = line
        l = len(lines)
        logging.warning(l)
        if l == lim:
            reply.value = 1  # Use reply.value to update the value
    except:
        lines[prev] = lines[prev] + data
    return reply.value  # Use reply.value to return the value

def main():
    server_ip = '10.194.28.9'  # Listen on all available interfaces
    base_port = 12345
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_ip, base_port))
    server_socket.listen(5)
    print("Server is listening for incoming connections...")

    my_thread = Process(target=my_client)
    my_thread.start()

    while reply.value != 1:
        client_socket, client_address = server_socket.accept()
        client_thread = Process(target=handle_client, args=(client_socket, client_address))
        client_thread.start()
        client_thread.join()

    my_thread.join()
    print("Done receiving")
    server_socket.close()

if __name__ == "__main__":
    main()