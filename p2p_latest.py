import socket
import threading

global lines 
lines = {}
lim=950
reply='0'
# Function to handle sending messages to clients
def send_messages(client_socket):
    while True:
        message = input("Enter a message to send to clients: ")
        client_socket.sendall(message.encode('utf-8'))

def handle_client(client_socket, client_address):
    print("Connected by", client_address)
    global reply
    global lines
    global lim
    # Start the thread for sending messages
    # send_thread = threading.Thread(target=send_messages, args=(client_socket,))
    # send_thread.start()

    while reply=='0':
        # Receive data from the client
        data = client_socket.recv(4096).decode('utf-8')
        if not data:
            break
        #print("Received from", client_address, ":", data)
        line_no, line, _ = map(str,data.split('\n')) 
        line_no = int(line_no)
        if line_no not in lines.keys():
            lines[line_no] = line
        if(len(lines)==lim):
            reply='1'
        client_socket.sendall(reply.encode())
        print("printinh length of lines ",len(lines))

    client_socket.close()        

    # Clean up the connection
    # client_socket.close()
    # print("Connection with", client_address, "closed")

def main():
    server_ip = '10.194.28.9'  # Listen on all available interfaces
    base_port = 12345

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((server_ip, base_port))
    server_socket.listen(5)
    print("Server is listening for incoming connections...")

    client_id = 1  # For tracking clients and assigning unique ports

    while True:
        client_socket, client_address = server_socket.accept()

        client_port = base_port + client_id
        client_id += 1

        print(f"Connection from {client_address[0]}:{client_address[1]} assigned to port {client_port}")

        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()
        if(reply=='1'):
            break
    print("Done receiving")
    # print("Closing server socket...")
    server_socket.close()

        

if __name__ == "__main__":
    main()
