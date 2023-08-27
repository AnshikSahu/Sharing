

import socket
import threading

def handle_client(client_socket, client_address):
    print("Connected by", client_address)

    while True:
        # Receive data from the client
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            break
        print("Received from", client_address, ":", data)

        # Send a response back to the client
        response = "Server received: " + data
        client_socket.sendall(response.encode('utf-8'))

    # Clean up the connection
    client_socket.close()
    print("Connection with", client_address, "closed")

def main():
    server_ip = '10.184.28.181'  # Listen on all available interfaces
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

if __name__ == "__main__":
    main()
