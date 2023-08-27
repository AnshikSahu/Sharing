import socket
import threading
import random

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

    while True:
        client_socket, client_address = server_socket.accept()

        # Create a new socket for the client
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the new socket to a random port
        client_port = base_port + random.randint(1, 65535)
        new_socket.bind((client_address[0], client_port))

        # Start a new thread to handle the client
        client_thread = threading.Thread(target=handle_client, args=(new_socket, client_address))
        client_thread.start()

if __name__ == "__main__":
    main()
