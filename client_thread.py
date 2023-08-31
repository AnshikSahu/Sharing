# Import socket module
import socket		
import threading	
from time import sleep

def main():
    port = 9801
    svayu = socket.socket()
    svayu.connect(("vayu.iitd.ac.in", 9801))
    
    while True:
        try:
            svayu.sendall(b"SENDLINE\n")
            response = svayu.recv(4096).decode('utf-8')
            print(response)
            sendmaster = threading.Thread(target = sendline, args = (response,))
            print("printing the number of threads active ",threading.active_count())
            sendmaster.start()
        except Exception as e:
            print("error: ", e)
            svayu.close()
            break
        sleep(0.01)



def sendline(sendtext):
    #what we want to send, format is: linenum + line
    # sendtext = linenum + '\n' + line
    echo = ''
    while (echo != sendtext):
        s = socket.socket()	
        # Define the port on which you want to connect
        port = 12345			
        # connect to the server on local computer
        s.connect(("10.194.28.9", port))
        

        s.send(sendtext.encode())

        # receive data from the server and decoding to get the string.
        echo =  s.recv(4096).decode()
        if (echo=="1"):
            s.close()
            break
        # print(echo, sendtext)
        # close the connection
        s.close()
    return

if __name__ == "__main__":
    main()
