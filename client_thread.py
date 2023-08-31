# Import socket module
import socket		
import threading	
from time import sleep
s = socket.socket()	
        # Define the port on which you want to connect
port = 12345			
# connect to the server on local computer
s.connect(("10.194.28.9", port))
def main():
    global s
    port = 9801
    svayu = socket.socket()
    svayu.connect(("vayu.iitd.ac.in", 9801))
    
    while True:
        try:
            svayu.sendall(b"SENDLINE\n")
            response = svayu.recv(4096).decode('utf-8')
            #print(response)
            # sendmaster = threading.Thread(target = sendline, args = (response,))
            # print("printing the number of threads active ",threading.active_count())
            # sendmaster.start()
            if(response=="-1\n-1\n"):
                continue
            sendline(response)
        except Exception as e:
            print("error: ", e)
            svayu.close()
            break
        sleep(0.03)
    s.close()
    svayu.close()



def sendline(sendtext):
    global s
    #what we want to send, format is: linenum + line
    # sendtext = linenum + '\n' + line
    echo = None
    while (echo != '0'):
        try:
        # s = socket.socket()	
        # # Define the port on which you want to connect
        # port = 12345			
        # # connect to the server on local computer
        # s.connect(("10.194.28.9", port))
        

            s.send(sendtext.encode())

            # receive data from the server and decoding to get the string.
            echo =  s.recv(4096).decode()
            if (echo=="1"):
                s.close()
                break
        except:
            print("error")
        # print(echo, sendtext)
        # close the connection
    return

if __name__ == "__main__":
    main()
