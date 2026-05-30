import socket
import json

class config:
    stream_host = "localhost"
    port = 6100

def connectTCP():
    TCP_IP = config.stream_host
    TCP_PORT = config.port

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print(f"Waiting for connection on port {TCP_PORT}...")
    connection, address = s.accept()
    print(f"Connected to {address}")

    return connection

tcp_connection = connectTCP()

# prepare the payload
payload = dict()
# payload can be any object serializable by json
payload["message"] = "Hello, this is a test message."
payload = (json.dumps(payload) + "\n").encode()
try:
    tcp_connection.send(payload)
    print("Sended frame")
except BrokenPipeError:
    print("Either image size is too big for the dataset or the connection was closed")
except Exception as error_message:
    print(f"Exception thrown but was handled: {error_message}")

