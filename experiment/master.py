import socket
import threading

counter = 0
lock = threading.Lock()

def handle_data(sock, addr):
    global counter
    lock.acquire()
    counter += 1
    sock.send(str(counter).encode('utf-8'))
    lock.release()

if __name__ == '__main__':
    # 获取本机电脑名
    master_name = socket.getfqdn(socket.gethostname())
    # 获取本机ip
    master_addr = socket.gethostbyname(master_name)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((master_addr, 16181))
    s.listen(1000)
    counter = 0
    while 1:
        sock, addr = s.accept()
        t = threading.Thread(target=handle_data, args=(sock, addr,))
        t.start()