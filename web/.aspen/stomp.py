#
# STOMP protocol adapter for diesel socket engine
#

import diesel
import time
import uuid

STOMP_PORT = 61613

class StompTimeout(Exception): pass

class TimeoutHandler(object):
    def __init__(self, timeout):
        self._timeout = timeout
        self._start = time.time()

    def remaining(self, raise_on_timeout=True):
        remaining = self._timeout - (time.time() - self._start)
        if remaining < 0 and raise_on_timeout:
            self.timeout()
        return remaining

    def timeout(self):
        raise StompTimeout("Timeout after %.2f seconds" % self._timeout)

class StompFrame(object):

    def __init__(self, command, headers, body):
        self.command = command
        self.headers = headers
        self.body = body

class StompClient(diesel.Client):

    def __init__(self, host='localhost', port=STOMP_PORT):
        diesel.Client.__init__(self, host, port)
        self.msgs_received = [ ]

    @diesel.call
    def connect(self, headers=None):
        self._write_frame("CONNECT", headers)
        f = self._read_frame(100)
        if f.command == "CONNECTED":
            self.session_id = f.headers["session"]
            self.connected = True
        else:
            raise IOError("Invalid frame after CONNECT: %s" % str(f))

    @diesel.call
    def disconnect(self, headers=None):
        self._write_frame("DISCONNECT", headers)
        diesel.Client.close(self)
        self.connected = False

    @diesel.call
    # not done yet..
    def __call(self, dest_name, body, timeout=60, call_sleep_sec=0.005,
             headers=None, receipt=False):
        resp_body = [ ]
        reply_to_id = uuid.uuid4().hex
        if not headers:
            headers = { }
        headers['reply-to'] = reply_to_id
        self.send(dest_name, body, headers, receipt)
        timeout_sec = time.time() + timeout
        while time.time() < timeout_sec and len(resp_body) == 0:
            time.sleep(self.call_sleep_sec)
        if resp_body:
            return resp_body[0]
        else:
            msg = "No response to: %s within timeout: %.1f" % (body, timeout)
            raise StompTimeout(msg)

    @diesel.call
    def send(self, dest_name, body, headers=None):
        if not headers:
            headers = { }
        headers["destination"] = dest_name
        self._create_receipt(headers)
        self._write_frame("SEND", headers=headers, body=body)
        self._wait_for_receipt(headers)

    @diesel.call
    def subscribe(self, dest_name, auto_ack=True, headers=None):
        if not headers:
            headers = { }
        ack = "client"
        if auto_ack: ack = "auto"
        headers["destination"] = dest_name
        headers["ack"] = ack
        self._create_receipt(headers)
        self._write_frame("SUBSCRIBE", headers=headers)
        self._wait_for_receipt(headers)

    @diesel.call
    def unsubscribe(self, dest_name, headers=None):
        if not headers:
            headers = { }
        headers["destination"] = dest_name
        self._create_receipt(headers)
        self._write_frame("UNSUBSCRIBE", headers=headers)
        self._wait_for_receipt(headers)

    @diesel.call
    def ack(self, msg_id, headers=None):
        if not headers:
            headers = { }
        headers["message-id"] = msg_id
        self._create_receipt(headers)
        self._write_frame("ACK", headers=headers)
        self._wait_for_receipt(headers)

    @diesel.call
    def receive(self, timeout=-1):
        if (len(self.msgs_received) > 0):
            return self.msgs_received.pop(0)
        else:
            try:
                return self._read_frame(timeout)
            except StompTimeout:
                return None
            
    def _create_headers(self, receipt, headers):
        if receipt:
            headers.append("receipt:%s" % uuid.uuid4().hex)
        return headers

    def _create_receipt(self, headers):
        if headers.has_key("receipt"):
            headers["receipt"] = uuid.uuid4().hex

    def _wait_for_receipt(self, headers, timeout=60):
        if headers.has_key("receipt"):
            receipt = headers["receipt"]
            timeout = time.time() + timeout
            while time.time() < timeout:
                try:
                    frame = self._read_frame(1)
                    if frame.command == 'RECEIPT' and \
                           frame.headers['receipt-id'] == receipt:
                        return
                    else:
                        self.msgs_received.append(frame)
                except StompTimeout:
                    pass
            err = "No receipt: %s received in: %.2f" % (receipt, timeout)
            raise StompTimeout(err)

    def _write_frame(self, command, headers=None, body=None):
        #print "SEND: command=%s headers=%s body=%s" % \
        #      (command, str(headers), str(body))
        if body:
            headers["content-length"] = len(body)
        diesel.send(command+"\n")
        if headers:
            for k,v in headers.items():
                diesel.send(k + ": " + str(v) + "\n")
        diesel.send("\n")
        if body:
            diesel.send(body)
        diesel.send(chr(0))

    def _read_frame(self, timeout=-1):
        self.timeout = timeout
        
        frame = StompFrame(None, { }, None)

        # command is first
        frame.command = self._readline().strip()
        
        # read headers
        content_length = 0
        line = self._readline()
        while line.strip() != "":
            pos = line.find(":")
            if pos >= 0:
                key = line[:pos].strip()
                val = line[(pos+1):].strip()
                frame.headers[key] = val
                if key.lower() == "content-length":
                    content_length = int(val)
            line = self._readline()
                
        # read body
        if content_length > 0:
            frame.body = diesel.receive(content_length)
            while True:
                if diesel.receive(1) == chr(0): break
        else:
            body = [ ]
            c = diesel.receive(1)
            while c != chr(0):
                body.append(c)
                c = diesel.receive(1)
            frame.body = "".join(body).rstrip("\n").rstrip("\r")

        # TODO: ActiveMQ sends an extra \n after the null terminator
        # RabbitMQ doesn't.. spec doesn't seem to mention it.
        # need a way to detect this anomoly and deal..
        #line = self._readline()

        #print "RECV: command=%s headers=%s body=%s" % \
        #      (frame.command, frame.headers, frame.body)
        return frame

    def _readline(self):
        if self.timeout > 0:
            timeout_handler = TimeoutHandler(self.timeout)
            ev, line = diesel.first(until="\n", sleep=timeout_handler.remaining())
            if ev == 'sleep': timeout_handler.timeout()
            else: return line
        else:
            line = diesel.until("\n")
            if line:
                return line
            else:
                raise StompTimeout
