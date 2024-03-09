import io

class Parser:
    def _parse_wire_protocol(self, msg_buffer):
        current_line = msg_buffer.readline()
        msg_type, remaining = chr(current_line[0]), current_line[1:]
        if msg_type == '+':
            return remaining.rstrip(b'\r\n').decode()
        elif msg_type == ':':
            return int(remaining)
        elif msg_type == '$':
            msg_length = int(remaining)
            if msg_length == -1:
                return None
            result = msg_buffer.read(msg_length)
            msg_buffer.readline() # move past \r\n
            return result
        elif msg_type == '*':
            array_length = int(remaining)
            return [self._parse_wire_protocol(msg_buffer) for _ in range(array_length)]
        
    def parse_wire_protocol(self, message):
        return self._parse_wire_protocol(io.BytesIO(message))
    
    def serialize_to_wire(self, value):
        if isinstance(value, str):
            return ('+%s' % value).encode() + b'\r\n'
        elif isinstance(value, bool) and value:
            return b"+OK\r\n"
        elif isinstance(value, int):
            return (':%s' % value).encode() + b'\r\n'
        elif isinstance(value, bytes):
            return (b'$' + str(len(value)).encode() +
                    b'\r\n' + value + b'\r\n')
        elif value is None:
            return b'$-1\r\n'
        elif isinstance(value, list):
            base = b'*' + str(len(value)).encode() + b'\r\n'
            for item in value:
                base += self.serialize_to_wire(item)
            return base