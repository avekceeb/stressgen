#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import sys
import socket
import time
import threading
import re
import signal

"""
Simple Web Server for StressGen 
   This 'lean and mean' :) utility is to monitor hosts
   via udp-listening for messages from 'stressgen' daemons
   (start stressgen with -M<host where this server expected>)
TODO:
   Should be refactored one day
"""

TIMEOUT = 20.0
HEARTBEAT_PORT = 60888
HostList = {}
HostStatus = {"on":0, "off":0}

Style = """
<style type="text/css">
h3#status {
    position: absolute;
    top: 0;
    left: 0;
    font-family:monospace;
    font-size: medium;
    width: 100%;
    text-align: center;
}
table#data {
    margin-top: 50px;
    margin-left: auto;
    margin-right: auto;
    background: none repeat scroll 0 0 linen;
    border: 1px dashed;
    width: 550px;
    empty-cells: show;
    font-family: monospace;
    font-size: small;
}
table#data thead {
    background-color: black;
    border:medium none;
    color: orange;
    font-weight:bold;
}
table#data td {
    border: solid 1px white;
    padding: 2px 2px 2px 15px;
}
table#data  table {
    border: none;
}
table#data table td {
    border: none;
    padding: 0 0 0 20px;
    margin: 0;
    font-family: monospace;
    font-size: small;
}
</style>
"""

HOST_ALIVE = ''
HOST_DEAD = " style='color:red;' "

def get_host_counts():
    return """<h3 id='status' name='status'>
    <span style='margin:0 20px; color:orange; background-color: black;'>::StressGen Monitor::</span>
    <span style='color:green;'>ONLINE: %d </span> 
    <span style='color:red;'>OFFLINE: %d </span> 
    <span style='color:gray;'>PEAK: %d</span></h3>""" % (HostStatus["on"], HostStatus["off"], len(HostList))

def get_table():
    table = ''
    HostStatus["on"] = 0
    HostStatus["off"] = 0
    for i in sorted(HostList):
        s = HostList[i]
        row = ""
        is_alive = HOST_ALIVE
        if s.has_key('t0') and s.has_key('N1'):
            dt = s['t1'] - s['t0']
            net_stat = '<table>'
            try:
                for iface in s['N1']:
                    net_stat += '<tr><td>%s</td><td>%d</td><td>%d</td></tr>\n' % \
                        (iface,
                        int((s['N1'][iface]['Tx']-s['N0'][iface]['Tx'])/dt),
                        int((s['N1'][iface]['Rx']-s['N0'][iface]['Rx'])/dt))
            except:
                net_stat += '<tr><td>???</td></tr>'
            net_stat += "</table>\n"
            row += "<td>%s</td>" % net_stat
            if TIMEOUT < (time.time() - s['t1']):
                is_alive = HOST_DEAD
                HostStatus["off"] += 1
            else:
                HostStatus["on"] += 1
        table += "<tr %s>" % is_alive
        table += "<td>%s - %s</td><td>%s</td>\n" % (i, HostList[i]['S'], HostList[i]['C'])
        table += row + "</tr>\n"
    return table


class ReqHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write("""<html><head><title>..::StressGen::..</title>
<script type='text/javascript'>
      setTimeout('location.reload(true)', 15000);
</script>
%s</head>
<body><table id='data' name='data' cellpadding='0' cellspacing='0'>
<thead>
<tr>
<td>Host</td>
<td>CPU Avg: 1m 5m 15m</td>
<td>Net: iface Tx Rx (Bytes/sec)</td>
</tr>
</thead>
""" % (Style))
        self.wfile.write(get_table())
        self.wfile.write("</table>")
        self.wfile.write(get_host_counts())
        self.wfile.write("</body></html>")
        return

    def do_POST(self):
        self.send_response(404)
        return


class HeartbeatListener(threading.Thread):
    def __init__(self):
        self.heartbeat_lstnr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.heartbeat_lstnr.bind(('', HEARTBEAT_PORT))
        self.reg = re.compile('^\s*(?P<iface>[a-z0-9]+):\s*(?P<rx>[0-9]+)\s*([0-9]+\s*){7}(?P<tx>[0-9]+)\s*')
        threading.Thread.__init__(self)

    def parse_stats(self, raw_msg):
        stats = {'t1': time.time(), 'C':'???'}
        a = raw_msg.split('\x00')
        for i in range(len(a)):
            if '' == a[i]:
                continue
            elif 'C' == a[i]:
                u = a[i+1].split()
                if len(u) < 3:
                    stats['C'] = repr(a[i+1])
                else:
                    stats['C'] = '%s %s %s' % (u[0], u[1], u[2]) 
            elif 'N' == a[i]:
                stats['N1'] = self.parse_network_stats(a[i+1])
            elif 'S' == a[i]:
                stats['S'] = a[i+1];

        return stats

    def parse_network_stats(self, msg):
        # skip header
        n = msg.find('\n')
        if -1 == n:
            return repr(msg)
        n = msg.find('\n', n+1)
        msg = msg[n+1:]
        ifaces = []
        n = 0
        while -1 != n:
           n = msg.find('\n', n+1)
           ifaces.append(n)
        n = 0
        iface_traffic = {}
        for i in ifaces[:-1]:
            m = self.reg.match(msg[n:i])
            if None != m:
                if 'lo' != m.group('iface'):
                    iface_traffic[m.group('iface')] = {'Rx':int(m.group('rx')),'Tx':int(m.group('tx'))}
            n = i + 1
        return iface_traffic

    def run(self):
        while True:
            msg, addr = self.heartbeat_lstnr.recvfrom(1024)
            if HostList.has_key(addr[0]) and HostList[addr[0]].has_key('N1'):
                HostList[addr[0]]['t0'] = HostList[addr[0]]['t1']
                HostList[addr[0]]['N0'] = HostList[addr[0]]['N1']
                HostList[addr[0]].update(self.parse_stats(msg))
            else:
                HostList[addr[0]] = self.parse_stats(msg)



class HttpSrv(threading.Thread, HTTPServer):
    def __init__(self, port):
        self.http_srv = None
        HTTPServer.__init__(self, ('', port), ReqHandler)
        threading.Thread.__init__(self)

    def run(self):
        self.serve_forever()

class Monitor:
    def __init__(self, port):
        self.hb = HeartbeatListener()
        self.hb.start()
        self.http = HttpSrv(port)
        self.http.start()

def main():
    http_port = 8080
    if 1 < len(sys.argv):
        http_port = int(sys.argv[1])
    m = Monitor(http_port)
    def shutup(signum, stack):
        m.hb._Thread__stop()
        m.http._Thread__stop()
        sys.exit(0)
    signal.signal(signal.SIGINT, shutup)
    while True:
        time.sleep(30)

if __name__ == '__main__':
    main()

