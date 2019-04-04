import argparse
import datetime
import queue
import select
import socket
import struct
import threading
import time
# import mutex

import ntplib

import numpy as np

parser = argparse.ArgumentParser(
    description="NTP server with a small random gain")


parser.add_argument("-s", "--server", default="",
                    help=("NTP server for fetching accurate time. "
                          "Default: '' (using system clock). "
                          "(Example: 'europe.pool.ntp.org')"))
parser.add_argument("-t", "--timezone", default=9 * 3600,
                    help="Timezone in seconds. Default: 9 * 3600")
parser.add_argument("-m", "--meandelta", default=5 * 60,
                    help="Mean delta time. Default: 5 * 60")
parser.add_argument("-M", "--maxdelta", default=30 * 60,
                    help="Max delta time. Default: 30 * 60")
parser.add_argument("-F", "--forcedelta",
                    help="Force all-time constant delta time. (for debugging)")

args = parser.parse_args()

"""
「正確な時刻」を取得するための NTP サーバー．
ローカルでサーバーとクライアントを両方動かす場合のみ指定が必要．
さもなくば，空文字列で良い（その場合はシステムの時計を参照する．）
"""
NTP_SERVER_FOR_ACCURATE_CURRENT_TIME = args.server
# NTP_SERVER_FOR_ACCURATE_CURRENT_TIME = "europe.pool.ntp.org"

"""
タイムゾーン（GMT からのずれ[秒]）．（ランダム時計の日内スケジューリングにのみ影響）
"""
TIMEZONE = float(args.timezone)

"""
「本日のランダム進み幅」の平均値[秒]．
"""
MEAN_DELTA = float(args.meandelta)
MAX_DELTA = float(args.maxdelta)
assert(0 <= MEAN_DELTA <= MAX_DELTA)
FORCE_DELTA = None
if args.forcedelta is not None:
    FORCE_DELTA = float(args.forcedelta)
    assert(0 <= FORCE_DELTA)

taskQueue = queue.Queue()
stopFlag = False


def system_to_ntp_time(timestamp):
    """Convert a system time to a NTP time.

    Parameters:
    timestamp -- timestamp in system time

    Returns:
    corresponding NTP time
    """
    return timestamp + NTP.NTP_DELTA


def _to_int(timestamp):
    """Return the integral part of a timestamp.

    Parameters:
    timestamp -- NTP timestamp

    Retuns:
    integral part
    """
    return int(timestamp)


def _to_frac(timestamp, n=32):
    """Return the fractional part of a timestamp.

    Parameters:
    timestamp -- NTP timestamp
    n         -- number of bits of the fractional part

    Retuns:
    fractional part
    """
    return int(abs(timestamp - _to_int(timestamp)) * 2**n)


def _to_time(integ, frac, n=32):
    """Return a timestamp from an integral and fractional part.

    Parameters:
    integ -- integral part
    frac  -- fractional part
    n     -- number of bits of the fractional part

    Retuns:
    timestamp
    """
    return integ + float(frac) / 2**n


def get_accurate_current_time():
    """
    正しい現在時刻を取得する．これは信頼できる時刻で，time.time() で取得できるものと同じフォーマットとする．
    """
    if NTP_SERVER_FOR_ACCURATE_CURRENT_TIME == "":
        return time.time()
    else:
        try:
            res = ntplib.NTPClient().request(NTP_SERVER_FOR_ACCURATE_CURRENT_TIME, version=3)
            dt = datetime.datetime.strptime(time.ctime(res.tx_time), "%a %b %d %H:%M:%S %Y")
            t = time.mktime(dt.timetuple()) + dt.microsecond / 1e6
            print("time provided by NTP_SERVER_FOR_ACCURATE_CURRENT_TIME: ", t)
            print("time provided by system (time.time()): ", time.time())
        except ntplib.NTPException:
            print("error while trying to connect NTP_SERVER_FOR_ACCURATE_CURRENT_TIME")
            return None
        return t


def get_random_delta_of_day(seed):
    """
    「進み幅分布」からランダムに進み幅をサンプルする．
    """
    np.random.seed(seed)
    if np.random.rand() < 0.5:
        while 1:
            x = -np.log(np.random.rand()) * MEAN_DELTA
            if x <= MAX_DELTA:
                break
        return x
    else:
        return 0


def get_schedule_in_day(time_ratio_in_day):
    """
    日内スケジュール．
    """
    x = time_ratio_in_day
    if x < 0.25:
        return x / 0.25
    elif x < 0.5:
        return 1.0
    elif x < 0.75:
        return 3.0 - x / 0.25
    else:
        return 0.0


def get_gain_schedule(t):
    """
    時刻 t における「進み幅（スケジュール上の）」[秒] を返す．

    Args:
        t: 時刻．time.time() で取得できるものと同じフォーマットとする．

    Returns:
        (float) 進み幅 [秒]．
    """
    if FORCE_DELTA is not None:
        return FORCE_DELTA
    t += TIMEZONE # ローカル時刻に修正．
    day, sec_in_day = int(t / 86400), t % 86400
    delta_of_day = get_random_delta_of_day(day)
    coef = get_schedule_in_day(sec_in_day / 86400)
    print("delta_of_day ==", delta_of_day)
    print("coef ==", coef)
    return delta_of_day * coef


class NTPException(Exception):
    """Exception raised by this module."""
    pass


class NTP:
    """Helper class defining constants."""

    _SYSTEM_EPOCH = datetime.date(*time.gmtime(0)[0:3])
    """system epoch"""
    _NTP_EPOCH = datetime.date(1900, 1, 1)
    """NTP epoch"""
    NTP_DELTA = (_SYSTEM_EPOCH - _NTP_EPOCH).days * 24 * 3600
    """delta between system and NTP time"""

    REF_ID_TABLE = {
            'DNC': "DNC routing protocol",
            'NIST': "NIST public modem",
            'TSP': "TSP time protocol",
            'DTS': "Digital Time Service",
            'ATOM': "Atomic clock (calibrated)",
            'VLF': "VLF radio (OMEGA, etc)",
            'callsign': "Generic radio",
            'LORC': "LORAN-C radionavidation",
            'GOES': "GOES UHF environment satellite",
            'GPS': "GPS UHF satellite positioning",
    }
    """reference identifier table"""

    STRATUM_TABLE = {
        0: "unspecified",
        1: "primary reference",
    }
    """stratum table"""

    MODE_TABLE = {
        0: "unspecified",
        1: "symmetric active",
        2: "symmetric passive",
        3: "client",
        4: "server",
        5: "broadcast",
        6: "reserved for NTP control messages",
        7: "reserved for private use",
    }
    """mode table"""

    LEAP_TABLE = {
        0: "no warning",
        1: "last minute has 61 seconds",
        2: "last minute has 59 seconds",
        3: "alarm condition (clock not synchronized)",
    }
    """leap indicator table"""


class NTPPacket:
    """NTP packet class.

    This represents an NTP packet.
    """

    _PACKET_FORMAT = "!B B B b 11I"
    """packet format to pack/unpack"""

    def __init__(self, version=2, mode=3, tx_timestamp=0):
        """Constructor.

        Parameters:
        version      -- NTP version
        mode         -- packet mode (client, server)
        tx_timestamp -- packet transmit timestamp
        """
        self.leap = 0
        """leap second indicator"""
        self.version = version
        """version"""
        self.mode = mode
        """mode"""
        self.stratum = 0
        """stratum"""
        self.poll = 0
        """poll interval"""
        self.precision = 0
        """precision"""
        self.root_delay = 0
        """root delay"""
        self.root_dispersion = 0
        """root dispersion"""
        self.ref_id = 0
        """reference clock identifier"""
        self.ref_timestamp = 0
        """reference timestamp"""
        self.orig_timestamp = 0
        self.orig_timestamp_high = 0
        self.orig_timestamp_low = 0
        """originate timestamp"""
        self.recv_timestamp = 0
        """receive timestamp"""
        self.tx_timestamp = tx_timestamp
        self.tx_timestamp_high = 0
        self.tx_timestamp_low = 0
        """tansmit timestamp"""

    def to_data(self):
        """Convert this NTPPacket to a buffer that can be sent over a socket.

        Returns:
        buffer representing this packet

        Raises:
        NTPException -- in case of invalid field
        """

        """
        print("======== ntp packet which will be sent to client ========")
        print("ref:     ", self.ref_timestamp)
        print("orig:    ", _to_time(self.orig_timestamp_high, self.orig_timestamp_low))
        print("receive: ", self.recv_timestamp)
        print("transmit:", self.tx_timestamp)
        print("=========================================================")
        """
        try:
            packed = struct.pack(NTPPacket._PACKET_FORMAT,
                (self.leap << 6 | self.version << 3 | self.mode),
                self.stratum,
                self.poll,
                self.precision,
                _to_int(self.root_delay) << 16 | _to_frac(self.root_delay, 16),
                _to_int(self.root_dispersion) << 16 |
                _to_frac(self.root_dispersion, 16),
                self.ref_id,
                _to_int(self.ref_timestamp),
                _to_frac(self.ref_timestamp),
                # Change by lichen, avoid loss of precision
                self.orig_timestamp_high,
                self.orig_timestamp_low,
                _to_int(self.recv_timestamp),
                _to_frac(self.recv_timestamp),
                _to_int(self.tx_timestamp),
                _to_frac(self.tx_timestamp))
        except struct.error:
            raise NTPException("Invalid NTP packet fields.")
        return packed

    def from_data(self, data):
        """Populate this instance from a NTP packet payload received from
        the network.

        Parameters:
        data -- buffer payload

        Raises:
        NTPException -- in case of invalid packet format
        """
        try:
            unpacked = struct.unpack(NTPPacket._PACKET_FORMAT,
                    data[0:struct.calcsize(NTPPacket._PACKET_FORMAT)])
        except struct.error:
            raise NTPException("Invalid NTP packet.")

        self.leap = unpacked[0] >> 6 & 0x3
        self.version = unpacked[0] >> 3 & 0x7
        self.mode = unpacked[0] & 0x7
        self.stratum = unpacked[1]
        self.poll = unpacked[2]
        self.precision = unpacked[3]
        self.root_delay = float(unpacked[4]) / 2**16
        self.root_dispersion = float(unpacked[5]) / 2**16
        self.ref_id = unpacked[6]
        self.ref_timestamp = _to_time(unpacked[7], unpacked[8])
        self.orig_timestamp = _to_time(unpacked[9], unpacked[10])
        self.orig_timestamp_high = unpacked[9]
        self.orig_timestamp_low = unpacked[10]
        self.recv_timestamp = _to_time(unpacked[11], unpacked[12])
        self.tx_timestamp = _to_time(unpacked[13], unpacked[14])
        self.tx_timestamp_high = unpacked[13]
        self.tx_timestamp_low = unpacked[14]
        """
        print("======== ntp packet from client ========")
        print("ref:     ", self.ref_timestamp)
        print("orig:    ", self.orig_timestamp)
        print("receive: ", self.recv_timestamp)
        print("transmit:", self.tx_timestamp)
        print("========================================")
        """

    def GetTxTimeStamp(self):
        return (self.tx_timestamp_high,self.tx_timestamp_low)

    def SetOriginTimeStamp(self,high,low):
        self.orig_timestamp_high = high
        self.orig_timestamp_low = low


class RecvThread(threading.Thread):
    def __init__(self,socket):
        threading.Thread.__init__(self)
        self.socket = socket

    def run(self):
        global taskQueue,stopFlag
        while True:
            if stopFlag:
                print("RecvThread Ended")
                break
            rlist, wlist, elist = select.select([self.socket], [], [], 1)
            if len(rlist) != 0:
                print("[{}] Received {} packets".format(datetime.datetime.now(), len(rlist)))
                for tempSocket in rlist:
                    try:
                        data, addr = tempSocket.recvfrom(1024)
                        current_time = get_accurate_current_time()
                        if current_time is not None:
                            recvTimestamp = recvTimestamp = system_to_ntp_time(current_time)
                            taskQueue.put((data, addr, recvTimestamp))
                    except socket.error as msg:
                        print(msg)


class WorkThread(threading.Thread):
    def __init__(self,socket):
        threading.Thread.__init__(self)
        self.socket = socket

    def run(self):
        global taskQueue, stopFlag
        while True:
            if stopFlag:
                print("WorkThread Ended")
                break
            try:
                data, addr, recvTimestamp = taskQueue.get(timeout=1)
                recvPacket = NTPPacket()
                recvPacket.from_data(data)
                timeStamp_high, timeStamp_low = recvPacket.GetTxTimeStamp()
                sendPacket = NTPPacket(version=3, mode=4)
                sendPacket.stratum = 2
                sendPacket.poll = 10
                '''
                sendPacket.precision = 0xfa
                sendPacket.root_delay = 0x0bfa
                sendPacket.root_dispersion = 0x0aa7
                sendPacket.ref_id = 0x808a8c2c
                '''
                sendPacket.ref_timestamp = recvTimestamp-5
                sendPacket.SetOriginTimeStamp(timeStamp_high,timeStamp_low)
                sendPacket.recv_timestamp = recvTimestamp

                current_time = get_accurate_current_time()

                if current_time is not None:
                    sendPacket.tx_timestamp = system_to_ntp_time(current_time)
                    
                    # 送るパケットに含まれる時刻情報を，gain 秒だけ進める．
                    gain = get_gain_schedule(current_time)
                    sendPacket.recv_timestamp += gain
                    sendPacket.tx_timestamp += gain

                    socket.sendto(sendPacket.to_data(),addr)
                    print("Sended to %s:%d" % (addr[0],addr[1]))
                    print()
            except queue.Empty:
                continue
                
        
listenIp = "0.0.0.0"
listenPort = 123
socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
socket.bind((listenIp,listenPort))
print("local socket: ", socket.getsockname())
recvThread = RecvThread(socket)
recvThread.start()
workThread = WorkThread(socket)
workThread.start()

while True:
    try:
        time.sleep(0.5)
    except KeyboardInterrupt:
        print("Exiting...")
        stopFlag = True
        recvThread.join()
        workThread.join()
        #socket.close()
        print("Exited")
        break
        
