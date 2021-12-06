from datetime import datetime
import sys


class LogGenerator:

    def __init__(self, date, hits, with_header, remote_host='10.0.0.1', rfc931='-', authuser='apache', method='GET',
                 url='/api/help', proto='HTTP/1.0', status='200', content_len='1234'):
        self.hits = hits
        self._log_line = f'"{remote_host}","{rfc931}","{authuser}",{date},"{method} {url} {proto}",{status},{content_len}\n'
        if with_header:
            self.header = '"remotehost","rfc931","authuser","date","request","status","bytes"\n'
        else:
            self.header = ''

    def output(self):
        log = self.header
        for i in range(self.hits):
            log += self._log_line
        return log[:-1]


if __name__ == "__main__":
    test_number = sys.argv[1]
    ts_now = int(datetime.now().timestamp())
    if test_number == '1':
        # TEST 1: generate 1200 hits every 120s, should not trigger the alert
        gen = LogGenerator(ts_now, 1200, True)
        print(gen.output())
        ts_now += 120
        gen = LogGenerator(ts_now, 1200, False)
        print(gen.output())
    if test_number == '2':
        # TEST 2: generate 1201 hits every 120s, alert should be triggered twice
        # and alert should be disarmed when the first log of the second timestamp.
        gen = LogGenerator(ts_now, 1201, True)
        print(gen.output())
        ts_now += 120
        gen = LogGenerator(ts_now, 1201, False)
        print(gen.output())
    if test_number == '3':
        # TEST 3: Disordered log
        # In the first 120s there are 1201 hits, but there is 1 hits arrives at the
        # last second of traffic monitor windows and it's disordered.
        # One alert should be expected
        gen = LogGenerator(ts_now, 1200, True)
        print(gen.output())
        ts_now += 120
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        # then a disordered log arrives
        ts_now -= 1
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
    if test_number == '4':
        # TEST 4: Disordered log lost
        # time 0 : 1 hit
        # time 1 : 1 hit
        # time 2 : 1 hit
        # time 3 : 1 hit
        # time 4 : 1 hit
        # time 119: 1195 hits
        # time 122: 1 hit ( the time of this hit tests OOO(out-of-order) buffering)
        #           122 = winows size(120) - 1 + OOO size(3) which is the edge case
        #           if place this hits later than [winows size - 1 + OOO size] will
        #           cause missed alert.
        # disordered time 119: 1 hit
        # Expect w
        #
        # hen receives final disordered log
        # 0-119: 1201 hits / 120s: alert should be generated
        # 3-122: 1199 hits / 120s: alert should be disarmed
        ts_now = 0
        gen = LogGenerator(ts_now, 1, True)
        print(gen.output())
        ts_now = 1
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        ts_now = 2
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        ts_now = 3
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        ts_now = 4
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        ######################
        # at 119 1195 hits (0-119s = 1200hits)
        ts_now = 119
        gen = LogGenerator(ts_now, 1195, False)
        print(gen.output())
        ######################
        # t = 124 gen 1 hits (5-124s = 1195 hits)
        ts_now = 122
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
        # Disordered part
        # t = 119 add 1 hits, (0-119s= 1201hits)
        ts_now = 119
        gen = LogGenerator(ts_now, 1, False)
        print(gen.output())
