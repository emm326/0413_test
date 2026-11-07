"""Little-endian, versioned binary contract shared with firmware/common/protocol/Wire.h."""

from dataclasses import dataclass
import binascii
import math
import struct

MAGIC, VERSION, BROADCAST = 0xA55A, 3, 0xFFFF
DATA, START, STOP, CONFIRM, STATUS, DISCOVER, ACK = range(1, 8)
GPS_HEALTHY, CLOCK_HEALTHY = 1, 2
HEADER = struct.Struct("<HBBHHIIQHBB")
START_OPTIONS = struct.Struct("<fIBBH")
STATUS_PAYLOAD = struct.Struct("<ffIIHBB")
ACK_ENTRY = struct.Struct("<HII")
STATE_NAMES = {0: "启动", 1: "待机", 2: "已确认待启动", 3: "采集中", 4: "补传尾包", 5: "硬件异常"}
RESULT_NAMES = {0: "已接受", 1: "未就绪", 2: "参数错误", 3: "忙碌", 4: "启动时间过近"}


@dataclass(frozen=True)
class Frame:
    kind: int
    node: int
    flags: int = 0
    run: int = 0
    sequence: int = 0
    epoch_us: int = 0
    rate: int = 100
    count: int = 0
    range_g: int = 4
    payload: bytes = b""

    def encode(self):
        body = (
            HEADER.pack(
                MAGIC,
                VERSION,
                self.kind,
                self.node,
                self.flags,
                self.run,
                self.sequence,
                self.epoch_us,
                self.rate,
                self.count,
                self.range_g,
            )
            + self.payload
        )
        if len(body) + 2 > 255:
            raise ValueError("packet exceeds 255 bytes")
        return body + struct.pack("<H", binascii.crc_hqx(body, 0xFFFF))

    @classmethod
    def decode(cls, raw):
        if not 30 <= len(raw) <= 255:
            raise ValueError("invalid packet length")
        values = HEADER.unpack_from(raw)
        if values[:2] != (MAGIC, VERSION):
            raise ValueError("unsupported magic/version")
        if not DATA <= values[2] <= ACK:
            raise ValueError("invalid frame type")
        if binascii.crc_hqx(raw[:-2], 0xFFFF) != int.from_bytes(raw[-2:], "little"):
            raise ValueError("CRC mismatch")
        frame = cls(*values[2:], payload=raw[HEADER.size : -2])
        if frame.kind == DATA:
            if (
                not 1 <= frame.count <= 25
                or len(frame.payload) != frame.count * 6
                or frame.rate not in (50, 100)
                or frame.range_g not in (2, 4, 8, 16)
            ):
                raise ValueError("invalid data layout")
        return frame

    def samples(self):
        if self.kind != DATA:
            raise ValueError("not a data frame")
        return list(struct.iter_unpack("<hhh", self.payload))


def start_frame(
    run,
    epoch_us,
    rate=100,
    range_g=4,
    sf=6,
    duration=0,
    node=BROADCAST,
    uplink=0.0,
    node_count=2,
    node_mask=0x3FF,
):
    if (
        rate not in (50, 100)
        or range_g not in (2, 4, 8, 16)
        or sf not in (5, 6, 7)
        or not 1 <= node_count <= 10
    ):
        raise ValueError("invalid sampling or radio settings")
    if not run or epoch_us <= 0 or duration < 0:
        raise ValueError("invalid run/time/duration")
    return Frame(
        START,
        node,
        run=run,
        epoch_us=epoch_us,
        rate=rate,
        range_g=range_g,
        payload=START_OPTIONS.pack(uplink, duration, sf, 0, node_mask),
    )


def airtime_seconds(length, sf=6, bandwidth=125, preamble=12):
    bits = max(0, 8 * length + 16 - 4 * sf + (0 if sf in (5, 6) else 8) + 20)
    symbols = preamble + 8 + (6.25 if sf in (5, 6) else 4.25) + math.ceil(bits / (4 * sf)) * 5
    return symbols * (2**sf) / (bandwidth * 1000)
