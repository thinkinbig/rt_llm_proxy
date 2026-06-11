package doubao

// Volcengine (Doubao) speech WebSocket binary V3 frame protocol, shared by the
// ASR / TTS / realtime-dialogue products. Ported from the reference Python impl.
//
// frame = 4-byte header
//         + optional (event int32 [+ session_id len+bytes])
//         + optional sequence int32
//         + optional error_code uint32 (server error frames)
//         + payload length uint32 + payload
//
// header bytes:
//   0: protocol_version(hi4) | header_size(lo4, =1 -> 4 bytes)
//   1: message_type(hi4)     | flags(lo4)
//   2: serialization(hi4)    | compression(lo4)
//   3: reserved 0x00

import (
	"encoding/binary"
	"errors"
)

const (
	dbProtoVersion = 0b0001
	dbHeaderSize   = 0b0001 // unit: 4 bytes

	// message_type (hi4)
	dbMsgFullClient  = 0b0001 // client JSON request
	dbMsgAudioClient = 0b0010 // client raw audio
	dbMsgError       = 0b1111 // server error (error_code in extension)

	// flags (lo4)
	dbFlagPosSeq    = 0b0001
	dbFlagNegSeq    = 0b0011
	dbFlagWithEvent = 0b0100

	// serialization (hi4 of byte2)
	dbSerialRaw  = 0b0000
	dbSerialJSON = 0b0001

	// compression (lo4 of byte2)
	dbCompressGzip = 0b0001

	// events (only those we send/handle; full enum in the V3 protocol docs)
	dbEvStartConnection    = 1
	dbEvFinishConnection   = 2
	dbEvConnectionStarted  = 50
	dbEvConnectionFailed   = 51
	dbEvConnectionFinished = 52
	dbEvStartSession       = 100
	dbEvSessionFinished    = 152
	dbEvSessionFailed      = 153
	dbEvTaskRequest        = 200
	dbEvTTSResponse        = 352 // payload is raw audio
	dbEvASRInfo            = 450 // first word of user speech detected → barge-in signal
	dbEvASRResponse        = 451 // user speech recognition (JSON: results[].text)
	dbEvChatResponse       = 550 // model reply text chunk (JSON: content)
	dbEvChatEnded          = 559 // model turn complete
)

func dbIsConnectionEvent(event int32) bool {
	switch event {
	case dbEvStartConnection, dbEvFinishConnection,
		dbEvConnectionStarted, dbEvConnectionFailed, dbEvConnectionFinished:
		return true
	}
	return false
}

// dbBuildFrame builds a client frame. We always set WITH_EVENT; session_id is
// included for non-connection events.
func dbBuildFrame(msgType, serial, compress byte, event int32, sessionID string, payload []byte) []byte {
	out := []byte{
		(dbProtoVersion << 4) | dbHeaderSize,
		(msgType << 4) | dbFlagWithEvent,
		(serial << 4) | compress,
		0x00,
	}
	out = binary.BigEndian.AppendUint32(out, uint32(event))
	if !dbIsConnectionEvent(event) && sessionID != "" {
		out = binary.BigEndian.AppendUint32(out, uint32(len(sessionID)))
		out = append(out, sessionID...)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

type dbFrame struct {
	msgType   byte
	flags     byte
	serial    byte
	compress  byte
	event     int32
	sessionID string
	errorCode uint32
	payload   []byte
}

var errShortFrame = errors.New("doubao: short frame")

func dbParseFrame(raw []byte) (*dbFrame, error) {
	if len(raw) < 4 {
		return nil, errShortFrame
	}
	f := &dbFrame{
		msgType:  raw[1] >> 4,
		flags:    raw[1] & 0x0f,
		serial:   raw[2] >> 4,
		compress: raw[2] & 0x0f,
	}
	off := int(raw[0]&0x0f) * 4

	read32 := func() (uint32, bool) {
		if off+4 > len(raw) {
			return 0, false
		}
		v := binary.BigEndian.Uint32(raw[off : off+4])
		off += 4
		return v, true
	}

	if f.flags&dbFlagWithEvent != 0 {
		ev, ok := read32()
		if !ok {
			return nil, errShortFrame
		}
		f.event = int32(ev)
		if !dbIsConnectionEvent(f.event) {
			sl, ok := read32()
			if !ok {
				return nil, errShortFrame
			}
			if sl > 0 {
				if off+int(sl) > len(raw) {
					return nil, errShortFrame
				}
				f.sessionID = string(raw[off : off+int(sl)])
				off += int(sl)
			}
		}
	}
	if f.flags&(dbFlagPosSeq|dbFlagNegSeq) != 0 {
		if _, ok := read32(); !ok {
			return nil, errShortFrame
		}
	}
	if f.msgType == dbMsgError {
		ec, ok := read32()
		if !ok {
			return nil, errShortFrame
		}
		f.errorCode = ec
	}
	size, ok := read32()
	if !ok {
		return nil, errShortFrame
	}
	if off+int(size) > len(raw) {
		return nil, errShortFrame
	}
	f.payload = raw[off : off+int(size)]
	return f, nil
}
