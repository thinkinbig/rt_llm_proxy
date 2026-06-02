"""
RealtimeSTT streaming ASR sidecar.

WebSocket  /v1/audio/transcriptions/streaming
  - Client sends raw 16 kHz mono s16le PCM as binary frames.
  - Server sends JSON events {"type": ..., "text": ...}:
        speech_start          VAD detected the user started talking (barge-in)
        partial   + text      live (interim) transcription from the realtime model
        final     + text      a completed utterance from the main model

This is the exact wire shape internal/model/cascade/stage_whisper.go already
parses, so the Go side needs no change — it just dials this server instead of
faster-whisper-server.

Wraps KoljaB/RealtimeSTT AudioToTextRecorder (Silero/WebRTC VAD + faster-whisper).
A RealtimeSTT recorder is a single, stateful audio stream, so each WebSocket
connection (= one voice session) gets its own recorder. The model therefore
loads per connection — acceptable for single-host, low-concurrency use; for
higher concurrency see RealtimeSTT/example_fastapi_server (inference pooling).
"""

from __future__ import annotations

import asyncio
import json
import os
import threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from RealtimeSTT import AudioToTextRecorder

# Main model produces the final transcription; the (smaller) realtime model
# produces the frequent partials that drive speculative LLM start.
MAIN_MODEL = os.getenv("STT_MODEL", "base.en")
REALTIME_MODEL = os.getenv("STT_REALTIME_MODEL", "tiny.en")
LANGUAGE = os.getenv("STT_LANGUAGE", "en")

app = FastAPI()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.websocket("/v1/audio/transcriptions/streaming")
async def transcribe(ws: WebSocket) -> None:
    await ws.accept()
    loop = asyncio.get_running_loop()
    outbox: asyncio.Queue[str] = asyncio.Queue()

    def emit(msg: dict) -> None:
        # Recorder callbacks fire on the recorder's own threads; hop the JSON
        # back onto the event loop for the WebSocket sender.
        loop.call_soon_threadsafe(outbox.put_nowait, json.dumps(msg))

    recorder = AudioToTextRecorder(
        model=MAIN_MODEL,
        realtime_model_type=REALTIME_MODEL,
        language=LANGUAGE,
        use_microphone=False,
        enable_realtime_transcription=True,
        use_main_model_for_realtime=False,
        spinner=False,
        on_recording_start=lambda: emit({"type": "speech_start"}),
        on_realtime_transcription_update=lambda text: emit({"type": "partial", "text": text}),
    )

    stop = threading.Event()

    def transcribe_loop() -> None:
        # recorder.text(cb) blocks until a full utterance, then calls cb with
        # the final text. Loop it for the life of the connection.
        while not stop.is_set():
            recorder.text(lambda text: emit({"type": "final", "text": text}))

    worker = threading.Thread(target=transcribe_loop, daemon=True)
    worker.start()

    async def sender() -> None:
        while True:
            await ws.send_text(await outbox.get())

    send_task = asyncio.create_task(sender())
    try:
        while True:
            # Our cascade ASR stage sends 16 kHz mono s16le; feed_audio's
            # default original_sample_rate is 16000, so no resample needed.
            recorder.feed_audio(await ws.receive_bytes())
    except WebSocketDisconnect:
        pass
    finally:
        stop.set()
        send_task.cancel()
        recorder.shutdown()  # unblocks the in-flight recorder.text() call
