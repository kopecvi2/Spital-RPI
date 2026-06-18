"""Raspberry Pi MQTT video player.

Usage:
  - Subscribe to COMMAND_TOPIC (pi/video/command)
    - play;<id>;<video_path>
    - play;<id>;<video_path>;<audio_path>
    - stop

  - publishes to STATUS_TOPIC (pi/video/status)
    - when playback fully finishes, publishes only the playback id

  - Subscribe to STATUS_CHECK_TOPIC (pi/video/status_check)
    - any message triggers an immediate check

  - publishes to STATUS_CHECK_RESPONSE_TOPIC (pi/video/status_check_response)
    - 0 if no playback is active
    - 1 if video or audio is currently running
"""

import os
import subprocess
import threading
import time
import paho.mqtt.client as mqtt


MQTT_BROKER = "192.168.0.185"  # Replace with your broker IP
MQTT_BROKER_PORT = 1883
COMMAND_TOPIC = "pi/video/command"
STATUS_TOPIC = "pi/video/status"
STATUS_CHECK_TOPIC = "pi/video/status_check"
STATUS_CHECK_RESPONSE_TOPIC = "pi/video/status_check_response"

state = {"processes": [], "id": None}
lock = threading.Lock()
monitor_running = True
client = mqtt.Client()


def publish_status(message):
    try:
        client.publish(STATUS_TOPIC, message)
        print(f"Published status: {message}")
    except Exception as exc:
        print(f"Publish failed: {exc}")


def stop_playback():
    with lock:
        for process in state["processes"]:
            try:
                process.kill()
            except Exception as exc:
                print(f"Failed to stop process: {exc}")
        state["processes"] = []
        state["id"] = None


def play_command(parts):
    if len(parts) < 3:
        return None
    playback_id = parts[1].strip()
    video_path = parts[2].strip()
    audio_path = parts[3].strip() if len(parts) > 3 else None
    if not playback_id or not video_path:
        return None
    return playback_id, video_path, audio_path


def start_playback(playback_id, video_path, audio_path):
    if not os.path.exists(video_path):
        print(f"Video file not found: {video_path}")
        return
    if audio_path and not os.path.exists(audio_path):
        print(f"Audio file not found: {audio_path}")
        return

    stop_playback()
    video_process = subprocess.Popen(["omxplayer", "-b", "-o", "hdmi", video_path])
    with lock:
        state["processes"] = [video_process]
        state["id"] = playback_id
    if audio_path:
        audio_process = subprocess.Popen(["omxplayer", "-o", "hdmi", audio_path])
        with lock:
            state["processes"].append(audio_process)
        print(f"Playing video {video_path} with audio {audio_path}")
    else:
        print(f"Playing video {video_path}")


def playback_monitor():
    global monitor_running
    while monitor_running:
        with lock:
            processes = list(state["processes"])
            playback_id = state["id"]
        if processes and all(process.poll() is not None for process in processes):
            if playback_id:
                publish_status(playback_id)
            with lock:
                state["processes"] = []
                state["id"] = None
        time.sleep(0.2)


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(COMMAND_TOPIC)
    client.subscribe(STATUS_CHECK_TOPIC)


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    print(f"Received message on {msg.topic}: {payload}")

    if msg.topic == STATUS_CHECK_TOPIC:
        with lock:
            processes = list(state["processes"])
        is_running = any(process.poll() is None for process in processes)
        response = "1" if is_running else "0"
        client.publish(STATUS_CHECK_RESPONSE_TOPIC, response)
        print(f"Published status check response: {response}")
        return

    parts = payload.split(";")
    if not parts:
        return
    command = parts[0].strip().lower()
    if command == "stop":
        stop_playback()
        print("Playback stopped.")
        return
    if command != "play":
        print(f"Unknown command: {command}")
        return
    parsed = play_command(parts)
    if not parsed:
        print("Invalid play command: expected 'play;id;video_path' or 'play;id;video_path;audio_path'")
        return
    playback_id, video_path, audio_path = parsed
    start_playback(playback_id, video_path, audio_path)


def main():
    global monitor_running

    monitor_thread = threading.Thread(target=playback_monitor, daemon=True)
    monitor_thread.start()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_BROKER_PORT, 60)
        client.loop_start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        monitor_running = False
        stop_playback()
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()
