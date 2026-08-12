from flask import Flask, request, jsonify
from flask_cors import CORS
import paho.mqtt.client as mqtt
import os
import threading
import time
import uuid


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)
CORS(app)


# ============================================================
# MQTT CONFIGURATION
# ============================================================

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

SIGNAL_TOPIC = "resqroute/traffic_signal"
STATUS_TOPIC = "resqroute/traffic_status"


# ============================================================
# ADMIN REQUEST CONFIGURATION
# ============================================================

REQUEST_TIMEOUT = 10

pending_request = None

request_lock = threading.Lock()


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2
)

# Automatically retry connection
mqtt_client.reconnect_delay_set(
    min_delay=2,
    max_delay=30
)


# ============================================================
# MQTT CONNECT CALLBACK
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    print()
    print("==========================================")
    print("CONNECTED TO EMQX MQTT BROKER")
    print("==========================================")
    print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Signal topic: {SIGNAL_TOPIC}")
    print(f"Status topic: {STATUS_TOPIC}")
    print()

    # Subscribe every time we reconnect
    result, mid = client.subscribe(
        STATUS_TOPIC,
        qos=1
    )

    if result == mqtt.MQTT_ERR_SUCCESS:
        print(f"Subscribed to: {STATUS_TOPIC}")
    else:
        print(f"⚠️ Subscription failed: {result}")

    print()


# ============================================================
# MQTT DISCONNECT CALLBACK
# ============================================================

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):

    print()
    print("==========================================")
    print("⚠️ MQTT DISCONNECTED")
    print("==========================================")
    print(f"Reason: {reason_code}")
    print("Paho will automatically reconnect...")
    print()


# ============================================================
# MQTT MESSAGE CALLBACK
# ============================================================

def on_message(client, userdata, msg):

    try:
        payload = msg.payload.decode("utf-8")

    except Exception:
        payload = str(msg.payload)

    print()
    print("==========================================")
    print("MQTT MESSAGE RECEIVED")
    print("==========================================")
    print(f"Topic:   {msg.topic}")
    print(f"Payload: {payload}")
    print()


# Register callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


# ============================================================
# MQTT BACKGROUND THREAD
# ============================================================

def mqtt_loop():

    print()
    print("==========================================")
    print("STARTING MQTT CONNECTION")
    print("==========================================")
    print()

    while True:

        try:

            print(
                f"Connecting to MQTT broker "
                f"{MQTT_BROKER}:{MQTT_PORT}..."
            )

            mqtt_client.connect(
                MQTT_BROKER,
                MQTT_PORT,
                60
            )

            print("MQTT socket connected.")
            print("Starting persistent MQTT loop...")

            # This stays running and handles:
            # - incoming messages
            # - outgoing messages
            # - automatic reconnects
            mqtt_client.loop_forever(
                retry_first_connection=True
            )

        except Exception as error:

            print()
            print("==========================================")
            print("⚠️ MQTT CONNECTION ERROR")
            print("==========================================")
            print(error)
            print("Retrying in 5 seconds...")
            print()

            time.sleep(5)


# Start MQTT thread
mqtt_thread = threading.Thread(
    target=mqtt_loop,
    daemon=True
)

mqtt_thread.start()


# ============================================================
# SEND MQTT STATUS
# ============================================================

def send_status(message):

    if not mqtt_client.is_connected():

        print(
            f"⚠️ MQTT disconnected - "
            f"status NOT sent: {message}",
            flush=True
        )

        return False

    try:

        result = mqtt_client.publish(
            STATUS_TOPIC,
            message,
            qos=1
        )

        print(
            f"📡 Status publish result: {result.rc}",
            flush=True
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            print(
                "⚠️ Failed to publish status",
                flush=True
            )

            return False

        result.wait_for_publish(
            timeout=5
        )

        print(
            f"🌐 Status sent: {message}",
            flush=True
        )

        return True

    except Exception as error:

        print(
            f"❌ Status MQTT error: {error}",
            flush=True
        )

        return False


# ============================================================
# ACTIVATE ROUTE
# ============================================================

def activate_route(route, source):

    route = str(route).upper()

    if route not in ["ROUTE1", "ROUTE2"]:

        print(
            f"❌ Invalid route: {route}",
            flush=True
        )

        return False

    print()
    print("==========================================")
    print(f"🚦 ACTIVATING {route}")
    print(f"Source: {source}")
    print("==========================================")
    print(
        f"MQTT connected: "
        f"{mqtt_client.is_connected()}",
        flush=True
    )

    # --------------------------------------------------------
    # Check MQTT
    # --------------------------------------------------------

    if not mqtt_client.is_connected():

        print(
            "❌ MQTT broker is NOT connected",
            flush=True
        )

        return False

    # --------------------------------------------------------
    # Publish route
    # --------------------------------------------------------

    try:

        print(
            f"📡 Publishing {route}...",
            flush=True
        )

        result = mqtt_client.publish(
            SIGNAL_TOPIC,
            route,
            qos=1
        )

        print(
            f"📡 MQTT publish result: {result.rc}",
            flush=True
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            print(
                f"❌ MQTT publish failed: {result.rc}",
                flush=True
            )

            return False

        # Wait for Paho to actually send it
        result.wait_for_publish(
            timeout=5
        )

        if not result.is_published():

            print(
                "❌ MQTT message was not confirmed "
                "as published",
                flush=True
            )

            return False

        print(
            f"✅ MQTT message successfully sent: "
            f"{route}",
            flush=True
        )

        print(
            f"📡 Topic: {SIGNAL_TOPIC}",
            flush=True
        )

    except Exception as error:

        print(
            f"❌ MQTT PUBLISH ERROR: {error}",
            flush=True
        )

        return False

    # --------------------------------------------------------
    # Print activation
    # --------------------------------------------------------

    print()
    print("===================================")
    print(f"🚦 {route} ACTIVATED")
    print(f"Source: {source}")
    print("===================================")
    print()

    # --------------------------------------------------------
    # Notify website
    # --------------------------------------------------------

    send_status(
        f"ROUTE_ACTIVATED:{route}:SOURCE={source}"
    )

    return True


# ============================================================
# CREATE EMERGENCY REQUEST
# ============================================================

def create_request(route, source):
    global pending_request

    route = str(route).upper()

    if route not in ["ROUTE1", "ROUTE2"]:
        return None, "Invalid route"

    now = time.time()

    with request_lock:

        # ------------------------------------------------
        # CLEAR STALE REQUEST
        # ------------------------------------------------
        if pending_request is not None:

            age = now - pending_request["created_at"]

            # If older than timeout + 5 seconds,
            # consider it dead/stale.
            if age > REQUEST_TIMEOUT + 5:

                print(
                    f"🧹 Clearing stale request: "
                    f"{pending_request['id']}",
                    flush=True
                )

                pending_request = None

        # ------------------------------------------------
        # CHECK AGAIN
        # ------------------------------------------------
        if pending_request is not None:

            return None, (
                "Another emergency request is already pending"
            )

        # ------------------------------------------------
        # CREATE NEW REQUEST
        # ------------------------------------------------
        request_id = str(uuid.uuid4())[:8]

        pending_request = {
            "id": request_id,
            "route": route,
            "source": source,
            "created_at": now,
            "status": "PENDING"
        }

        request_copy = pending_request.copy()

    print()
    print("===================================")
    print("🚨 NEW EMERGENCY REQUEST")
    print(f"ID:     {request_id}")
    print(f"Route:  {route}")
    print(f"Source: {source}")
    print("===================================")
    print(flush=True)

    send_status(
        f"REQUEST_CREATED:{request_id}:{route}:{source}"
    )

    timeout_thread = threading.Thread(
        target=request_timeout_worker,
        args=(request_id,),
        daemon=True
    )

    timeout_thread.start()

    return request_copy, None
    # --------------------------------------------------------
    # Print request
    # --------------------------------------------------------

    print()
    print("===================================")
    print("🚨 NEW EMERGENCY REQUEST")
    print(f"ID:     {request_id}")
    print(f"Route:  {route}")
    print(f"Source: {source}")
    print("===================================")
    print()

    # --------------------------------------------------------
    # Notify admin website
    # --------------------------------------------------------

    send_status(
        f"REQUEST_CREATED:{request_id}:{route}:{source}"
    )

    # --------------------------------------------------------
    # Start timeout watcher
    # --------------------------------------------------------

    timeout_thread = threading.Thread(
        target=request_timeout_worker,
        args=(request_id,),
        daemon=True
    )

    timeout_thread.start()

    return request_copy, None


# ============================================================
# REQUEST TIMEOUT
# ============================================================

def request_timeout_worker(request_id):

    global pending_request

    time.sleep(
        REQUEST_TIMEOUT
    )

    with request_lock:

        if pending_request is None:
            return

        if pending_request["id"] != request_id:
            return

        if pending_request["status"] != "PENDING":
            return

        route = pending_request["route"]

        pending_request["status"] = "AUTO_APPROVED"

        pending_request["decision"] = "TIMEOUT"

        pending_request["decision_at"] = time.time()

    print()
    print("===================================")
    print("⏱️ ADMIN TIMEOUT")
    print(f"Route: {route}")
    print("No response received.")
    print("Automatically activating route.")
    print("===================================")
    print()

    success = activate_route(
        route,
        "AUTO_TIMEOUT"
    )

    if success:

        send_status(
            f"REQUEST_AUTO_APPROVED:"
            f"{request_id}:{route}"
        )

    else:

        send_status(
            f"REQUEST_ACTIVATION_FAILED:"
            f"{request_id}:{route}"
        )

    # Keep request visible briefly
    time.sleep(2)

    with request_lock:

        if (
            pending_request is not None
            and
            pending_request["id"] == request_id
        ):

            pending_request = None


# ============================================================
# HOME / HEALTH CHECK
# ============================================================

@app.route("/", methods=["GET"])
def home():

    return jsonify({

        "success": True,

        "service": "RESQROUTE Backend",

        "status": "online",

        "mqtt_connected":
            mqtt_client.is_connected()

    })


# ============================================================
# CREATE ROUTE REQUEST
# ============================================================

@app.route(
    "/api/route-request",
    methods=["POST"]
)
def route_request():

    data = request.get_json(
        silent=True
    )

    if data is None:

        return jsonify({

            "success": False,

            "error":
                "Request must contain JSON"

        }), 400

    route = data.get("route")

    source = data.get(
        "source",
        "MANUAL"
    )

    if route is None:

        return jsonify({

            "success": False,

            "error":
                "Missing route"

        }), 400

    route = str(route).upper()

    source = str(source)

    new_request, error = create_request(
        route,
        source
    )

    if error:

        return jsonify({

            "success": False,

            "error": error

        }), 409

    return jsonify({

        "success": True,

        "message":
            "Emergency request sent to traffic control",

        "request": new_request,

        "timeout":
            REQUEST_TIMEOUT

    })


# ============================================================
# GET CURRENT REQUEST
# ============================================================

@app.route(
    "/api/requests",
    methods=["GET"]
)
def get_requests():

    with request_lock:

        if pending_request is None:

            return jsonify({

                "success": True,

                "request": None

            })

        request_copy = pending_request.copy()

    elapsed = (
        time.time()
        -
        request_copy["created_at"]
    )

    remaining = max(
        0,
        REQUEST_TIMEOUT - elapsed
    )

    request_copy["remaining_seconds"] = round(
        remaining,
        1
    )

    return jsonify({

        "success": True,

        "request": request_copy

    })


# ============================================================
# ADMIN ACCEPT
# ============================================================

@app.route(
    "/api/requests/<request_id>/accept",
    methods=["POST"]
)
def accept_request(request_id):

    global pending_request

    with request_lock:

        if pending_request is None:

            return jsonify({

                "success": False,

                "error":
                    "No pending request"

            }), 404

        if pending_request["id"] != request_id:

            return jsonify({

                "success": False,

                "error":
                    "Request not found"

            }), 404

        if pending_request["status"] != "PENDING":

            return jsonify({

                "success": False,

                "error":
                    "Request has already been processed"

            }), 409

        route = pending_request["route"]

        source = pending_request["source"]

        # Don't clear the request yet
        pending_request["status"] = "APPROVING"

    # --------------------------------------------------------
    # Activate route
    # --------------------------------------------------------

    success = activate_route(
        route,
        "ADMIN_APPROVED"
    )

    if not success:

        with request_lock:

            if pending_request is not None:

                pending_request["status"] = "ACTIVATION_FAILED"

        return jsonify({

            "success": False,

            "error":
                "Could not activate route"

        }), 503

    # --------------------------------------------------------
    # Mark approved
    # --------------------------------------------------------

    with request_lock:

        if pending_request is not None:

            pending_request["status"] = "APPROVED"

            pending_request["decision"] = "ACCEPTED"

            pending_request["decision_at"] = time.time()

    send_status(
        f"REQUEST_APPROVED:{request_id}:{route}"
    )

    print()
    print("===================================")
    print("👮 ADMIN APPROVED REQUEST")
    print(f"Route: {route}")
    print(f"Original source: {source}")
    print("===================================")
    print()

    # --------------------------------------------------------
    # Clear later
    # --------------------------------------------------------

    threading.Thread(
        target=clear_request_later,
        args=(request_id,),
        daemon=True
    ).start()

    return jsonify({

        "success": True,

        "message":
            "Request approved",

        "route": route

    })


# ============================================================
# ADMIN DENY
# ============================================================

@app.route(
    "/api/requests/<request_id>/deny",
    methods=["POST"]
)
def deny_request(request_id):

    global pending_request

    with request_lock:

        if pending_request is None:

            return jsonify({

                "success": False,

                "error":
                    "No pending request"

            }), 404

        if pending_request["id"] != request_id:

            return jsonify({

                "success": False,

                "error":
                    "Request not found"

            }), 404

        if pending_request["status"] != "PENDING":

            return jsonify({

                "success": False,

                "error":
                    "Request has already been processed"

            }), 409

        route = pending_request["route"]

        source = pending_request["source"]

        pending_request["status"] = "DENIED"

        pending_request["decision"] = "DENIED"

        pending_request["decision_at"] = time.time()

    print()
    print("===================================")
    print("❌ ADMIN DENIED REQUEST")
    print(f"Route: {route}")
    print(f"Original source: {source}")
    print("===================================")
    print()

    send_status(
        f"REQUEST_DENIED:{request_id}:{route}"
    )

    threading.Thread(
        target=clear_request_later,
        args=(request_id,),
        daemon=True
    ).start()

    return jsonify({

        "success": True,

        "message":
            "Request denied",

        "route": route

    })


# ============================================================
# CLEAR COMPLETED REQUEST
# ============================================================

def clear_request_later(request_id):

    global pending_request

    time.sleep(3)

    with request_lock:

        if (
            pending_request is not None
            and
            pending_request["id"] == request_id
        ):

            pending_request = None


# ============================================================
# EXISTING TRAFFIC API
# ============================================================

@app.route(
    "/api/traffic",
    methods=["POST"]
)
def traffic_control():

    data = request.get_json(
        silent=True
    )

    if data is None:

        return jsonify({

            "success": False,

            "error":
                "Request must contain JSON"

        }), 400

    signal = data.get("signal")

    if signal is None:

        return jsonify({

            "success": False,

            "error":
                "Missing signal"

        }), 400

    signal = str(signal).upper()

    if signal not in ["ON", "OFF"]:

        return jsonify({

            "success": False,

            "error":
                "Signal must be ON or OFF"

        }), 400

    if not mqtt_client.is_connected():

        return jsonify({

            "success": False,

            "error":
                "MQTT broker is not connected"

        }), 503

    try:

        result = mqtt_client.publish(
            SIGNAL_TOPIC,
            signal,
            qos=1
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            return jsonify({

                "success": False,

                "error":
                    "Failed to publish MQTT message"

            }), 500

        result.wait_for_publish(
            timeout=5
        )

    except Exception as error:

        print(
            f"❌ Traffic MQTT error: {error}",
            flush=True
        )

        return jsonify({

            "success": False,

            "error":
                "MQTT publish failed"

        }), 500

    print()
    print("===================================")
    print(f"Traffic signal sent: {signal}")
    print(f"MQTT topic: {SIGNAL_TOPIC}")
    print("Source: Manual API")
    print("===================================")
    print()

    return jsonify({

        "success": True,

        "signal": signal,

        "message":
            f"Traffic light command "
            f"'{signal}' sent"

    })


# ============================================================
# AI CAMERA DETECTION
# ============================================================

@app.route(
    "/camera-detection",
    methods=["POST"]
)
def camera_detection():

    data = request.get_json(
        silent=True
    )

    if data is None:

        return jsonify({

            "success": False,

            "error":
                "Invalid JSON"

        }), 400

    detected = data.get(
        "detected",
        False
    )

    detected = bool(detected)

    # --------------------------------------------------------
    # AMBULANCE DETECTED
    # --------------------------------------------------------

    if detected:

        print()
        print("===================================")
        print("🚑 AMBULANCE DETECTED BY AI")
        print("===================================")
        print()

        new_request, error = create_request(
            "ROUTE1",
            "AI_CAMERA"
        )

        if error:

            return jsonify({

                "success": False,

                "error": error

            }), 409

        return jsonify({

            "success": True,

            "detected": True,

            "route": "ROUTE1",

            "source": "AI Camera",

            "request": new_request,

            "message":
                "Ambulance detected. "
                "Traffic control approval requested."

        })

    # --------------------------------------------------------
    # NO AMBULANCE
    # --------------------------------------------------------

    print(
        "ℹ️ Camera detection cleared"
    )

    return jsonify({

        "success": True,

        "detected": False,

        "message":
            "No ambulance detected"

    })


# ============================================================
# START FLASK SERVER
# ============================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    print()
    print("==========================================")
    print("🚑 RESQROUTE BACKEND")
    print("==========================================")
    print(f"Flask port:    {port}")
    print(f"MQTT broker:   {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Signal topic:  {SIGNAL_TOPIC}")
    print(f"Status topic:  {STATUS_TOPIC}")
    print(f"Admin timeout: {REQUEST_TIMEOUT} seconds")
    print("==========================================")
    print()

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
