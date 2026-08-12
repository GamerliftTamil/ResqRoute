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
# REQUEST CONFIGURATION
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

mqtt_client.reconnect_delay_set(
    min_delay=2,
    max_delay=30
)


# ============================================================
# MQTT CONNECT
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    print()
    print("==========================================")
    print("✅ CONNECTED TO EMQX MQTT BROKER")
    print("==========================================")
    print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print()

    try:

        result, mid = client.subscribe(
            STATUS_TOPIC,
            qos=1
        )

        if result == mqtt.MQTT_ERR_SUCCESS:

            print(
                f"✅ Subscribed to: {STATUS_TOPIC}",
                flush=True
            )

        else:

            print(
                f"❌ Subscription failed: {result}",
                flush=True
            )

    except Exception as error:

        print(
            f"❌ Subscribe error: {error}",
            flush=True
        )

    print()


# ============================================================
# MQTT DISCONNECT
# ============================================================

def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):

    print()
    print("==========================================")
    print("⚠️ MQTT DISCONNECTED")
    print("==========================================")
    print(f"Reason: {reason_code}")
    print("Paho will reconnect...")
    print()


# ============================================================
# MQTT MESSAGE
# ============================================================

def on_message(client, userdata, msg):

    try:

        payload = msg.payload.decode("utf-8")

    except Exception:

        payload = str(msg.payload)

    print()
    print("==========================================")
    print("📨 MQTT MESSAGE RECEIVED")
    print("==========================================")
    print(f"Topic:   {msg.topic}")
    print(f"Payload: {payload}")
    print("==========================================")
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
    print("🚀 STARTING MQTT THREAD")
    print("==========================================")
    print()

    while True:

        try:

            print(
                f"🔌 Connecting to "
                f"{MQTT_BROKER}:{MQTT_PORT}...",
                flush=True
            )

            mqtt_client.connect(
                MQTT_BROKER,
                MQTT_PORT,
                60
            )

            print(
                "✅ MQTT socket connected",
                flush=True
            )

            print(
                "🔄 Starting persistent MQTT loop...",
                flush=True
            )

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
# SEND STATUS MESSAGE
# ============================================================

def send_status(message):

    if not mqtt_client.is_connected():

        print(
            f"⚠️ MQTT offline. "
            f"Status not sent: {message}",
            flush=True
        )

        return False

    try:

        result = mqtt_client.publish(
            STATUS_TOPIC,
            message,
            qos=1
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:

            print(
                f"❌ Status publish failed: {result.rc}",
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
    # MQTT CONNECTION CHECK
    # --------------------------------------------------------

    if not mqtt_client.is_connected():

        print(
            "❌ MQTT broker is NOT connected",
            flush=True
        )

        return False

    # --------------------------------------------------------
    # PUBLISH ROUTE
    # --------------------------------------------------------

    try:

        print(
            f"📡 Publishing {route} "
            f"to {SIGNAL_TOPIC}...",
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
                "❌ MQTT publish failed",
                flush=True
            )

            return False

        result.wait_for_publish(
            timeout=5
        )

        if not result.is_published():

            print(
                "❌ MQTT message was not confirmed",
                flush=True
            )

            return False

        print(
            f"✅ MQTT message successfully sent: {route}",
            flush=True
        )

    except Exception as error:

        print(
            f"❌ MQTT PUBLISH ERROR: {error}",
            flush=True
        )

        return False

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    print()
    print("==========================================")
    print(f"🚦 {route} ACTIVATED")
    print(f"Source: {source}")
    print("==========================================")
    print()

    # --------------------------------------------------------
    # NOTIFY STATUS
    # --------------------------------------------------------

    send_status(
        f"ROUTE_ACTIVATED:{route}:SOURCE={source}"
    )

    return True


# ============================================================
# CREATE REQUEST
# ============================================================

def create_request(route, source):

    global pending_request

    route = str(route).upper()
    source = str(source)

    if route not in ["ROUTE1", "ROUTE2"]:

        return None, "Invalid route"

    now = time.time()

    # --------------------------------------------------------
    # CREATE REQUEST SAFELY
    # --------------------------------------------------------

    with request_lock:

        # Remove stale request
        if pending_request is not None:

            age = (
                now -
                pending_request["created_at"]
            )

            if age > REQUEST_TIMEOUT + 5:

                print(
                    f"🧹 Removing stale request "
                    f"{pending_request['id']}",
                    flush=True
                )

                pending_request = None

        # Still pending?
        if pending_request is not None:

            return None, (
                "Another emergency request is already pending"
            )

        # Create ID
        request_id = str(
            uuid.uuid4()
        )[:8]

        pending_request = {

            "id": request_id,

            "route": route,

            "source": source,

            "created_at": now,

            "status": "PENDING"

        }

        request_copy = pending_request.copy()

    # --------------------------------------------------------
    # PRINT
    # --------------------------------------------------------

    print()
    print("==========================================")
    print("🚨 NEW EMERGENCY REQUEST")
    print("==========================================")
    print(f"ID:     {request_id}")
    print(f"Route:  {route}")
    print(f"Source: {source}")
    print("==========================================")
    print()

    # --------------------------------------------------------
    # NOTIFY ADMIN
    # --------------------------------------------------------

    send_status(
        f"REQUEST_CREATED:"
        f"{request_id}:"
        f"{route}:"
        f"{source}"
    )

    # --------------------------------------------------------
    # START TIMEOUT THREAD
    # --------------------------------------------------------

    threading.Thread(
        target=request_timeout_worker,
        args=(request_id,),
        daemon=True
    ).start()

    return request_copy, None


# ============================================================
# REQUEST TIMEOUT WORKER
# ============================================================

def request_timeout_worker(request_id):

    global pending_request

    # Wait for admin
    time.sleep(REQUEST_TIMEOUT)

    # --------------------------------------------------------
    # CHECK REQUEST
    # --------------------------------------------------------

    with request_lock:

        if pending_request is None:
            return

        if pending_request["id"] != request_id:
            return

        if pending_request["status"] != "PENDING":
            return

        route = pending_request["route"]

        pending_request["status"] = "AUTO_APPROVING"

    # --------------------------------------------------------
    # ACTIVATE WITHOUT LOCK
    # --------------------------------------------------------

    print()
    print("==========================================")
    print("⏱️ ADMIN TIMEOUT")
    print("==========================================")
    print(f"Request: {request_id}")
    print(f"Route:   {route}")
    print("No admin response.")
    print("Automatically activating route.")
    print("==========================================")
    print()

    success = activate_route(
        route,
        "AUTO_TIMEOUT"
    )

    # --------------------------------------------------------
    # UPDATE REQUEST
    # --------------------------------------------------------

    with request_lock:

        if (
            pending_request is not None
            and
            pending_request["id"] == request_id
        ):

            if success:

                pending_request["status"] = (
                    "AUTO_APPROVED"
                )

                pending_request["decision"] = (
                    "TIMEOUT"
                )

                pending_request["decision_at"] = (
                    time.time()
                )

            else:

                pending_request["status"] = (
                    "ACTIVATION_FAILED"
                )

                pending_request["decision"] = (
                    "TIMEOUT"
                )

                pending_request["decision_at"] = (
                    time.time()
                )

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if success:

        send_status(
            f"REQUEST_AUTO_APPROVED:"
            f"{request_id}:"
            f"{route}"
        )

    else:

        send_status(
            f"REQUEST_ACTIVATION_FAILED:"
            f"{request_id}:"
            f"{route}"
        )

    # --------------------------------------------------------
    # KEEP RESULT FOR ADMIN FOR 3 SEC
    # --------------------------------------------------------

    time.sleep(3)

    clear_request_later(request_id)


# ============================================================
# CLEAR REQUEST
# ============================================================

def clear_request_later(request_id):

    global pending_request

    with request_lock:

        if (
            pending_request is not None
            and
            pending_request["id"] == request_id
        ):

            print(
                f"🧹 Clearing request {request_id}",
                flush=True
            )

            pending_request = None


# ============================================================
# HOME / HEALTH
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
            "Emergency request sent "
            "to traffic control",

        "request":
            new_request,

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

        request_copy = (
            pending_request.copy()
        )

    # --------------------------------------------------------
    # Calculate remaining time
    # --------------------------------------------------------

    elapsed = (
        time.time()
        -
        request_copy["created_at"]
    )

    remaining = max(
        0,
        REQUEST_TIMEOUT - elapsed
    )

    request_copy[
        "remaining_seconds"
    ] = round(
        remaining,
        1
    )

    return jsonify({

        "success": True,

        "request":
            request_copy

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

    # --------------------------------------------------------
    # GET REQUEST
    # --------------------------------------------------------

    with request_lock:

        if pending_request is None:

            return jsonify({

                "success": False,

                "error":
                    "No pending request"

            }), 404

        if (
            pending_request["id"]
            != request_id
        ):

            return jsonify({

                "success": False,

                "error":
                    "Request not found"

            }), 404

        if (
            pending_request["status"]
            != "PENDING"
        ):

            return jsonify({

                "success": False,

                "error":
                    "Request has already been processed"

            }), 409

        route = pending_request["route"]

        source = pending_request["source"]

        # Reserve request
        pending_request["status"] = (
            "APPROVING"
        )

    # --------------------------------------------------------
    # ACTIVATE ROUTE
    # --------------------------------------------------------

    success = activate_route(
        route,
        "ADMIN_APPROVED"
    )

    # --------------------------------------------------------
    # MQTT FAILED
    # --------------------------------------------------------

    if not success:

        with request_lock:

            if (
                pending_request is not None
                and
                pending_request["id"]
                == request_id
            ):

                pending_request["status"] = (
                    "PENDING"
                )

        return jsonify({

            "success": False,

            "error":
                "Could not activate route"

        }), 503

    # --------------------------------------------------------
    # MARK APPROVED
    # --------------------------------------------------------

    with request_lock:

        if (
            pending_request is not None
            and
            pending_request["id"]
            == request_id
        ):

            pending_request["status"] = (
                "APPROVED"
            )

            pending_request["decision"] = (
                "ACCEPTED"
            )

            pending_request["decision_at"] = (
                time.time()
            )

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    send_status(
        f"REQUEST_APPROVED:"
        f"{request_id}:"
        f"{route}"
    )

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    print()
    print("==========================================")
    print("👮 ADMIN APPROVED REQUEST")
    print("==========================================")
    print(f"ID:     {request_id}")
    print(f"Route:  {route}")
    print(f"Source: {source}")
    print("==========================================")
    print()

    # --------------------------------------------------------
    # CLEAR AFTER 3 SEC
    # --------------------------------------------------------

    threading.Thread(
        target=clear_request_later_delayed,
        args=(request_id,),
        daemon=True
    ).start()

    return jsonify({

        "success": True,

        "message":
            "Request approved",

        "route":
            route

    })


# ============================================================
# DELAYED CLEAR
# ============================================================

def clear_request_later_delayed(request_id):

    time.sleep(3)

    clear_request_later(
        request_id
    )


# ============================================================
# ADMIN DENY
# ============================================================

@app.route(
    "/api/requests/<request_id>/deny",
    methods=["POST"]
)
def deny_request(request_id):

    global pending_request

    # --------------------------------------------------------
    # CHECK REQUEST
    # --------------------------------------------------------

    with request_lock:

        if pending_request is None:

            return jsonify({

                "success": False,

                "error":
                    "No pending request"

            }), 404

        if (
            pending_request["id"]
            != request_id
        ):

            return jsonify({

                "success": False,

                "error":
                    "Request not found"

            }), 404

        if (
            pending_request["status"]
            != "PENDING"
        ):

            return jsonify({

                "success": False,

                "error":
                    "Request has already been processed"

            }), 409

        route = pending_request["route"]

        source = pending_request["source"]

        pending_request["status"] = (
            "DENIED"
        )

        pending_request["decision"] = (
            "DENIED"
        )

        pending_request["decision_at"] = (
            time.time()
        )

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------

    print()
    print("==========================================")
    print("❌ ADMIN DENIED REQUEST")
    print("==========================================")
    print(f"ID:     {request_id}")
    print(f"Route:  {route}")
    print(f"Source: {source}")
    print("==========================================")
    print()

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    send_status(
        f"REQUEST_DENIED:"
        f"{request_id}:"
        f"{route}"
    )

    # --------------------------------------------------------
    # CLEAR AFTER 3 SEC
    # --------------------------------------------------------

    threading.Thread(
        target=clear_request_later_delayed,
        args=(request_id,),
        daemon=True
    ).start()

    return jsonify({

        "success": True,

        "message":
            "Request denied",

        "route":
            route

    })


# ============================================================
# MANUAL TRAFFIC API
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

    if signal not in [
        "ON",
        "OFF"
    ]:

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

        if (
            result.rc
            != mqtt.MQTT_ERR_SUCCESS
        ):

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
    print("==========================================")
    print(f"🚦 Traffic signal sent: {signal}")
    print(f"Topic: {SIGNAL_TOPIC}")
    print("==========================================")
    print()

    return jsonify({

        "success": True,

        "signal":
            signal,

        "message":
            f"Traffic light command "
            f"'{signal}' sent"

    })


# ============================================================
# CAMERA DETECTION
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

    detected = bool(
        data.get(
            "detected",
            False
        )
    )

    # --------------------------------------------------------
    # AMBULANCE DETECTED
    # --------------------------------------------------------

    if detected:

        print()
        print("==========================================")
        print("🚑 AMBULANCE DETECTED BY CAMERA")
        print("==========================================")
        print()

        new_request, error = create_request(
            "ROUTE1",
            "AI_CAMERA"
        )

        if error:

            return jsonify({

                "success": False,

                "error":
                    error

            }), 409

        return jsonify({

            "success": True,

            "detected": True,

            "route": "ROUTE1",

            "source": "AI Camera",

            "request":
                new_request,

            "message":
                "Ambulance detected. "
                "Traffic control approval requested."

        })

    # --------------------------------------------------------
    # NO AMBULANCE
    # --------------------------------------------------------

    print(
        "ℹ️ Camera detection cleared",
        flush=True
    )

    return jsonify({

        "success": True,

        "detected": False,

        "message":
            "No ambulance detected"

    })


# ============================================================
# START SERVER
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
    print(
        f"MQTT broker:   "
        f"{MQTT_BROKER}:{MQTT_PORT}"
    )
    print(
        f"Signal topic:  "
        f"{SIGNAL_TOPIC}"
    )
    print(
        f"Status topic:  "
        f"{STATUS_TOPIC}"
    )
    print(
        f"Admin timeout: "
        f"{REQUEST_TIMEOUT} seconds"
    )
    print("==========================================")
    print()

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
