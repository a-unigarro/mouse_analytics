from fastapi import FastAPI, WebSocket
from websocket_server import mouse_socket


app = FastAPI()


@app.get("/")
def home():
    return {
        "message": "Mouse tracking server running"
    }


@app.websocket("/mouse") #### chang the protocol to ws://localhost:8000/mouse
async def mouse_endpoint(
    websocket: WebSocket
):

    await mouse_socket.connect(websocket)

    try:
        await mouse_socket.receive_mouse_position(
            websocket
        )

    except Exception:
        mouse_socket.disconnect(websocket)