from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from backend.websocket_server import mouse_socket
from backend.read_api import router as read_router
app = FastAPI()



# Dev-only, permissive CORS so the static frontend (opened as a file or
# served separately from the API) can call the read endpoints below.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
 
app.include_router(read_router, prefix="/api")


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