from fastapi import WebSocket
from schemas import UserEvent

from kafka_producer import send_event

class MouseWebSocket:
    def __init__(self):
        self.connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.connections.remove(websocket)

    async def receive_mouse_position(self, websocket: WebSocket):
        while True:
            data = await websocket.receive_json() ### if te connection fails, this will throw an exception and exit the loop
            event = UserEvent(**data)
            send_event(event)
            #print(
            #    f"{event.event_type}: ({event.x}, {event.y}), {event.timestamp}"
            #)


mouse_socket = MouseWebSocket()