from fastapi import WebSocket


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
            data = await websocket.receive_json()

            print(
                f"x={data['x']} y={data['y']}"
            )


mouse_socket = MouseWebSocket()