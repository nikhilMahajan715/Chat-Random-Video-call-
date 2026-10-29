
import asyncio
import json
import uuid

from channels.generic.websocket import AsyncWebsocketConsumer


# Development-only matchmaking queue.
# For production with multiple workers, use Redis-backed matchmaking.
waiting_queue = []
queue_lock = asyncio.Lock()


class ChatConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.room_group_name = None
        self.peer_channel_name = None

        await self.accept()

        await self.send(text_data=json.dumps({
            "type": "connected",
            "message": "WebSocket connected successfully."
        }))

    async def disconnect(self, close_code):
        global waiting_queue

        # Remove disconnected user from the waiting queue.
        async with queue_lock:
            waiting_queue = [
                channel for channel in waiting_queue
                if channel != self.channel_name
            ]

        # Notify the partner and leave the room.
        if self.room_group_name:
            room = self.room_group_name
            self.room_group_name = None

            await self.channel_layer.group_send(
                room,
                {
                    "type": "peer_disconnected",
                    "sender": self.channel_name,
                }
            )

            await self.channel_layer.group_discard(
                room,
                self.channel_name
            )

    async def receive(self, text_data=None, bytes_data=None):
        # Handle incoming WebSocket messages safely.
        try:
            data = json.loads(text_data or "{}")
        except (json.JSONDecodeError, TypeError):
            await self.send(text_data=json.dumps({
                "type": "error",
                "message": "Invalid JSON message."
            }))
            return

        action = data.get("action")

        if action == "find_partner":
            await self.handle_find_partner()

        elif action == "leave_room":
            await self.handle_leave_room()

        elif action in ("signal", "chat_message"):
            if self.room_group_name:
                await self.channel_layer.group_send(
                    self.room_group_name,
                    {
                        "type": "forward_message",
                        "sender": self.channel_name,
                        "payload": data,
                    }
                )
            else:
                await self.send(text_data=json.dumps({
                    "type": "error",
                    "message": "You are not connected to a partner yet."
                }))

    async def handle_find_partner(self):
        global waiting_queue

        # Leave the existing room before finding another partner.
        if self.room_group_name:
            await self.handle_leave_room()

        partner_channel = None

        # Find a waiting connection without adding duplicates.
        async with queue_lock:
            waiting_queue = [
                channel for channel in waiting_queue
                if channel != self.channel_name
            ]

            while waiting_queue:
                candidate = waiting_queue.pop(0)

                if candidate != self.channel_name:
                    partner_channel = candidate
                    break

            if partner_channel is None:
                waiting_queue.append(self.channel_name)

        # No partner is available yet.
        if partner_channel is None:
            await self.send(text_data=json.dumps({
                "type": "searching",
                "message": "Searching for a partner..."
            }))
            return

        # Create a unique room for both users.
        room_id = f"room_{uuid.uuid4().hex}"

        await self.channel_layer.group_add(
            room_id,
            partner_channel
        )
        await self.channel_layer.group_add(
            room_id,
            self.channel_name
        )

        self.room_group_name = room_id

        # The waiting user will initiate the WebRTC offer.
        await self.channel_layer.send(
            partner_channel,
            {
                "type": "match_found",
                "room": room_id,
                "is_initiator": True,
            }
        )

        # Notify the user who just found a partner.
        await self.send(text_data=json.dumps({
            "type": "match_found",
            "room": room_id,
            "is_initiator": False,
        }))

    async def handle_leave_room(self):
        global waiting_queue

        # Ensure the user is not still in the waiting queue.
        async with queue_lock:
            waiting_queue = [
                channel for channel in waiting_queue
                if channel != self.channel_name
            ]

        if not self.room_group_name:
            return

        room = self.room_group_name
        self.room_group_name = None

        # Tell the other user that the partner has left.
        await self.channel_layer.group_send(
            room,
            {
                "type": "peer_disconnected",
                "sender": self.channel_name,
            }
        )

        # Remove this user from the room.
        await self.channel_layer.group_discard(
            room,
            self.channel_name
        )

    async def match_found(self, event):
        # Handler for channel-layer messages of type match_found.
        self.room_group_name = event["room"]

        await self.send(text_data=json.dumps({
            "type": "match_found",
            "room": event["room"],
            "is_initiator": event["is_initiator"],
        }))

    async def forward_message(self, event):
        # Handler for WebRTC signaling and chat messages.
        # Do not send the message back to its sender.
        if event["sender"] != self.channel_name:
            await self.send(
                text_data=json.dumps(event["payload"])
            )

    async def peer_disconnected(self, event):
        # Handler when the other user leaves the room.
        if event["sender"] != self.channel_name:
            room = self.room_group_name
            self.room_group_name = None

            if room:
                await self.channel_layer.group_discard(
                    room,
                    self.channel_name
                )

            await self.send(text_data=json.dumps({
                "type": "peer_disconnected",
                "message": "Partner left the chat."
            }))
