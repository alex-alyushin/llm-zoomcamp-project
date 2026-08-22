import os
import json
import asyncio
import logging

from openai import OpenAI
from openai.types.responses import Response, ResponseInputParam

from psycopg import AsyncCursor

from store.messages_store import MessagesStore
from store.message_entity import MessageEntity

from embedding.embedder import Embedder

from utils.utils import is_json, truncate
from utils.log import configure_logging

from .system_prompt_v2 import system_prompt_v2

from .tools import TOOLS

ALLOWED_ROLES = [
    "assistant",
    "system",
    "developer",
    "user"
]


class LLMService:

    def __init__(self, openai_token: str, openai_model: str, messages_store: MessagesStore):
        self.logger = logging.getLogger("llm_service")
        self.logger.setLevel(logging.INFO)

        self.openai_client = OpenAI(api_key=openai_token)
        self.openai_model = openai_model

        self.messages_store = messages_store

        self.embedder = Embedder()


    async def run(self):
        await self.messages_store.listen(
            gateway="telegram",
            direction="incoming",
            listener=self.ask
        )


    ##################
    # STORE LISTENER #
    ##################

    async def ask(self, cursor: AsyncCursor, message: MessageEntity):

        # 1. Load LLM history

        messages = await self.messages_store.load_unresolved_messages(
            cursor,
            chat_id=message.external_chat_id
        )

        history: list[ResponseInputParam] = [{
            "role": "developer",
            "content": system_prompt_v2
        }]

        for item in messages:
            content = ""

            if item.role not in ALLOWED_ROLES:
                continue

            if item.text_content is not None:
                content = content + item.text_content + "\n\n"

            if item.file_content is not None:
                # @todo: remove dependency from job search
                content = content + f"My Resume here:\n\n{item.file_content}"

            history.append({
                "role": item.role,
                "content": content
            })

        # 2. Ask LLM 

        # debug
        self._log_llm_history(history)

        response = self.openai_client.responses.create(
            model=self.openai_model,
            input=history,
            tools=TOOLS,
            tool_choice="auto"
        )

        # 3. Tools calling

        tools_response = await self._tools_call(
            cursor,
            response=response,
            message=message
        )

        if tools_response:
            response = tools_response

        # 3. Store response

        response_direction = "outgoing"
        if is_json(response.output_text):
            response_direction = "internal"

        await self.messages_store.store(
            role="assistant",
            gateway=message.gateway,
            direction=response_direction,
            text_content=response.output_text,
            # need for table users
            external_chat_id=message.external_chat_id,
            external_user_id=message.external_user_id,
            external_user_name=message.external_user_name,
            attributes={
                "llm_model": self.openai_model,
                "llm_usage": response.usage.model_dump(),
            }
        )


    async def _tools_call(
        self, cursor, *,
        response: Response,
        message: MessageEntity
    ) -> Response | None:

        tools_history: list[ResponseInputParam] = []

        for item in response.output:

            if item.type != "function_call":
                continue

            if item.name == "store_user_cv":

                self.logger.info(f"[LLMTools] Name: {item.name} | Args: {item.arguments}")

                self.logger.info(
                    "[LLMTools] [MESSAGE] Text: %s | File: %s",
                    truncate(message.text_content, max_length=64, flat=True),
                    truncate(message.file_content, max_length=64, flat=True),
                )

                result = await self._store_user_cv_impl(cursor, message=message)

                tools_history.append({
                    "output": json.dumps(result),
                    "type": "function_call_output",
                    "call_id": item.call_id,
                })

                self.logger.info(f"[LLMTools] Result: {result}")

            else:
                self.logger.warning("Unknown tool call: %s", item.name)

        if len(tools_history):
            # debug
            self._log_llm_history(tools_history)

            return self.openai_client.responses.create(
                previous_response_id=response.id,
                model=self.openai_model,
                input=tools_history,
            )

        return None


    #######################
    # TOOL: Store User CV #
    #######################

    async def _store_user_cv_impl(self, cursor, *, message: MessageEntity) -> dict:

        user = await self.messages_store.ensure_user(cursor, message=message)

        if user is None:
            return {"status": "user_not_found"}

        content = f"{message.text_content or ""} {message.file_content or ""}"

        embedding = self.embedder.encode(text=content)

        await self.messages_store.store_user_cv(
            cursor,
            content=content,
            embedding=embedding,
            user=user,
        )

        return {"status": "ok"}


    # debug
    def _log_llm_history(self, history: list[ResponseInputParam]):

        lines = '\n'.join(
            [
                "%-2s %-12s %-128s" % (
                    index + 1,
                    item.get("role", "no_role")[:12],
                    truncate(item.get("content", ""), max_length=128, flat=True),
                ) for index, item in enumerate(history)
            ]
        )

        self.logger.info(
            "LLM History\n\nHistory (count = %d):\n\n%s\n",
            len(history), lines
        )


async def main() -> None:

    from dotenv import load_dotenv
    load_dotenv()

    configure_logging(service_name="LLMService")

    openai_token = os.getenv("OPENAI_TOKEN")
    openai_model = os.getenv("OPENAI_MODEL")

    messages_store = await MessagesStore.create(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    llm_service = LLMService(
        openai_token=openai_token,
        openai_model=openai_model,
        messages_store=messages_store
    )

    await llm_service.run()


if __name__ == "__main__":
    asyncio.run(main())
