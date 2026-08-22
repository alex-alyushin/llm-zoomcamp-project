import os
import json
import time
import httpx
import asyncio
import logging

from pathlib import Path
from urllib.parse import urlencode
from pydantic import ValidationError
from psycopg import AsyncCursor

from store.messages_store import MessagesStore
from store.message_entity import MessageEntity
from store.user_entity import UserEntity

from search_service.schema_brightdata_linkedin import LinkedInJobsInput

from embedding.embedder import Embedder

from utils.log import configure_logging
from utils.utils import truncate


class SearchService:

    def __init__(self, brigth_data_token, messages_store: MessagesStore):
        self.logger = logging.getLogger("search_service")
        self.brigth_data_token = brigth_data_token
        self.messages_store = messages_store
        self.embedder = Embedder()


    async def run(self):
        await self.messages_store.listen(
            gateway="telegram",
            direction="internal",
            listener=self.search
        )


    ##################
    # STORE LISTENER #
    ##################

    async def search(self, cursor: AsyncCursor, message: MessageEntity):

        user: UserEntity = await self.messages_store.ensure_user(cursor, message=message)

        if user is None:
            self.logger.error("[ERROR] User not found")
            return

        request = self._validate_linkedin_jobs_input(message.text_content)

        if request is None:
            return await self._NOTIFY_USER(
                text="🚧 <b>Invalid request</b>",
                user=user,
            )

        await self._NOTIFY_USER(
            text=f"🔎 <b>Searching...</b>\n\n{self._format_search_params(request)}",
            user=user,
        )

        response = await self._brightdata_discover_linkedin_jobs(
            request=request,
            user=user,
        )

        if response is None:
            return await self._NOTIFY_USER(
                text="🪫 <b>No results</b>",
                user=user,
            )

        # debug
        self._dump_to_file(
            data=response.text,
            user=user,
        )

        records: list[str] = response.text.splitlines()

        documents: list[dict] = []

        # parse documents

        for record in records:
            document = self._parse_document(record=record)
            if document is not None:
                documents.append(document)

        self.logger.info("Found %s records", len(records))
        self.logger.info("Parsed %s documents", len(documents))

        if not documents:
            return

        # store documents

        embeddings = self.embedder.encode_batch(
            texts=[
                self._extract_relevant_fields(
                    document=document,
                    relevant_fields=[
                        "job_title",
                        "job_summary",
                        "job_description_formatted"
                    ]
                ) for document in documents
            ]
        )

        for document, embedding in zip(documents, embeddings):
            await self.messages_store.store_document(
                cursor,
                document=document,
                embedding=embedding,
                message=message,
                user=user,
            )

        # notify report

        await self.messages_store.store(
            role="searcher",
            gateway=message.gateway,
            direction="report",
            # dirty hack: for report
            text_content=message.id,
            external_chat_id=message.external_chat_id,
        )


    def _validate_linkedin_jobs_input(self, text_content: str):
        try:
            return LinkedInJobsInput.model_validate_json(text_content)

        except ValidationError as validation_error:
            self.logger.error(validation_error)
            return None


    async def _brightdata_discover_linkedin_jobs(
        self,
        request: LinkedInJobsInput,
        user: UserEntity,
    ):
        """
        Use the Bright Data Web Scraper API to discover LinkedIn Jobs by Keyword.
        Calls the POST /datasets/v3/scrape endpoint.
        Documentation: https://docs.brightdata.com/api-reference/scrapers/social-media-apis/linkedin-jobs-discover-by-keyword
        """

        try:
            DISCOVER_URL = "https://api.brightdata.com/datasets/v3/scrape"

            DISCOVER_LINKEDIN_PARAMS = {
                "dataset_id": "gd_lpfll7v5hcqtkxl6l",
                "notify": "false",
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "keyword",
            }

            discover_url = f"{DISCOVER_URL}?{urlencode(DISCOVER_LINKEDIN_PARAMS)}"

            payload = {
                "input": [request.model_dump(exclude_none=True)],
                "limit_per_input": 12,
            }

            headers = {
                "Authorization": f"Bearer {self.brigth_data_token}",
                "Content-Type": "application/json",
            }

            self.logger.info(
                "POST %s Payload: %s",
                truncate(discover_url, max_length=30, flat=True),
                payload
            )

            async with httpx.AsyncClient() as async_client:
                response = await async_client.post(
                    discover_url,
                    json=payload,
                    headers=headers,
                    timeout=180,
                )

                response.raise_for_status()

                if response.status_code == httpx.codes.ACCEPTED:
                    result = response.json()
                    snapshot_id = result["snapshot_id"]                    
                    await self._brightdata_monitor_progress(snapshot_id=snapshot_id, user=user)
                    return await self._brightdata_download_snapshot(snapshot_id=snapshot_id, user=user)

        except httpx.TimeoutException as timeout_error:
            self.logger.error(timeout_error)
            return None

        except httpx.ConnectError as connection_error:
            self.logger.error(connection_error)
            return None

        except httpx.HTTPError as http_error:
            self.logger.error(http_error)
            return None

        self.logger.info(
            "%s %-30s %s %s",
            response.request.method,
            response.url.path[:30],
            response.status_code,
            response.reason_phrase
        )

        return response


    async def _brightdata_monitor_progress(
        self,
        snapshot_id: str,
        user: UserEntity,
    ):
        """
        Use Bright Data Web Scraper API management endpoints to monitor Progress.
        GET /datasets/v3/progress/ returns snapshot or job status as JSON.
        Documentation: https://docs.brightdata.com/api-reference/scrapers/management-apis/monitor-progress
        """

        MONITOR_PROGRESS_URL = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"

        headers = {
            "Authorization": f"Bearer {self.brigth_data_token}",
        }

        async with httpx.AsyncClient() as async_client:

            while True:

                polling_response = await async_client.get(
                    MONITOR_PROGRESS_URL,
                    headers=headers,
                )

                polling_response.raise_for_status()

                result = polling_response.json()
                snapshot_status = result["status"]

                await self._NOTIFY_USER(
                    text=f"📡 Progress status: {snapshot_status}",
                    user=user,
                )

                self.logger.info(
                    "[Monitor progress] snapshot: %s, status: %s",
                    snapshot_id,
                    snapshot_status,
                )

                if snapshot_status == "ready":
                    break

                if snapshot_status == "failed":
                    break

                await asyncio.sleep(30)


    async def _brightdata_download_snapshot(
        self,
        snapshot_id: str,
        user: UserEntity,
    ):
        """
        Download results from a completed Bright Data Web Scraper API async job by snapshot ID.
        Pull-based delivery with a 5 GB per-request size limit.
        Documentation: https://docs.brightdata.com/api-reference/scrapers/delivery-apis/download-snapshot
        """

        DOWNLOAD_SNAPSHOT_URL = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"

        headers = {"Authorization": f"Bearer {self.brigth_data_token}"}
        params = {"format": "jsonl"}

        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(
                DOWNLOAD_SNAPSHOT_URL,
                headers=headers,
                params=params,
            )

            response.raise_for_status()

        self.logger.info(
            "%s %-30s %s %s",
            response.request.method,
            response.url.path[:30],
            response.status_code,
            response.reason_phrase
        )

        return response


    def _parse_document(self, record: str) -> dict:
        try:
            document = json.loads(record)

            if document.get("url") is None:
                return None

            return document

        except json.JSONDecodeError:
            return None


    def _extract_relevant_fields(
        self,
        document: dict,
        relevant_fields: list[str],
    ) -> str:
        values = []

        for field in relevant_fields:
            values.append(document.get(field) or "")

        return "\n\n".join(values)


    def _format_search_params(self, request: LinkedInJobsInput) -> str:
        lines = []

        for field, value in request.model_dump(exclude_none=True).items():
            if value in (None, "", []):
                continue

            lines.append(f"<b>{field}:</b> {json.dumps(value, ensure_ascii=False)}")

        return "\n".join(lines)


    # debug
    def _dump_to_file(self, data: str, user: UserEntity):
        timestamp = int(time.time() * 1000)
        debug_path = Path(f"tmp/brightdata_response_{user.id}_{timestamp}.json")
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(data, encoding="utf-8")

        self.logger.info(
            "Bright Data response saved to %s",
            debug_path,
        )


    async def _NOTIFY_USER(self, text: str, user: UserEntity):
        await self.messages_store.store(
            role="searcher",
            gateway=user.gateway,
            direction="outgoing",
            text_content=truncate(text, max_length=1024),
            external_chat_id=user.external_chat_id,
            external_user_id=user.external_user_id,
            external_user_name=user.external_user_name,
        )


async def main() -> None:

    from dotenv import load_dotenv
    load_dotenv()

    configure_logging(service_name="SearchService")

    brigth_data_token = os.getenv("BRIGHT_DATA_TOKEN")

    messages_store = await MessagesStore.create(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    search_service = SearchService(
        brigth_data_token=brigth_data_token,
        messages_store=messages_store
    )

    await search_service.run()


if __name__ == "__main__":
    asyncio.run(main())
