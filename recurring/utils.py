from ccdefundamentals.mongodb import (
    Collections,
    CollectionsUtilities,
)
from pymongo.collection import Collection
from enum import Enum
import dateutil.parser
from pymongo import ReplaceOne
from datetime import timedelta
import datetime as dt
from ccdefundamentals.cis import (
    MongoTypeTokenAddress,
    TokenMetaData,
    FailedAttempt,
)
import aiohttp
from datetime import timezone
import io
import chardet
from rich.console import Console
from ccdefundamentals.enums import NET

console = Console()


class RecurringType(Enum):
    """
    PreRender are slow queries that are being processed on a regular schedule
    and displayed in a frontend.
    """

    tps_table = "tps_table"
    accounts_table = "accounts_table"
    dashboard_nodes = "dashboard_nodes"
    exchange_rates = "exchange_rates"
    exchange_rates_historical = "exchange_rates_historical"
    web23_metadata = "web23_metadata"
    read_metadata = "read_metadata"
    tokens_page = "tokens_page"
    addresses_by_contract = "addresses_by_contract"
    tokens_by_address = "tokens_by_address"
    update_memos = "update_memos"
    impacted_addresses_top = "impacted_addresses_top"


class Utils:
    def start_time_and_log(self, recurring_type: RecurringType, net: NET = None):
        s = dt.datetime.now().astimezone(dt.timezone.utc)
        if net:
            console.log(f"Start {recurring_type.value} for {net.value}...")
        else:
            console.log(f"Start {recurring_type.value}...")
        return s

    def end_and_log(
        self, recurring_type: RecurringType, s: dt.datetime, net: NET = None
    ):
        e = dt.datetime.now().astimezone(dt.timezone.utc)
        elapsed = (e - s).total_seconds()
        if net:
            console.log(
                f"Start {recurring_type.value} for {net.value}...done in {elapsed:,.3f}s"
            )
        else:
            console.log(f"Start {recurring_type.value}...done in {elapsed:,.3f}s")

    def log_last_heartbeat_memo_to_hashes_in_mongo(self, height: int):
        query = {"_id": "heartbeat_memos_last_processed_block"}
        self.db[Collections.helpers].replace_one(
            query,
            {
                "_id": "heartbeat_memos_last_processed_block",
                "height": height,
            },
            upsert=True,
        )

    def decode_memo(self, hex):
        # bs = bytes.fromhex(hex)
        # return bytes.decode(bs[1:], 'UTF-8')
        try:
            bs = io.BytesIO(bytes.fromhex(hex))
            value = bs.read()

            encoding_guess = chardet.detect(value)
            if encoding_guess["confidence"] < 0.1:
                encoding_guess = chardet.detect(value[2:])
                value = value[2:]

            if encoding_guess["encoding"] and encoding_guess["confidence"] > 0.5:
                try:
                    memo = bytes.decode(value, encoding_guess["encoding"])

                    # memo = bytes.decode(value, "UTF-8")
                    return False, memo[1:]
                except UnicodeDecodeError:
                    memo = bytes.decode(value[1:], "UTF-8")
                    return False, memo[1:]
            else:
                return True, "Decoding failure..."
        except:  # noqa: E722
            return True, "Decoding failure..."

    def get_exchanges(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        result = self.utilities[CollectionsUtilities.labeled_accounts].find(
            {"label_group": "exchanges"}
        )

        exchanges = {}
        for r in result:
            key = r["label"].lower().split(" ")[0]
            current_list: list[str] = exchanges.get(key, [])
            current_list.append(r["_id"][:29])
            exchanges[key] = current_list
        return exchanges

    def get_exchanges_as_list(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        result = self.utilities[CollectionsUtilities.labeled_accounts].find(
            {"label_group": "exchanges"}
        )

        exchanges = []
        for r in result:
            exchanges.append(r["_id"][:29])
        return exchanges

    def get_usecases(self):
        self.utilities: dict[CollectionsUtilities, Collection]
        self.mainnet: dict[Collections, Collection]
        self.testnet: dict[Collections, Collection]
        usecase_addresses = {}
        result = self.utilities[CollectionsUtilities.usecases].find({})
        for usecase in list(result):
            mainnet_usecase_addresses = self.mainnet[Collections.usecases].find(
                {"usecase_id": usecase["usecase_id"]}
            )
            for address in mainnet_usecase_addresses:
                if address["type"] == "account_address":
                    usecase_addresses[address["account_address"]] = usecase[
                        "display_name"
                    ]

        return usecase_addresses

    def write_queue_to_prerender_collection(
        self, queue: list[ReplaceOne], prerender: RecurringType
    ):
        self.mainnet: dict[Collections, Collection]

        if len(queue) > 0:
            _ = self.mainnet[Collections.pre_render].bulk_write(queue)

        result = self.mainnet[Collections.helpers].find_one({"_id": "prerender_runs"})
        # set rerun to False
        if not result:
            result = {}

        result[prerender.value] = dt.datetime.now().astimezone(dt.timezone.utc)
        _ = self.mainnet[Collections.helpers].bulk_write(
            [
                ReplaceOne(
                    {"_id": "prerender_runs"},
                    replacement=result,
                    upsert=True,
                )
            ]
        )

    def get_all_dates(self) -> list[str]:
        return [x["date"] for x in self.mainnet[Collections.blocks_per_day].find({})]

    def get_start_end_block_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        return result["height_for_first_block"], result["height_for_last_block"]

    def get_hash_from_date(self, date: str) -> str:
        self.mainnet: dict[Collections, Collection]
        result = self.mainnet[Collections.blocks_per_day].find_one({"date": date})
        return result["hash_for_last_block"]

    def generate_dates_from_start_until_date(self, date: str):
        start_date = dateutil.parser.parse("2021-06-09")
        end_date = dateutil.parser.parse(date)
        date_range = []

        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        return date_range

    async def read_and_store_metadata(
        self,
        token_address_as_class: MongoTypeTokenAddress,
        db: dict[Collections, Collection],
    ):
        """
        This method takes as input a typed Token address and, depending on previous attempts,
        tries to read the contents of the metadata Url. Failed attempts are stored in the collection
        itself, where the time to next try increases quadratically.
        """
        timeout = 1  # sec

        url = token_address_as_class.metadata_url
        error = None

        try:
            do_request = url is not None
            if token_address_as_class.failed_attempt:
                timeout = 10

                if dt.datetime.now().astimezone(
                    tz=timezone.utc
                ) < token_address_as_class.failed_attempt.do_not_try_before.astimezone(
                    timezone.utc
                ):
                    do_request = False

            if do_request:
                if url[:4] == "ipfs":
                    url = f"https://ipfs.io/ipfs/{url[7:]}"

                session: aiohttp.ClientSession = (
                    self.quick_session if timeout == 1 else self.slow_session
                )

                async with session.get(url) as resp:
                    t = await resp.json()

                    metadata = None
                    if resp.status == 200:
                        try:
                            metadata = TokenMetaData(**t)
                            token_address_as_class.token_metadata = metadata
                            token_address_as_class.failed_attempt = None
                            self.save_token_address(token_address_as_class, db)
                        except Exception as e:
                            error = str(e)
                    else:
                        error = f"Failed with status code: {resp.status}"

        except Exception as e:
            error = str(e)

        if error:
            failed_attempt = token_address_as_class.failed_attempt
            if not failed_attempt:
                failed_attempt = FailedAttempt(
                    attempts=1,
                    do_not_try_before=dt.datetime.now().astimezone(tz=timezone.utc)
                    + dt.timedelta(hours=2),
                    last_error=error,
                )
            else:
                failed_attempt.attempts += 1
                failed_attempt.do_not_try_before = dt.datetime.now().astimezone(
                    tz=timezone.utc
                ) + dt.timedelta(
                    hours=failed_attempt.attempts * failed_attempt.attempts
                )
                failed_attempt.last_error = error

            token_address_as_class.failed_attempt = failed_attempt
            self.save_token_address(token_address_as_class, db)
