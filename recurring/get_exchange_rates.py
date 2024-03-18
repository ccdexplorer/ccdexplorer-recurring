from rich.console import Console
from pymongo import ReplaceOne
from .utils import Utils, RecurringType
from ccdefundamentals.mongodb import Collections
from ccdefundamentals.mongodb import MongoDB, CollectionsUtilities
import dateutil
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
import datetime as dt
import aiohttp
import asyncio
from env import COIN_API_KEY
from datetime import timezone
import sys

if sys.version_info >= (3, 11):
    from typing import Self

console = Console()


class ExchangeRates(Utils):
    async def coinapi(self, token, session: aiohttp.ClientSession):
        url = f"https://rest.coinapi.io/v1/exchangerate/{token}/USD/apikey-{COIN_API_KEY}/"
        async with session.get(url) as resp:
            if resp.ok:
                result = await resp.json()
                return_dict = {
                    "_id": f"USD/{token}",
                    "token": token,
                    "timestamp": dateutil.parser.parse(result["time"]),
                    "rate": result["rate"],
                    "source": "CoinAPI",
                }
            else:
                return_dict = None

        return return_dict

    async def coingecko(self, token):
        token_to_request = self.coingecko_token_translation.get(token)
        if token_to_request:

            url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_to_request}&vs_currencies=usd&include_last_updated_at=true"
            async with self.session.get(url) as resp:
                if resp.ok:
                    result = await resp.json()
                    result = result[token_to_request]
                    return_dict = {
                        "_id": f"USD/{token}",
                        "token": token,
                        "timestamp": dt.datetime.fromtimestamp(
                            result["last_updated_at"], tz=timezone.utc
                        ),
                        "rate": result["usd"],
                        "source": "CoinGecko",
                    }
                else:
                    return_dict = None
        else:
            return_dict = None
        return return_dict

    async def get_token_translations_from_mongo(self: Self):

        self.mongodb: MongoDB
        result = self.mongodb.utilities[
            CollectionsUtilities.token_api_translations
        ].find({"service": "coingecko"})
        self.coingecko_token_translation = {
            x["token"]: x["translation"] for x in list(result)
        }

    async def coingecko_historical(self, token: str):
        """
        This is the implementation of the CoinGecko historical API.
        """
        await self.get_token_translations_from_mongo()
        token_to_request = self.coingecko_token_translation.get(token)

        return_list_for_token = []
        # only for tokens that we know we can request
        if token_to_request:

            self.session: aiohttp.ClientSession
            url = f"https://api.coingecko.com/api/v3/coins/{token_to_request}/market_chart?vs_currency=usd&days=max&interval=daily&precision=full"
            async with self.session.get(url) as resp:
                if resp.ok:

                    result = await resp.json()
                    result = result["prices"]
                    console.log(
                        f"Historic: {token} | {token_to_request} | {resp.status} | {len(result)} days"
                    )
                    for timestamp, price in result:
                        formatted_date = f"{dt.datetime.fromtimestamp(timestamp/1000, tz=timezone.utc):%Y-%m-%d}"
                        return_dict = {
                            "_id": f"USD/{token}-{formatted_date}",
                            "token": token,
                            "timestamp": timestamp,
                            "date": formatted_date,
                            "rate": price,
                            "source": "CoinGecko",
                        }
                        return_list_for_token.append(
                            ReplaceOne(
                                {"_id": f"USD/{token}-{formatted_date}"},
                                return_dict,
                                upsert=True,
                            )
                        )
                    # sleep to prevent from being rate-limited.
                    await asyncio.sleep(61)
                else:
                    console.log(f"{token} | {token_to_request} | {resp.status}")
                    return_dict = None

        return return_list_for_token

    async def update_exchange_rates_for_tokens(self):
        while True:
            await self.get_token_translations_from_mongo()
            recurring_type = RecurringType.exchange_rates
            s = self.start_time_and_log(recurring_type)

            try:
                token_list = [
                    x["_id"].replace("w", "")
                    for x in self.mainnet[Collections.tokens_tags].find(
                        {"owner": "Arabella"}
                        # {"token_type": "fungible"}
                    )
                ]
                token_list.insert(0, "CCD")
                token_list.insert(0, "EUROe")
                queue = []

                for token in token_list:
                    # print(f"{token} start...")
                    result = await self.coinapi(token, self.coin_api_session)
                    if not result:
                        result = await self.coingecko(token)
                    if result:
                        # print(f"{token} save...")
                        queue.append(
                            ReplaceOne(
                                {"_id": f"USD/{token}"},
                                result,
                                upsert=True,
                            )
                        )

                if len(queue) > 0:
                    _ = self.utilities[CollectionsUtilities.exchange_rates].bulk_write(
                        queue
                    )

                    # update exchange rates retrieval
                    query = {"_id": "heartbeat_last_timestamp_exchange_rates"}
                    self.mainnet[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "heartbeat_last_timestamp_exchange_rates",
                            "timestamp": dt.datetime.now().astimezone(dt.timezone.utc),
                        },
                        upsert=True,
                    )
                    self.end_and_log(recurring_type, s)
            except Exception as e:
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Recurring: Failed to get exchange rates. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )
            await asyncio.sleep(60)

    async def update_exchange_rates_historical_for_tokens(self):
        await self.get_token_translations_from_mongo()
        recurring_type = RecurringType.exchange_rates_historical
        s = self.start_time_and_log(recurring_type)
        try:
            token_list = [
                x["_id"].replace("w", "")
                for x in self.mainnet[Collections.tokens_tags].find(
                    {"token_type": "fungible"}
                )
            ]

            queue = []
            for token in token_list:

                queue = await self.coingecko_historical(token)

                if len(queue) > 0:
                    _ = self.utilities[
                        CollectionsUtilities.exchange_rates_historical
                    ].bulk_write(queue)

                    # update exchange rates retrieval
                    query = {
                        "_id": "heartbeat_last_timestamp_exchange_rates_historical"
                    }
                    self.mainnet[Collections.helpers].replace_one(
                        query,
                        {
                            "_id": "heartbeat_last_timestamp_exchange_rates_historical",
                            "timestamp": dt.datetime.now().astimezone(dt.timezone.utc),
                        },
                        upsert=True,
                    )

            self.end_and_log(recurring_type, s)

        except Exception as e:
            self.tooter: Tooter
            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"Recurring: Failed to get exchange rates historical. Error: {e}",
                notifier_type=TooterType.REQUESTS_ERROR,
            )
