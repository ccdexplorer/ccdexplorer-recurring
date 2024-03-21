from rich.console import Console
from .utils import Utils, RecurringType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
import asyncio
from ccdexplorer_fundamentals.enums import NET

console = Console()


class TokensByAddress(Utils):

    async def pre_main_tokens_by_address_canonical(self):
        while True:
            self.mainnet: dict[Collections, Collection]
            recurring_type = RecurringType.tokens_by_address

            for net in NET:
                s = self.start_time_and_log(recurring_type, net)

                db: dict[Collections, Collection] = (
                    self.mainnet if net.value == "mainnet" else self.testnet
                )
                motordb: dict[Collections, Collection] = (
                    self.motor_mainnet if net.value == "mainnet" else self.motor_testnet
                )
                try:
                    result = (
                        await motordb[Collections.tokens_links_v2]
                        .aggregate(
                            [
                                {
                                    "$group": {
                                        "_id": "$account_address_canonical",
                                        "count": {"$sum": 1},
                                    }
                                },
                                {"$sort": {"count": -1}},
                            ]
                        )
                        .to_list(1_000_000_000)
                    )
                    tokens_by_address = {x["_id"]: x["count"] for x in result}

                    queue = []
                    for account_address_canonical, count in tokens_by_address.items():
                        repl_dict = {"_id": account_address_canonical, "count": count}
                        if "id" in repl_dict:
                            del repl_dict["id"]
                        queue_item = ReplaceOne(
                            {"_id": account_address_canonical},
                            replacement=repl_dict,
                            upsert=True,
                        )
                        queue.append(queue_item)
                    _ = db[Collections.pre_tokens_by_address].bulk_write(queue)
                    self.end_and_log(recurring_type, s, net)
                except Exception as e:
                    self.tooter: Tooter
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Recurring: Failed to get pre_main_tokens_by_address_canonical. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
            await asyncio.sleep(60)
