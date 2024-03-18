from rich.console import Console
from .utils import Utils, RecurringType
from pymongo import ReplaceOne
from ccdefundamentals.mongodb import Collections
from pymongo.collection import Collection
from ccdefundamentals.tooter import Tooter, TooterChannel, TooterType
import asyncio
from ccdefundamentals.enums import NET

console = Console()


class TokensPage(Utils):

    async def get_tokens_page(self):
        while True:
            self.mainnet: dict[Collections, Collection]
            recurring_type = RecurringType.tokens_page

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
                        await motordb[Collections.tokens_logged_events]
                        .aggregate(
                            [
                                {"$group": {"_id": "$contract", "count": {"$sum": 1}}},
                                {"$sort": {"count": -1}},
                            ]
                        )
                        .to_list(1_000_000_000)
                    )
                    logged_events_by_contract = {x["_id"]: x["count"] for x in result}

                    queue = []
                    for ctr_id, count in logged_events_by_contract.items():
                        repl_dict = {"_id": ctr_id, "count": count}
                        if "id" in repl_dict:
                            del repl_dict["id"]
                        queue_item = ReplaceOne(
                            {"_id": ctr_id},
                            replacement=repl_dict,
                            upsert=True,
                        )
                        queue.append(queue_item)
                    _ = db[Collections.pre_tokens_overview].bulk_write(queue)

                    self.end_and_log(recurring_type, s, net)

                except Exception as e:
                    self.tooter: Tooter
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Recurring: Failed to get tokens page. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
            await asyncio.sleep(60)