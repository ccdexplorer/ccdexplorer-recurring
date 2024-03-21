from rich.console import Console
from .utils import Utils, RecurringType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
import asyncio
from ccdexplorer_fundamentals.enums import NET

console = Console()


class AddressesByContract(Utils):

    async def pre_addresses_by_contract_count(self):
        while True:
            self.mainnet: dict[Collections, Collection]
            recurring_type = RecurringType.addresses_by_contract

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
                        await motordb[Collections.tokens_token_addresses_v2]
                        .aggregate(
                            [
                                {"$group": {"_id": "$contract", "count": {"$sum": 1}}},
                                {"$sort": {"count": -1}},
                            ]
                        )
                        .to_list(1_000_000_000)
                    )
                    addresses_by_contract = {x["_id"]: x["count"] for x in result}

                    queue = []
                    for ctr_id, count in addresses_by_contract.items():
                        repl_dict = {"_id": ctr_id, "count": count}
                        if "id" in repl_dict:
                            del repl_dict["id"]
                        queue_item = ReplaceOne(
                            {"_id": ctr_id},
                            replacement=repl_dict,
                            upsert=True,
                        )
                        queue.append(queue_item)
                    _ = db[Collections.pre_addresses_by_contract_count].bulk_write(
                        queue
                    )
                    self.end_and_log(recurring_type, s, net)
                except Exception as e:
                    self.tooter: Tooter
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Recurring: Failed to get pre_addresses_by_contract_count. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
            await asyncio.sleep(60)
